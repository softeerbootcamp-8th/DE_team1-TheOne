"""추천 알고리즘 공통 계약과 재고 배정 인프라."""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

# threshold를 쓰지 않는 알고리즘이 DriverCarSuggestion.threshold에 채우는 sentinel.
# 실제 threshold는 항상 0 이상이라 이 값과 구분됩니다.
NO_THRESHOLD = -1


class VehicleRecommendationAlgorithm:
    """추천 알고리즘 공통 계약.

    `recommend()`는 완성된 `DriverCarSuggestion` 스키마(`threshold`,
    `recommendation_algorithm_version_id` 포함)를 반환해야 합니다. 알고리즘마다
    다른 건 배정 우선순위·필터 기준뿐이라, 후보 생성·재고 배정 같은 공통 인프라는
    각 구현이 이 모듈의 `_validate_candidate_grain`/`_allocate_candidates_by_stock`을
    재사용합니다.

    `schema.gold.RecommendationAlgorithm`(알고리즘 버전 설명 마스터 테이블)과는
    다른 클래스입니다 — 그건 DB에 저장되는 메타데이터이고, 이건 실제 배정 로직입니다.
    """

    ALGORITHM_VERSION_ID: int

    def recommend(self, driver_metrics: DataFrame, inventory: DataFrame) -> DataFrame:
        raise NotImplementedError


def _validate_candidate_grain(
    driver_profit: DataFrame,
    recommendation_candidates: DataFrame,
    inventory: DataFrame,
) -> None:
    """내부 후보가 실제 기사 N × 재고 모델 M 조합을 모두 보존하는지 검증합니다."""
    driver_count = driver_profit.select("driver_id").distinct().count()
    inventory_models = inventory.select("vehicle_model_id").distinct().count()
    candidate_stats = recommendation_candidates.agg(
        F.count(F.lit(1)).alias("rows"),
        F.countDistinct("driver_id").alias("drivers"),
        F.countDistinct("driver_id", "_candidate_vehicle_model_id").alias(
            "candidate_keys"
        ),
    ).first()
    expected_candidates = driver_count * inventory_models
    if (
        candidate_stats["rows"] != expected_candidates
        or candidate_stats["rows"] != candidate_stats["candidate_keys"]
        or candidate_stats["drivers"] != driver_count
    ):
        raise ValueError(
            "Gold 추천 후보 수 불일치: "
            f"drivers={driver_count} "
            f"vehicle_models={inventory_models} "
            f"expected={expected_candidates} actual={candidate_stats['rows']}"
        )


def _allocate_candidates_by_stock(candidates: DataFrame) -> DataFrame:
    """기사별 수익 순위대로 제안하고 남은 모델 재고 안에서 Spark로 배정합니다."""
    preference = Window.partitionBy("driver_id").orderBy(
        F.col("expected_monthly_net_profit").desc(),
        F.col("_is_current").desc(),
        F.col("_candidate_model_year").desc(),
        F.col("_candidate_vehicle_model_id").asc(),
    )
    ranked = candidates.withColumn(
        "_driver_rank", F.row_number().over(preference)
    ).persist()
    occupied_stock = (
        ranked.filter(F.col("_is_current"))
        .groupBy("_candidate_vehicle_model_id")
        .agg(F.count(F.lit(1)).alias("_occupied_stock"))
    )
    max_rank = ranked.agg(F.max("_driver_rank")).first()[0]
    assigned = None

    for driver_rank in range(1, max_rank + 1):
        proposals = ranked.filter(F.col("_driver_rank") == driver_rank)
        if assigned is not None:
            proposals = proposals.join(
                assigned.select("driver_id"), "driver_id", "left_anti"
            )

        keep_current = proposals.filter(F.col("_is_current"))
        changes = (
            proposals.filter(~F.col("_is_current"))
            .join(occupied_stock, "_candidate_vehicle_model_id", "left")
            .fillna({"_occupied_stock": 0})
        )
        if assigned is None:
            changes = changes.withColumn("_used_stock", F.lit(0))
        else:
            used_stock = (
                assigned.filter(~F.col("_is_current"))
                .groupBy("_candidate_vehicle_model_id")
                .agg(F.count(F.lit(1)).alias("_used_stock"))
            )
            changes = changes.join(
                used_stock, "_candidate_vehicle_model_id", "left"
            ).fillna({"_used_stock": 0})

        stock_priority = Window.partitionBy("_candidate_vehicle_model_id").orderBy(
            F.col("expected_net_profit_increase").desc(),
            F.col("expected_revenue_increase").desc(),
            F.col("driver_id").asc(),
        )
        changes = (
            changes.withColumn("_stock_rank", F.row_number().over(stock_priority))
            .filter(
                F.col("_stock_rank")
                <= F.col("_candidate_stock")
                - F.col("_occupied_stock")
                - F.col("_used_stock")
            )
            .drop("_occupied_stock", "_used_stock", "_stock_rank")
        )
        winners = keep_current.unionByName(changes)
        assigned = winners if assigned is None else assigned.unionByName(winners)
        assigned = assigned.coalesce(8).localCheckpoint(eager=False)

    ranked.unpersist()
    return assigned
