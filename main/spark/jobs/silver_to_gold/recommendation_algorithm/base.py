"""추천 알고리즘 공통 계약과 재고 배정 인프라."""

from pyspark.sql import Column, DataFrame, Window
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


def _allocate_candidates_by_stock(
    candidates: DataFrame,
    preference_order: list[Column],
    stock_priority_order: list[Column],
) -> DataFrame:
    """기사별로 `preference_order` 순위대로 제안하고, 같은 모델에 여러 기사가
    몰리면 `stock_priority_order` 기준으로 남은 재고 안에서 배정합니다.

    배정 메커니즘(랭킹 → 라운드별로 점유·소진 재고를 빼고 남은 재고만큼 채움)은
    알고리즘과 무관한 공통 부분이라 여기 있고, "누구를 먼저 챙기는가"만 두
    정렬 기준으로 각 알고리즘이 주입합니다.
    """
    preference = Window.partitionBy("driver_id").orderBy(*preference_order)
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
            *stock_priority_order
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
