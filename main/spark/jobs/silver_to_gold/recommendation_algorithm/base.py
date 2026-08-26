"""추천 알고리즘 공통 계약과 재고 배정 인프라."""

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from main.spark.jobs.silver_to_gold.transformer import (
    KWH_PER_GALLON_EQUIVALENT,
    _columns,
)
from schema.gold import DriverCarSuggestion

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


def build_recommendation_candidates(
    driver_metrics: DataFrame, inventory: DataFrame
) -> DataFrame:
    """기사 N × 재고 모델 M 후보를 만들고 예상 비용·순수익·매출 증가·추천 사유를
    계산합니다. 알고리즘마다 배정 우선순위·적격 필터는 다르지만 이 계산 자체는
    같습니다."""
    available = inventory.select(
        F.col("vehicle_model_id").alias("_candidate_vehicle_model_id"),
        F.col("manufacturer").alias("_candidate_manufacturer"),
        F.col("model_name").alias("_candidate_model_name"),
        F.col("model_year").alias("_candidate_model_year"),
        F.col("fuel_type").alias("_candidate_fuel_type"),
        F.col("fuel_efficiency").alias("_candidate_fuel_efficiency"),
        F.col("comfort_eligible").alias("_candidate_comfort_eligible"),
        F.col("extra_comfort_eligible").alias("_candidate_extra_comfort_eligible"),
        F.col("weekly_lease_fee").alias("_candidate_weekly_lease_fee"),
        F.col("stock").alias("_candidate_stock"),
    )
    if available.isEmpty():
        raise ValueError("추천할 수 있는 재고 차량이 없습니다")

    candidates = driver_metrics.crossJoin(F.broadcast(available)).withColumn(
        "_is_current",
        F.col("vehicle_model_id") == F.col("_candidate_vehicle_model_id"),
    )
    expected_fuel_cost = F.when(
        F.col("_candidate_fuel_type") == "EV",
        F.col("_ev_price_miles")
        * F.lit(KWH_PER_GALLON_EQUIVALENT)
        / F.col("_candidate_fuel_efficiency"),
    ).otherwise(F.col("_gas_price_miles") / F.col("_candidate_fuel_efficiency"))
    expected_lease_fee = F.when(
        F.col("_is_current"), F.col("monthly_lease_fee")
    ).otherwise(
        F.col("_candidate_weekly_lease_fee")
        * F.col("_lease_weeks_in_month")
    )
    gains_comfort = (
        ~F.col("comfort_eligible") & F.col("_candidate_comfort_eligible")
    )
    gains_extra_comfort = (
        ~F.col("extra_comfort_eligible")
        & F.col("_candidate_extra_comfort_eligible")
    )
    expected_driver_pay = (
        F.when(
            gains_comfort & gains_extra_comfort,
            F.col("_monthly_driver_pay_if_both"),
        )
        .when(gains_comfort, F.col("_monthly_driver_pay_if_comfort"))
        .when(
            gains_extra_comfort,
            F.col("_monthly_driver_pay_if_extra_comfort"),
        )
        .otherwise(F.col("monthly_driver_pay"))
    )
    candidates = (
        candidates.withColumn("expected_monthly_fuel_cost", expected_fuel_cost)
        .withColumn("recommended_monthly_lease_fee", expected_lease_fee)
        .withColumn(
            "expected_monthly_net_profit",
            expected_driver_pay
            + F.col("monthly_tips")
            - F.col("expected_monthly_fuel_cost")
            - F.col("recommended_monthly_lease_fee"),
        )
        .withColumn(
            "expected_net_profit_increase",
            F.col("expected_monthly_net_profit") - F.col("monthly_net_profit"),
        )
        .withColumn(
            "expected_revenue_increase",
            F.col("recommended_monthly_lease_fee") - F.col("monthly_lease_fee"),
        )
    )
    eligible_tiers = F.concat_ws(
        ", ",
        F.when(
            ~F.col("comfort_eligible") & F.col("_candidate_comfort_eligible"),
            F.lit("Comfort(Uber)"),
        ),
        F.when(
            ~F.col("extra_comfort_eligible")
            & F.col("_candidate_extra_comfort_eligible"),
            F.lit("Extra Comfort(Lyft)"),
        ),
    )
    reasons = F.concat_ws(
        ", ",
        F.when(
            F.col("recommended_monthly_lease_fee") < F.col("monthly_lease_fee"),
            F.lit("렌트비 절감"),
        ),
        F.when(
            F.col("expected_monthly_fuel_cost") < F.col("monthly_fuel_cost"),
            F.lit("연료비 절감"),
        ),
        F.when(
            F.length(eligible_tiers) > 0,
            F.concat(eligible_tiers, F.lit(" 등급 가능")),
        ),
    )
    return candidates.withColumn("_reasons", reasons).withColumn(
        "recommendation_reason",
        F.when(F.col("_is_current"), F.lit("현재 차량 유지"))
        .when(F.length("_reasons") > 0, F.col("_reasons"))
        .otherwise(F.lit("예상 순수익 개선")),
    )


def _finalize_recommendation_output(
    rows: DataFrame, algorithm_version_id: int, threshold: int | Column
) -> DataFrame:
    """배정 결과를 확정된 `DriverCarSuggestion` 스키마로 정리합니다.

    `threshold`는 스칼라(예: `NO_THRESHOLD`)를 리터럴로 채우거나, `rows`가 이미
    그룹별로 다른 threshold를 담고 있으면 그 컬럼(`F.col("threshold")`)을 그대로
    넘겨 씁니다.
    """
    threshold_column = threshold if isinstance(threshold, Column) else F.lit(threshold)
    return rows.select(
        "driver_id",
        "year_month",
        "service_area",
        F.col("_candidate_comfort_eligible").alias("comfort_eligible"),
        F.col("_candidate_extra_comfort_eligible").alias("extra_comfort_eligible"),
        F.col("_candidate_vehicle_model_id").alias("vehicle_model_id"),
        F.col("_candidate_manufacturer").alias("manufacturer"),
        F.col("_candidate_model_name").alias("model_name"),
        F.col("_candidate_model_year").alias("model_year"),
        "recommendation_reason",
        F.col("_candidate_fuel_efficiency").alias("fuel_efficiency"),
        "recommended_monthly_lease_fee",
        "expected_monthly_fuel_cost",
        "expected_monthly_net_profit",
        "expected_net_profit_increase",
        "expected_revenue_increase",
        F.lit(algorithm_version_id).alias("recommendation_algorithm_version_id"),
        threshold_column.alias("threshold"),
    ).select(*_columns(DriverCarSuggestion))


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
    group_columns: tuple[str, ...] = (),
) -> DataFrame:
    """기사별로 `preference_order` 순위대로 제안하고, 같은 모델에 여러 기사가
    몰리면 `stock_priority_order` 기준으로 남은 재고 안에서 배정합니다.

    배정 메커니즘(랭킹 → 라운드별로 점유·소진 재고를 빼고 남은 재고만큼 채움)은
    알고리즘과 무관한 공통 부분이라 여기 있고, "누구를 먼저 챙기는가"만 두
    정렬 기준으로 각 알고리즘이 주입합니다.

    `group_columns`을 주면 그 값이 다른 행끼리는 서로 완전히 독립된 배정으로
    취급합니다 — 같은 물리 재고를 그룹마다 전부 가진 것처럼 각자 배정합니다.
    threshold 스윕처럼 원래 서로 다른 배정을 여러 번 만들어야 하는 경우, 이걸
    쓰면 라운드 루프 자체는 한 번만 돌면서도 그룹별 배정 결과를 그대로 얻을 수
    있습니다(#1021) — `occupied_stock`(실제 현재 보유 현황)은 그룹과 무관해
    그대로 공유하고, `used_stock`·랭킹·재고 경쟁만 그룹별로 나눕니다.
    """
    preference = Window.partitionBy("driver_id", *group_columns).orderBy(
        *preference_order
    )
    ranked = candidates.withColumn(
        "_driver_rank", F.row_number().over(preference)
    ).persist()
    occupied_stock = (
        ranked.filter(F.col("_is_current"))
        .select("driver_id", "_candidate_vehicle_model_id")
        .distinct()
        .groupBy("_candidate_vehicle_model_id")
        .agg(F.count(F.lit(1)).alias("_occupied_stock"))
    )
    max_rank = ranked.agg(F.max("_driver_rank")).first()[0]
    assigned = None

    for driver_rank in range(1, max_rank + 1):
        proposals = ranked.filter(F.col("_driver_rank") == driver_rank)
        if assigned is not None:
            proposals = proposals.join(
                assigned.select("driver_id", *group_columns),
                ["driver_id", *group_columns],
                "left_anti",
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
                .groupBy("_candidate_vehicle_model_id", *group_columns)
                .agg(F.count(F.lit(1)).alias("_used_stock"))
            )
            changes = changes.join(
                used_stock, ["_candidate_vehicle_model_id", *group_columns], "left"
            ).fillna({"_used_stock": 0})

        stock_priority = Window.partitionBy(
            "_candidate_vehicle_model_id", *group_columns
        ).orderBy(*stock_priority_order)
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
