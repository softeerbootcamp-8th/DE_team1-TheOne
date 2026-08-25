"""기사 순수익 증가 최우선 배정 알고리즘. (v1, #927 재고 기반 배정 + #955 매출 우선 필터)"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from main.spark.jobs.silver_to_gold.recommendation_algorithm.base import (
    NO_THRESHOLD,
    VehicleRecommendationAlgorithm,
    _allocate_candidates_by_stock,
    _validate_candidate_grain,
)
from main.spark.jobs.silver_to_gold.transformer import (
    KWH_PER_GALLON_EQUIVALENT,
    _columns,
    build_driver_monthly_profit,
)
from schema.gold import DriverCarSuggestion


class ProfitFirstAlgorithm(VehicleRecommendationAlgorithm):
    """기사 순수익 증가를 최우선으로 배정하고, 회사 매출 증가(>0)를 필터로 건다.
    (v1, #927 재고 기반 배정 + #955 매출 우선 필터)"""

    ALGORITHM_VERSION_ID = 1

    def recommend(self, driver_metrics: DataFrame, inventory: DataFrame) -> DataFrame:
        """동적 기사 N×모델 M 후보를 계산하고 재고 안에서 기사별 차량을 배정합니다."""
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
        candidates = candidates.withColumn("_reasons", reasons).withColumn(
            "recommendation_reason",
            F.when(F.col("_is_current"), F.lit("현재 차량 유지"))
            .when(F.length("_reasons") > 0, F.col("_reasons"))
            .otherwise(F.lit("예상 순수익 개선")),
        )

        driver_profit = build_driver_monthly_profit(driver_metrics)
        _validate_candidate_grain(driver_profit, candidates, inventory)

        def recommendation_output(rows: DataFrame) -> DataFrame:
            return rows.select(
                "driver_id",
                "year_month",
                "service_area",
                F.col("_candidate_comfort_eligible").alias("comfort_eligible"),
                F.col("_candidate_extra_comfort_eligible").alias(
                    "extra_comfort_eligible"
                ),
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
                F.lit(self.ALGORITHM_VERSION_ID).alias(
                    "recommendation_algorithm_version_id"
                ),
                F.lit(NO_THRESHOLD).alias("threshold"),
            ).select(*_columns(DriverCarSuggestion))

        # 회사 매출에 기여 못 하는 차량 교체는 추천에서 제외한다(#955). 현재 차량은
        # 정의상 매출 증가가 0이라 이 조건에서 예외로 둬 최후의 보루로 남긴다.
        assignable = candidates.filter(
            F.col("_is_current")
            | (
                (F.col("_candidate_stock") > 0)
                & (F.col("expected_revenue_increase") > 0)
            )
        )
        allocated = _allocate_candidates_by_stock(
            assignable,
            preference_order=[
                F.col("expected_monthly_net_profit").desc(),
                F.col("_is_current").desc(),
                F.col("_candidate_model_year").desc(),
                F.col("_candidate_vehicle_model_id").asc(),
            ],
            stock_priority_order=[
                F.col("expected_net_profit_increase").desc(),
                F.col("expected_revenue_increase").desc(),
                F.col("driver_id").asc(),
            ],
        )
        return recommendation_output(allocated)
