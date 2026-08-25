"""기사 순수익 증가 최우선 배정 알고리즘. (v1, #927 재고 기반 배정 + #955 매출 우선 필터)"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from main.spark.jobs.silver_to_gold.recommendation_algorithm.base import (
    NO_THRESHOLD,
    VehicleRecommendationAlgorithm,
    _allocate_candidates_by_stock,
    _finalize_recommendation_output,
    _validate_candidate_grain,
    build_recommendation_candidates,
)
from main.spark.jobs.silver_to_gold.transformer import build_driver_monthly_profit


class ProfitFirstAlgorithm(VehicleRecommendationAlgorithm):
    """기사 순수익 증가를 최우선으로 배정하고, 회사 매출 증가(>0)를 필터로 건다.
    (v1, #927 재고 기반 배정 + #955 매출 우선 필터)"""

    ALGORITHM_VERSION_ID = 1

    def recommend(self, driver_metrics: DataFrame, inventory: DataFrame) -> DataFrame:
        """동적 기사 N×모델 M 후보를 계산하고 재고 안에서 기사별 차량을 배정합니다."""
        candidates = build_recommendation_candidates(driver_metrics, inventory)

        driver_profit = build_driver_monthly_profit(driver_metrics)
        _validate_candidate_grain(driver_profit, candidates, inventory)

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
        return _finalize_recommendation_output(
            allocated, self.ALGORITHM_VERSION_ID, NO_THRESHOLD
        )
