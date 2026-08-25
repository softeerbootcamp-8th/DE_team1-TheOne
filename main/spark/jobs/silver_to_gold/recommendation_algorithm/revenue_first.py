"""회사 매출 증가 최우선, 기사 순수익 증가 threshold 필터 배정 알고리즘. (v2, #997)

기사 순수익 증가(threshold)를 여러 값으로 스윕해 값마다 별도 `threshold`로 태그한
행을 쌓는다. threshold가 0보다 크므로 "현재 차량 유지"보다 못한(순수익이 줄어드는)
배정은 적격 필터에서 걸러져 나올 수 없다 — 기존 기사가 차량 교체로 손해를 보는
경우는 생기지 않는다.

threshold별로 적격 후보군이 달라 배정도 서로 독립적이어야 하지만, 그 배정을 값마다
따로 라운드 루프를 돌려 계산하지는 않는다(#1021) — threshold를 candidates에
차원으로 얹고 `_allocate_candidates_by_stock`의 `group_columns`로 넘겨, 한 번의
라운드 루프 안에서 threshold별로 서로 섞이지 않게 배정한다.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from main.spark.jobs.silver_to_gold.recommendation_algorithm.base import (
    VehicleRecommendationAlgorithm,
    _allocate_candidates_by_stock,
    _finalize_recommendation_output,
    _validate_candidate_grain,
    build_recommendation_candidates,
)
from main.spark.jobs.silver_to_gold.transformer import build_driver_monthly_profit

# 회사 정책 파라미터가 없을 때 스윕할 기본 threshold 목록. Airflow Variable로
# 덮어쓸 수 있다 — job.py --thresholds 참고.
DEFAULT_THRESHOLDS = (100, 200, 300, 400, 500)


class RevenueFirstAlgorithm(VehicleRecommendationAlgorithm):
    """회사 매출 증가를 최우선으로 배정하고, 기사 순수익 증가가 threshold 이상인
    후보만 적격으로 본다. (v2, #997)"""

    ALGORITHM_VERSION_ID = 2

    def __init__(self, thresholds: tuple[int, ...] = DEFAULT_THRESHOLDS):
        self._thresholds = thresholds

    def recommend(self, driver_metrics: DataFrame, inventory: DataFrame) -> DataFrame:
        candidates = build_recommendation_candidates(driver_metrics, inventory).persist()

        driver_profit = build_driver_monthly_profit(driver_metrics)
        _validate_candidate_grain(driver_profit, candidates, inventory)

        thresholds = driver_metrics.sparkSession.createDataFrame(
            [(threshold,) for threshold in self._thresholds], ["threshold"]
        )
        # 현재 차량은 항상 적격(교체 안 함 = 손해 없음). 교체 후보는 재고가 있고
        # 기사 순수익 증가가 threshold 이상일 때만 — threshold > 0 이라 이 필터를
        # 통과한 교체는 항상 현재보다 기사에게 이득이다.
        eligible = candidates.crossJoin(F.broadcast(thresholds)).filter(
            F.col("_is_current")
            | (
                (F.col("_candidate_stock") > 0)
                & (F.col("expected_net_profit_increase") >= F.col("threshold"))
            )
        )
        allocated = _allocate_candidates_by_stock(
            eligible,
            preference_order=[
                F.col("expected_revenue_increase").desc(),
                F.col("expected_net_profit_increase").desc(),
                F.col("_is_current").desc(),
                F.col("_candidate_vehicle_model_id").asc(),
            ],
            stock_priority_order=[
                F.col("expected_revenue_increase").desc(),
                F.col("expected_net_profit_increase").desc(),
                F.col("driver_id").asc(),
            ],
            group_columns=("threshold",),
        )
        candidates.unpersist()
        return _finalize_recommendation_output(
            allocated, self.ALGORITHM_VERSION_ID, F.col("threshold")
        )
