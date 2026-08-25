"""validate_gold_business_invariants가 (algorithm, threshold) 조합별로 독립적인
가정 리포트를 검증하는지 확인. 이슈 #997 — driver_car_suggestion이 알고리즘·threshold
조합마다 별도 배정 리포트를 담게 되면서, 한 조합의 재고 초과·순수익 음수가 다른
조합까지 오염시키거나 가려서는 안 된다."""

import pytest

from main.spark.jobs.silver_to_gold.transformer import validate_gold_business_invariants
from shared.spark.common.session import get_or_create_spark_session


@pytest.fixture(scope="module")
def spark():
    session = get_or_create_spark_session("test_validate_gold_business_invariants")
    yield session
    session.stop()


def _driver_profit(spark, driver_ids):
    return spark.createDataFrame([(d,) for d in driver_ids], ["driver_id"])


def _driver_snapshot(spark, driver_ids):
    return spark.createDataFrame([(d,) for d in driver_ids], ["driver_id"])


def _inventory(spark, stocks: dict[str, int]):
    return spark.createDataFrame(list(stocks.items()), ["vehicle_model_id", "stock"])


def _suggestion_row(driver_id, vehicle_model_id, algorithm_version_id, threshold, profit_increase=10.0):
    return (driver_id, vehicle_model_id, profit_increase, algorithm_version_id, threshold)


_SUGGESTION_COLUMNS = [
    "driver_id", "vehicle_model_id", "expected_net_profit_increase",
    "recommendation_algorithm_version_id", "threshold",
]


def test_알고리즘별_threshold별_조합이_모두_정상이면_통과한다(spark):
    driver_profit = _driver_profit(spark, ["D1", "D2"])
    driver_snapshot = _driver_snapshot(spark, ["D1", "D2"])
    inventory = _inventory(spark, {"A": 2})
    recommendation = spark.createDataFrame(
        [
            _suggestion_row("D1", "A", 1, -1),
            _suggestion_row("D2", "A", 1, -1),
            _suggestion_row("D1", "A", 2, 100),
            _suggestion_row("D2", "A", 2, 100),
        ],
        _SUGGESTION_COLUMNS,
    )

    validate_gold_business_invariants(
        driver_profit, recommendation, driver_snapshot, inventory
    )


def test_한_조합만_재고를_초과해도_실패한다(spark):
    """algorithm=1은 정상, algorithm=2가 모델 A 재고(1)를 초과해 배정."""
    driver_profit = _driver_profit(spark, ["D1", "D2"])
    driver_snapshot = _driver_snapshot(spark, ["D1", "D2"])
    inventory = _inventory(spark, {"A": 1, "B": 1})
    recommendation = spark.createDataFrame(
        [
            _suggestion_row("D1", "A", 1, -1),
            _suggestion_row("D2", "B", 1, -1),
            _suggestion_row("D1", "A", 2, 100),
            _suggestion_row("D2", "A", 2, 100),
        ],
        _SUGGESTION_COLUMNS,
    )

    with pytest.raises(ValueError, match=r"algorithm=2.*재고 초과"):
        validate_gold_business_invariants(
            driver_profit, recommendation, driver_snapshot, inventory
        )


def test_한_조합만_순수익_증가가_음수여도_실패한다(spark):
    driver_profit = _driver_profit(spark, ["D1"])
    driver_snapshot = _driver_snapshot(spark, ["D1"])
    inventory = _inventory(spark, {"A": 2})
    recommendation = spark.createDataFrame(
        [
            _suggestion_row("D1", "A", 1, -1, profit_increase=10.0),
            _suggestion_row("D1", "A", 2, 100, profit_increase=-5.0),
        ],
        _SUGGESTION_COLUMNS,
    )

    with pytest.raises(ValueError, match=r"algorithm=2.*순수익 증가액이 음수"):
        validate_gold_business_invariants(
            driver_profit, recommendation, driver_snapshot, inventory
        )


def test_한_조합에서만_기사가_빠지면_실패한다(spark):
    driver_profit = _driver_profit(spark, ["D1", "D2"])
    driver_snapshot = _driver_snapshot(spark, ["D1", "D2"])
    inventory = _inventory(spark, {"A": 2})
    recommendation = spark.createDataFrame(
        [
            _suggestion_row("D1", "A", 1, -1),
            _suggestion_row("D2", "A", 1, -1),
            _suggestion_row("D1", "A", 2, 100),
            # D2가 algorithm=2 그룹에서 빠짐
        ],
        _SUGGESTION_COLUMNS,
    )

    with pytest.raises(ValueError, match=r"기사 수 불일치.*algorithm=2"):
        validate_gold_business_invariants(
            driver_profit, recommendation, driver_snapshot, inventory
        )
