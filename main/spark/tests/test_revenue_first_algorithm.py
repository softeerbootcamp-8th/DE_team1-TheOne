"""RevenueFirstAlgorithm(v2) 시나리오. 이슈 #997.

1. 기사 순수익 threshold를 넘는 후보 중에서는 회사 매출 증가가 큰 쪽을 우선한다
2. 기사 순수익 증가가 threshold 미만인 후보는 매출이 커도 배정하지 않는다
   (기존 차량보다 손해 보는 배정은 나올 수 없다)
3. threshold를 여러 개 스윕하면 값마다 별도로 태그된 행이 쌓인다

기준 기사(A 보유, 월 순수익 600 = 1000 - 200(연료비) - 200(리스료)):
_gas_price_miles=1100, A 연비=5.5 → 연료비 1100/5.5=200 (기존 monthly_fuel_cost와 일치,
즉 "현재 차량 유지" 후보의 순수익 증가는 항상 0이어야 한다). 교체 후보 D/E/G는 연비 10
(연료비 1100/10=110)으로 통일하고 리스료만 바꿔 순수익·매출 증가를 계산한다.

  후보 |  월 리스료 | 순수익 증가(400-110-리스료) | 매출 증가(리스료-200)
  D    |  220       |  70                          |  20
  E    |  260       |  30                          |  60
  G    |  400       | -110                         | 200 (순수익 손해라 항상 제외)
"""

import pytest

from main.spark.jobs.silver_to_gold.recommendation_algorithm import (
    DEFAULT_THRESHOLDS,
    RevenueFirstAlgorithm,
)
from shared.spark.common.session import get_or_create_spark_session

_DRIVER_METRICS_COLUMNS = [
    "driver_id", "year_month", "service_area", "comfort_eligible",
    "extra_comfort_eligible", "taxi_id", "vehicle_model_id", "manufacturer",
    "model_name", "model_year", "fuel_efficiency", "monthly_mileage",
    "monthly_driver_pay", "monthly_tips", "monthly_fuel_cost",
    "monthly_lease_fee", "monthly_net_profit", "_gas_price_miles", "_ev_price_miles",
    "_monthly_driver_pay_if_comfort", "_monthly_driver_pay_if_extra_comfort",
    "_monthly_driver_pay_if_both", "_lease_weeks_in_month",
]
_INVENTORY_COLUMNS = [
    "vehicle_model_id", "manufacturer", "model_name", "model_year",
    "fuel_type", "fuel_efficiency", "comfort_eligible",
    "extra_comfort_eligible", "weekly_lease_fee", "stock",
]


@pytest.fixture(scope="module")
def spark():
    session = get_or_create_spark_session("test_revenue_first_algorithm")
    yield session
    session.stop()


def _driver_metrics(spark):
    return spark.createDataFrame(
        [(
            "D1", "2026-01", "NYC", False, False, "T1", "A", "MAKE", "A", 2024,
            5.5, 1000.0, 1000.0, 0.0, 200.0, 200.0, 600.0, 1100.0, 50.0,
            1000.0, 1000.0, 1000.0, 4.0,
        )],
        _DRIVER_METRICS_COLUMNS,
    )


def test_기사_순수익_threshold를_넘는_후보중_회사_매출_증가가_큰_쪽을_우선한다(spark):
    """D는 순수익 증가(70)가 더 크고, E는 매출 증가(60)가 더 크다 — 둘 다
    threshold(20)를 넘으므로 v2는 매출이 더 큰 E를 골라야 한다. G는 매출이
    가장 크지만(200) 순수익 증가가 음수(-110)라 threshold 미달로 제외된다."""
    driver_metrics = _driver_metrics(spark)
    inventory = spark.createDataFrame(
        [
            ("A", "MAKE", "A", 2024, "GAS", 5.5, False, False, 50.0, 2),
            ("D", "MAKE", "D", 2024, "GAS", 10.0, False, False, 55.0, 2),
            ("E", "MAKE", "E", 2024, "GAS", 10.0, False, False, 65.0, 2),
            ("G", "MAKE", "G", 2024, "GAS", 10.0, False, False, 100.0, 2),
        ],
        _INVENTORY_COLUMNS,
    )

    result = RevenueFirstAlgorithm(thresholds=(20,)).recommend(driver_metrics, inventory)
    row = result.collect()[0]

    assert row["vehicle_model_id"] == "E"
    assert row["threshold"] == 20
    assert row["recommendation_algorithm_version_id"] == 2
    assert row["expected_net_profit_increase"] == pytest.approx(30.0)
    assert row["expected_revenue_increase"] == pytest.approx(60.0)


def test_threshold_미달_후보만_있으면_현재_차량을_유지한다(spark):
    """D(순수익 증가 70)만 있고 threshold가 100이면 아무도 못 넘어 현재 차량 유지."""
    driver_metrics = _driver_metrics(spark)
    inventory = spark.createDataFrame(
        [
            ("A", "MAKE", "A", 2024, "GAS", 5.5, False, False, 50.0, 2),
            ("D", "MAKE", "D", 2024, "GAS", 10.0, False, False, 55.0, 2),
        ],
        _INVENTORY_COLUMNS,
    )

    result = RevenueFirstAlgorithm(thresholds=(100,)).recommend(driver_metrics, inventory)
    row = result.collect()[0]

    assert row["vehicle_model_id"] == "A"
    assert row["expected_net_profit_increase"] == pytest.approx(0.0)


def test_threshold를_스윕하면_값마다_별도_행이_쌓이고_배정도_달라질_수_있다(spark):
    """threshold=20에서는 매출 큰 E, threshold=60에서는 E가 탈락해 D가 남는다."""
    driver_metrics = _driver_metrics(spark)
    inventory = spark.createDataFrame(
        [
            ("A", "MAKE", "A", 2024, "GAS", 5.5, False, False, 50.0, 2),
            ("D", "MAKE", "D", 2024, "GAS", 10.0, False, False, 55.0, 2),
            ("E", "MAKE", "E", 2024, "GAS", 10.0, False, False, 65.0, 2),
        ],
        _INVENTORY_COLUMNS,
    )

    result = RevenueFirstAlgorithm(thresholds=(20, 60)).recommend(driver_metrics, inventory)
    rows = result.collect()
    by_threshold = {row["threshold"]: row["vehicle_model_id"] for row in rows}

    assert by_threshold == {20: "E", 60: "D"}
    assert len(rows) == 2


def test_기본_threshold는_100부터_500까지_100단위_다섯개다():
    assert DEFAULT_THRESHOLDS == (100, 200, 300, 400, 500)


def test_어떤_threshold에서도_기존_차량보다_기사_순수익이_줄지_않는다(spark):
    """G(매출 증가 200, 순수익 증가 -110)가 있어도 어떤 threshold에서도 뽑히면 안 된다."""
    driver_metrics = _driver_metrics(spark)
    inventory = spark.createDataFrame(
        [
            ("A", "MAKE", "A", 2024, "GAS", 5.5, False, False, 50.0, 2),
            ("G", "MAKE", "G", 2024, "GAS", 10.0, False, False, 100.0, 2),
        ],
        _INVENTORY_COLUMNS,
    )

    result = RevenueFirstAlgorithm(thresholds=DEFAULT_THRESHOLDS).recommend(
        driver_metrics, inventory
    )
    rows = result.collect()

    assert len(rows) == len(DEFAULT_THRESHOLDS)
    assert all(row["expected_net_profit_increase"] >= 0 for row in rows)
    assert all(row["vehicle_model_id"] == "A" for row in rows)
