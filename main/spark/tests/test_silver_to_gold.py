"""Silver → Gold 차량 재고 배정 시나리오. 이슈 #561.

1. 희소 차량은 예상 순이익 증가액이 큰 기사에게 우선 배정
2. 재고에서 밀린 기사는 다음 순위 차량을 배정받고 모델별 재고를 초과하지 않음
3. 월간 리포트는 기사 순수익 기준과 회사 객단가 상승을 모두 만족한 기사만 집계
"""

import pytest

from main.spark.jobs.silver_to_gold.transformer import (
    _allocate_candidates_by_stock,
    build_monthly_report,
)
from shared.spark.common.session import get_or_create_spark_session


@pytest.fixture(scope="module")
def spark():
    session = get_or_create_spark_session("test_silver_to_gold_stock")
    yield session
    session.stop()


def _candidate(
    driver_id: str,
    model_id: str,
    net_profit: float,
    stock: int,
    *,
    current: bool = False,
    revenue_increase: float = 0.0,
) -> dict:
    return {
        "driver_id": driver_id,
        "expected_monthly_net_profit": net_profit,
        "expected_net_profit_increase": net_profit - 100.0,
        "expected_revenue_increase": revenue_increase,
        "_is_current": current,
        "_candidate_model_year": 2026,
        "_candidate_vehicle_model_id": model_id,
        "_candidate_stock": stock,
    }


def test_재고_한대는_순이익_증가가_큰_기사에게_우선_배정한다(spark):
    candidates = spark.createDataFrame(
        [
            _candidate("high", "rare", 200.0, 1),
            _candidate("high", "high-current", 100.0, 0, current=True),
            _candidate("low", "rare", 150.0, 1, revenue_increase=20.0),
            _candidate("low", "low-current", 100.0, 0, current=True),
        ]
    )

    assigned = {
        row.driver_id: row._candidate_vehicle_model_id
        for row in _allocate_candidates_by_stock(candidates).collect()
    }

    assert assigned == {"high": "rare", "low": "low-current"}


def test_재고에서_밀린_기사는_차선_차량을_받는다(spark):
    candidates = spark.createDataFrame(
        [
            _candidate("high", "rare", 200.0, 1),
            _candidate("high", "high-current", 100.0, 0, current=True),
            _candidate("low", "rare", 180.0, 1),
            _candidate("low", "second", 160.0, 1),
            _candidate("low", "low-current", 100.0, 0, current=True),
        ]
    )

    rows = _allocate_candidates_by_stock(candidates).collect()
    assigned = {row.driver_id: row._candidate_vehicle_model_id for row in rows}

    assert assigned == {"high": "rare", "low": "second"}
    assert sum(row._candidate_vehicle_model_id == "rare" for row in rows) == 1


def test_월간_리포트는_회사_객단가가_실제로_상승한_기사만_집계한다(spark):
    recommendation = spark.createDataFrame(
        [
            ("eligible", 600.0, 1.0),
            ("no_company_gain", 700.0, 0.0),
            ("low_profit", 599.0, 10.0),
            ("company_loss", 700.0, -1.0),
        ],
        ["driver_id", "expected_net_profit_increase", "expected_revenue_increase"],
    )

    report = build_monthly_report(recommendation, "2026-01", 600.0).first()

    assert report.recommended_driver_count == 1
    assert report.avg_net_profit_increase_per_driver == 600.0
    assert report.avg_revenue_increase_per_driver == 1.0
    assert report.total_revenue_increase == 1.0
