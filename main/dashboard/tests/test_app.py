"""Gold 차량 추천 대시보드 회귀 시나리오. 이슈 #562.

1. 월 파티션별 Gold CSV를 모두 로드 (`LocalCsvDataSource`, 이슈 #778로 이전)
2. 월간 리포트와 같은 순이익·회사 매출 기준을 통과한 기사만 표시
3. 기사·월 기준으로 현재 차량 비용과 최종 추천 차량 비용을 결합
"""

from pathlib import Path

import pandas as pd

from app import recommendation_scope
from datasource import LocalCsvDataSource


def _write_partition(
    root: Path, dataset: str, year_month: str, rows: list[dict]
) -> None:
    partition = root / dataset / f"year_month={year_month}"
    partition.mkdir(parents=True)
    pd.DataFrame(rows).to_csv(partition / f"{dataset}.csv", index=False)


def _suggestion(driver_id: str, profit: float, revenue: float) -> dict:
    return {
        "driver_id": driver_id,
        "year_month": "2026-01",
        "manufacturer": "KIA",
        "model_name": "FORTE",
        "model_year": 2024,
        "recommendation_reason": "연료비 절감",
        "recommended_monthly_lease_fee": 2100.0,
        "expected_monthly_fuel_cost": 80.0,
        "expected_monthly_net_profit": 3000.0,
        "expected_net_profit_increase": profit,
        "expected_revenue_increase": revenue,
    }


def _aggregation(driver_id: str, year_month: str = "2026-01") -> dict:
    return {
        "driver_id": driver_id,
        "year_month": year_month,
        "manufacturer": "TOYOTA",
        "model_name": "CAMRY",
        "model_year": 2023,
        "monthly_lease_fee": 2200.0,
        "monthly_fuel_cost": 120.0,
        "monthly_net_profit": 2400.0,
        "monthly_mileage": 1000.0,
        "monthly_driver_pay": 4500.0,
        "monthly_tips": 220.0,
    }


def test_월별_Gold_파티션을_모두_읽는다(tmp_path):
    _write_partition(
        tmp_path,
        "monthly_report",
        "2026-01",
        [{"year_month": "2026-01", "recommended_driver_count": 1}],
    )
    _write_partition(
        tmp_path,
        "monthly_report",
        "2026-02",
        [{"year_month": "2026-02", "recommended_driver_count": 2}],
    )

    frame = LocalCsvDataSource(tmp_path).load("monthly_report")

    assert sorted(frame["year_month"]) == ["2026-01", "2026-02"]


def test_리포트_추천_기준을_통과한_기사만_표시한다():
    suggestion = pd.DataFrame(
        [
            _suggestion("eligible", 600.0, 1.0),
            _suggestion("no_company_gain", 700.0, 0.0),
            _suggestion("low_profit", 599.0, 10.0),
            _suggestion("company_loss", 700.0, -1.0),
        ]
    )
    aggregation = pd.DataFrame(
        [_aggregation(driver_id) for driver_id in suggestion["driver_id"]]
    )

    scope = recommendation_scope(
        suggestion, aggregation, "2026-01", threshold=600.0
    )

    assert scope["driver_id"].tolist() == ["eligible"]


def test_기사와_월이_같은_현재_비용을_최종_추천에_붙인다():
    suggestion = pd.DataFrame([_suggestion("D1", 700.0, 20.0)])
    aggregation = pd.DataFrame(
        [
            _aggregation("D1", "2025-12"),
            _aggregation("D1", "2026-01"),
        ]
    )

    row = recommendation_scope(
        suggestion, aggregation, "2026-01", threshold=600.0
    ).iloc[0]

    assert row["manufacturer"] == "KIA"
    assert row["current_manufacturer"] == "TOYOTA"
    assert row["recommended_monthly_lease_fee"] == 2100.0
    assert row["current_monthly_lease_fee"] == 2200.0
    assert row["expected_monthly_fuel_cost"] == 80.0
    assert row["current_monthly_fuel_cost"] == 120.0
