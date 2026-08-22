"""대시보드 Gold 데이터 소스 전환. 이슈 #778.

1. 로컬 CSV 파티션을 모두 이어붙인다
2. 로컬 파티션이 하나도 없으면 빈 데이터프레임
3. RDS 쿼리 컬럼은 schema.gold 필드에서 자동 생성된다 (하드코딩 컬럼 금지)
4. RDS는 year_month별 최신 version 행만 읽는다 — 과거 재실행 버전은 제외
5. RDS는 같은 (year_month, version) 안의 여러 행(기사별)을 전부 읽는다
6. 알 수 없는 dataset 이름은 RdsDataSource에서 즉시 ValueError
7. DASHBOARD_DATA_SOURCE 기본값은 local
8. DASHBOARD_DATA_SOURCE=rds인데 GOLD_DATABASE_URL이 없으면 즉시 ValueError
9. 알 수 없는 DASHBOARD_DATA_SOURCE 값은 즉시 ValueError
"""

import sqlite3
from dataclasses import fields

import pandas as pd
import pytest

from datasource import (
    LocalCsvDataSource,
    RdsDataSource,
    _latest_version_query,
    build_data_source,
)
from schema.gold import DriverMonthlyProfit, MonthlyReport


def test_로컬_소스는_모든_파티션을_이어붙인다(tmp_path):
    for year_month in ("2026-01", "2026-02"):
        partition = tmp_path / "monthly_report" / f"year_month={year_month}"
        partition.mkdir(parents=True)
        pd.DataFrame([{"year_month": year_month}]).to_csv(
            partition / "monthly_report.csv", index=False
        )

    frame = LocalCsvDataSource(tmp_path).load("monthly_report")

    assert sorted(frame["year_month"]) == ["2026-01", "2026-02"]


def test_로컬_소스는_파티션이_없으면_빈_데이터프레임(tmp_path):
    frame = LocalCsvDataSource(tmp_path).load("monthly_report")

    assert frame.empty


def test_최신버전_쿼리는_스키마_필드_순서로_컬럼을_고른다():
    query = _latest_version_query("monthly_report", ["year_month", "is_rerun"])

    assert "t.year_month" in query
    assert "t.is_rerun" in query
    assert "MAX(version)" in query
    assert "WHERE year_month = t.year_month" in query


def _sqlite_conn_with(table: str, columns: list[str], rows: list[dict]) -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute(f"CREATE TABLE {table} ({', '.join(columns)})")
    placeholders = ", ".join("?" for _ in columns)
    for row in rows:
        conn.execute(
            f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})",
            [row[column] for column in columns],
        )
    conn.commit()
    return conn


def _monthly_report_row(year_month: str, version: int, **overrides) -> dict:
    row = {
        "version": version,
        "year_month": year_month,
        "threshold_profit_increase": 500.0,
        "is_rerun": False,
        "recommended_driver_count": 1,
        "avg_net_profit_increase_per_driver": 10.0,
        "avg_revenue_increase_per_driver": 5.0,
        "total_revenue_increase": 100.0,
    }
    row.update(overrides)
    return row


def test_RDS_소스는_컬럼을_스키마에서_자동생성한다(monkeypatch):
    columns = [field.name for field in fields(MonthlyReport)]
    conn = _sqlite_conn_with(
        "monthly_report",
        columns,
        [_monthly_report_row("2026-05", 1, total_revenue_increase=999.0)],
    )
    monkeypatch.setattr("datasource.psycopg2.connect", lambda dsn: conn)

    frame = RdsDataSource("dsn").load("monthly_report")

    assert set(frame.columns) == set(columns)
    assert frame.loc[0, "total_revenue_increase"] == 999.0


def test_RDS_소스는_year_month별_최신_버전만_읽는다(monkeypatch):
    columns = [field.name for field in fields(MonthlyReport)]
    conn = _sqlite_conn_with(
        "monthly_report",
        columns,
        [
            _monthly_report_row("2026-05", 1, total_revenue_increase=100.0),
            _monthly_report_row("2026-05", 2, total_revenue_increase=200.0),
            _monthly_report_row("2026-06", 1, total_revenue_increase=300.0),
        ],
    )
    monkeypatch.setattr("datasource.psycopg2.connect", lambda dsn: conn)

    frame = RdsDataSource("dsn").load("monthly_report")

    assert sorted(frame["year_month"]) == ["2026-05", "2026-06"]
    by_month = frame.set_index("year_month")
    assert by_month.loc["2026-05", "total_revenue_increase"] == 200.0


def _driver_aggregation_row(driver_id: str, version: int, **overrides) -> dict:
    row = {
        "version": version,
        "driver_id": driver_id,
        "year_month": "2026-05",
        "comfort_eligible": False,
        "extra_comfort_eligible": False,
        "taxi_id": "T1",
        "vehicle_model_id": "V1",
        "manufacturer": "KIA",
        "model_name": "K5",
        "model_year": 2023,
        "fuel_efficiency": 30.0,
        "monthly_mileage": 1000.0,
        "monthly_driver_pay": 1000.0,
        "monthly_tips": 10.0,
        "monthly_fuel_cost": 50.0,
        "monthly_lease_fee": 200.0,
        "monthly_net_profit": 700.0,
    }
    row.update(overrides)
    return row


def test_RDS_소스는_같은_버전_안의_여러_행을_모두_읽는다(monkeypatch):
    columns = [field.name for field in fields(DriverMonthlyProfit)]
    conn = _sqlite_conn_with(
        "driver_aggregation",
        columns,
        [
            _driver_aggregation_row("D1", 1),
            _driver_aggregation_row("D2", 1),
            _driver_aggregation_row("D1", 2),
            _driver_aggregation_row("D2", 2),
        ],
    )
    monkeypatch.setattr("datasource.psycopg2.connect", lambda dsn: conn)

    frame = RdsDataSource("dsn").load("driver_aggregation")

    assert sorted(frame["driver_id"]) == ["D1", "D2"]


def test_알수없는_dataset이름은_RDS에서_ValueError(monkeypatch):
    monkeypatch.setattr(
        "datasource.psycopg2.connect", lambda dsn: sqlite3.connect(":memory:")
    )

    with pytest.raises(ValueError, match="알 수 없는 Gold 데이터셋"):
        RdsDataSource("dsn").load("nonexistent")


def test_기본값은_local_소스다(monkeypatch, tmp_path):
    monkeypatch.delenv("DASHBOARD_DATA_SOURCE", raising=False)
    monkeypatch.setenv("GOLD_DIR", str(tmp_path))

    source = build_data_source()

    assert isinstance(source, LocalCsvDataSource)


def test_rds_지정시_dsn_없으면_ValueError(monkeypatch):
    monkeypatch.setenv("DASHBOARD_DATA_SOURCE", "rds")
    monkeypatch.delenv("GOLD_DATABASE_URL", raising=False)

    with pytest.raises(ValueError, match="GOLD_DATABASE_URL"):
        build_data_source()


def test_알수없는_DASHBOARD_DATA_SOURCE값은_ValueError(monkeypatch):
    monkeypatch.setenv("DASHBOARD_DATA_SOURCE", "s3")

    with pytest.raises(ValueError, match="알 수 없는 DASHBOARD_DATA_SOURCE"):
        build_data_source()
