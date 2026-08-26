"""대시보드 Gold 데이터 소스 전환. 이슈 #778.

1. 로컬 CSV 파티션을 모두 이어붙인다
2. 로컬 파티션이 하나도 없으면 빈 데이터프레임
3. RDS 쿼리 컬럼은 노출된 Gold 데이터셋의 schema.gold 필드에서 자동 생성된다
4. RDS는 service_area·year_month별 최신 version 행만 읽는다
5. RDS는 같은 (year_month, version) 안의 여러 행(기사별)을 전부 읽는다
6. 알 수 없는 dataset 이름은 RdsDataSource에서 즉시 ValueError
7. DASHBOARD_DATA_SOURCE 기본값은 local
8. DASHBOARD_DATA_SOURCE=rds인데 GOLD_DATABASE_URL이 없으면 즉시 ValueError
9. 알 수 없는 DASHBOARD_DATA_SOURCE 값은 즉시 ValueError
10. silver_lineage는 중앙 스키마의 실행·코드·설정 식별자를 최신 버전에서 읽는다
"""

import sqlite3
from dataclasses import fields

import pandas as pd
import pytest

from datasource import (
    LocalCsvDataSource,
    RdsDataSource,
    _TABLE_MODELS,
    _latest_version_query,
    build_data_source,
)
from schema.gold import (
    DriverAggregation,
    DriverCarSuggestion,
    RecommendationAlgorithm,
    SilverLineage,
)


def test_RDS_소스는_현재_Gold_4종만_노출한다():
    assert set(_TABLE_MODELS) == {
        "driver_aggregation",
        "driver_car_suggestion",
        "silver_lineage",
        "recommendation_algorithm",
    }


def test_로컬_소스는_모든_파티션을_이어붙인다(tmp_path):
    for service_area, year_month in (("NYC", "2026-01"), ("TX", "2026-02")):
        partition = (
            tmp_path
            / "lease_vehicle_inventory"
            / f"service_area={service_area}"
            / f"year_month={year_month}"
        )
        partition.mkdir(parents=True)
        pd.DataFrame(
            [{"service_area": service_area, "year_month": year_month}]
        ).to_csv(
            partition / "lease_vehicle_inventory.csv", index=False
        )

    frame = LocalCsvDataSource(tmp_path).load("lease_vehicle_inventory")

    assert sorted(frame["year_month"]) == ["2026-01", "2026-02"]
    assert sorted(frame["service_area"]) == ["NYC", "TX"]


def test_로컬_소스는_파티션이_없으면_빈_데이터프레임(tmp_path):
    frame = LocalCsvDataSource(tmp_path).load("lease_vehicle_inventory")

    assert frame.empty


def test_최신버전_쿼리는_스키마_필드_순서로_컬럼을_고른다():
    query = _latest_version_query(
        "lease_vehicle_inventory", ["year_month", "vehicle_model_id"]
    )

    assert "year_month, vehicle_model_id" in query
    # 파티션당 MAX(version)을 한 번만 계산한다 — 상관 서브쿼리로 행마다 반복
    # 계산하지 않는다(#1069).
    assert "MAX(t.version) OVER (PARTITION BY t.service_area, t.year_month)" in query
    assert "WHERE version = partition_latest_version" in query


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


def _driver_aggregation_row(driver_id: str, version: int, **overrides) -> dict:
    row = {
        "version": version,
        "driver_id": driver_id,
        "year_month": "2026-05",
        "service_area": "NYC",
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


def test_RDS_소스는_컬럼을_스키마에서_자동생성한다(monkeypatch):
    columns = [field.name for field in fields(DriverAggregation)]
    conn = _sqlite_conn_with(
        "driver_aggregation",
        columns,
        [_driver_aggregation_row("D1", 1, monthly_net_profit=999)],
    )
    monkeypatch.setattr("datasource.psycopg2.connect", lambda dsn: conn)

    frame = RdsDataSource("dsn").load("driver_aggregation")

    assert set(frame.columns) == set(columns)
    assert frame.loc[0, "monthly_net_profit"] == 999


def test_RDS_소스는_지역과_year_month별_최신_버전만_읽는다(monkeypatch):
    columns = [field.name for field in fields(DriverAggregation)]
    conn = _sqlite_conn_with(
        "driver_aggregation",
        columns,
        [
            _driver_aggregation_row("D1", 1, monthly_net_profit=100),
            _driver_aggregation_row("D1", 2, monthly_net_profit=200),
            _driver_aggregation_row(
                "D1", 1, service_area="TX", monthly_net_profit=400
            ),
            _driver_aggregation_row(
                "D1", 1, year_month="2026-06", monthly_net_profit=300
            ),
        ],
    )
    monkeypatch.setattr("datasource.psycopg2.connect", lambda dsn: conn)

    frame = RdsDataSource("dsn").load("driver_aggregation")

    assert len(frame) == 3
    by_area_month = frame.set_index(["service_area", "year_month"])
    assert by_area_month.loc[("NYC", "2026-05"), "monthly_net_profit"] == 200
    assert by_area_month.loc[("TX", "2026-05"), "monthly_net_profit"] == 400


def test_RDS_소스는_같은_버전_안의_여러_행을_모두_읽는다(monkeypatch):
    columns = [field.name for field in fields(DriverAggregation)]
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


def _driver_car_suggestion_row(driver_id: str, version: int, **overrides) -> dict:
    row = {
        "version": version,
        "driver_id": driver_id,
        "year_month": "2026-05",
        "service_area": "NYC",
        "recommendation_algorithm_version_id": 1,
        "threshold": -1,
        "comfort_eligible": False,
        "extra_comfort_eligible": False,
        "vehicle_model_id": "V1",
        "manufacturer": "KIA",
        "model_name": "K5",
        "model_year": 2023,
        "recommendation_reason": "연비",
        "fuel_efficiency": 30.0,
        "recommended_monthly_lease_fee": 200.0,
        "expected_monthly_fuel_cost": 40.0,
        "expected_monthly_net_profit": 800.0,
        "expected_net_profit_increase": 100.0,
        "expected_revenue_increase": 20.0,
    }
    row.update(overrides)
    return row


def test_RDS_소스는_driver_car_suggestion을_읽는다(monkeypatch):
    columns = [field.name for field in fields(DriverCarSuggestion)]
    conn = _sqlite_conn_with(
        "driver_car_suggestion",
        columns,
        [_driver_car_suggestion_row("D1", 1, expected_net_profit_increase=999)],
    )
    monkeypatch.setattr("datasource.psycopg2.connect", lambda dsn: conn)

    frame = RdsDataSource("dsn").load("driver_car_suggestion")

    assert set(frame.columns) == set(columns)
    assert frame.loc[0, "expected_net_profit_increase"] == 999


def test_RDS_소스는_알고리즘_버전별로_최신_버전을_따로_읽는다(monkeypatch):
    """알고리즘 A 가 v1, 알고리즘 B 가 나중에 v2 로 재실행돼도 A 의 이력이 가려지면 안 된다."""
    columns = [field.name for field in fields(DriverCarSuggestion)]
    conn = _sqlite_conn_with(
        "driver_car_suggestion",
        columns,
        [
            _driver_car_suggestion_row(
                "D1", 1, recommendation_algorithm_version_id=1,
                expected_net_profit_increase=100,
            ),
            _driver_car_suggestion_row(
                "D1", 2, recommendation_algorithm_version_id=2,
                expected_net_profit_increase=200,
            ),
        ],
    )
    monkeypatch.setattr("datasource.psycopg2.connect", lambda dsn: conn)

    frame = RdsDataSource("dsn").load("driver_car_suggestion")

    assert len(frame) == 2
    by_algorithm = frame.set_index("recommendation_algorithm_version_id")
    assert by_algorithm.loc[1, "expected_net_profit_increase"] == 100
    assert by_algorithm.loc[2, "expected_net_profit_increase"] == 200


def test_RDS_소스는_최신_SilverLineage의_실행코드설정_식별자를_읽는다(
    monkeypatch,
):
    columns = [field.name for field in fields(SilverLineage)]

    def lineage_row(version: int, **overrides) -> dict:
        row = {
            "version": version,
            "service_area": "NYC",
            "year_month": "2026-05",
            "airflow_run_id": f"scheduled__v{version}",
            "code_sha": f"sha{version}",
            "config_hash": f"config{version}",
            "silver_monthly_taxi_trip_s3_link": "s3://silver/trips/v1",
            "silver_driver_vehicle_monthly_snapshot_s3_link": "s3://silver/drivers/v1",
            "silver_lease_vehicle_inventory_s3_link": "s3://silver/inventory/v1",
            "silver_gas_ev_price_s3_link": "s3://silver/fuel/v1",
        }
        row.update(overrides)
        return row

    conn = _sqlite_conn_with(
        "silver_lineage",
        columns,
        [lineage_row(1), lineage_row(2)],
    )
    monkeypatch.setattr("datasource.psycopg2.connect", lambda dsn: conn)

    frame = RdsDataSource("dsn").load("silver_lineage")

    assert list(frame["version"]) == [2]
    assert frame.loc[0, ["airflow_run_id", "code_sha", "config_hash"]].to_dict() == {
        "airflow_run_id": "scheduled__v2",
        "code_sha": "sha2",
        "config_hash": "config2",
    }


def test_RDS_소스는_버전_개념이_없는_마스터_테이블은_전체를_읽는다(monkeypatch):
    columns = [field.name for field in fields(RecommendationAlgorithm)]
    conn = _sqlite_conn_with(
        "recommendation_algorithm",
        columns,
        [
            {"recommendation_algorithm_version_id": 1, "description": "초기 배정"},
            {"recommendation_algorithm_version_id": 2, "description": "매출 우선"},
        ],
    )
    monkeypatch.setattr("datasource.psycopg2.connect", lambda dsn: conn)

    frame = RdsDataSource("dsn").load("recommendation_algorithm")

    assert sorted(frame["recommendation_algorithm_version_id"]) == [1, 2]


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


def test_연결이_끊기면_다시_맺어_이어간다(monkeypatch):
    """연결 하나를 프로세스 내내 재사용하므로 한 번 끊기면 이후 모든 조회가
    `InterfaceError: connection already closed` 로 죽었다 — 컨테이너를 다시 띄우기
    전까지 대시보드가 통째로 에러였다. RDS 재기동·유휴 타임아웃·네트워크 순단으로
    흔히 끊긴다.
    """
    import psycopg2

    columns = [field.name for field in fields(DriverAggregation)]
    conns = [
        _sqlite_conn_with("driver_aggregation", columns,
                          [_driver_aggregation_row("D1", 1)]),
        _sqlite_conn_with("driver_aggregation", columns,
                          [_driver_aggregation_row("D2", 1)]),
    ]
    made = []

    def connect(dsn):
        made.append(dsn)
        return conns[len(made) - 1]

    monkeypatch.setattr("datasource.psycopg2.connect", connect)
    source = RdsDataSource("dsn")
    assert len(source.load("driver_aggregation")) == 1

    # 죽은 연결을 흉내낸다 — 커서를 열자마자 InterfaceError.
    class Dead:
        def cursor(self):
            raise psycopg2.InterfaceError("connection already closed")

        def close(self):
            raise psycopg2.InterfaceError("connection already closed")

    source._conn = Dead()

    frame = source.load("driver_aggregation")
    assert frame["driver_id"].tolist() == ["D2"], "새 연결로 다시 읽어야 합니다"
    assert len(made) == 2, "정확히 한 번만 다시 맺어야 합니다"


def test_다시_맺어도_실패하면_그대로_올린다(monkeypatch):
    """진짜 못 붙는 상황까지 삼키면 화면에 빈 표가 뜨고 원인을 못 찾는다."""
    import psycopg2

    def connect(dsn):
        raise psycopg2.OperationalError("could not connect to server")

    monkeypatch.setattr("datasource.psycopg2.connect", connect)
    with pytest.raises(psycopg2.OperationalError):
        RdsDataSource("dsn")
