"""월별 가짜 기사-운행 원천 생성 시나리오. 이슈 #452.

1. 월별 상태 갱신 → 월초에 기존 기사 0.5~1% 이탈·동일 수 신규 유입
2. 배정 결과 분리 → HVFHV+taxi_id, 기사 리스, 보유 차량 데이터
3. 릴리스 재실행 → 완결된 기존 결과를 중복 생성하지 않음
4. 정제 코드 소유권 → source job의 HVFHV 정제는 shared/ 를 그대로 쓰고, sub/ 자체
   스키마(schema/source)는 shared/ 가 쓰는 main 쪽 스키마와 구조가 같아야 함
"""

from datetime import date, datetime

import numpy as np
import pandas as pd
import pytest
from pyspark.sql.functions import lit

from schema.source.hvfhv import FINAL_SCHEMA
from schema.source import (
    DRIVER_VEHICLE_LEASE_SCHEMA,
    LEASE_VEHICLE_INVENTORY_SCHEMA,
)
from shared.spark.common.session import get_or_create_spark_session
from shared.spark.hvfhv_clean_transformer import FINAL_SCHEMA as SOURCE_FINAL_SCHEMA
from sub.generators.synthetic_company_snapshot.snapshot import (
    build_company_snapshot,
    build_vehicle_pool,
    read_snapshot,
    write_snapshot,
)
from sub.generators.synthetic_driver_trip_source import monthly
from sub.spark.jobs.driver_assignment.source_job import (
    INVENTORY_COLUMNS,
    LEASE_SOURCE_COLUMNS,
    _apply_test_row_limit,
    _test_scoped_root,
    add_trip_keys,
    build_driver_vehicle_leases,
    build_lease_vehicle_inventory,
    build_trip_source,
    write_source_release,
)
from sub.spark.jobs.driver_master.preference import build_driver_preferences


def test_가짜원천_정제는_중앙_Silver_스키마와_구조가_같다():
    assert SOURCE_FINAL_SCHEMA == FINAL_SCHEMA
    assert LEASE_SOURCE_COLUMNS == DRIVER_VEHICLE_LEASE_SCHEMA.names
    assert INVENTORY_COLUMNS == LEASE_VEHICLE_INVENTORY_SCHEMA.names


def test_임시행제한은_입력과_출력경로를_프로덕션에서_분리한다(tmp_path):
    class Frame:
        def __init__(self):
            self.limit_value = None

        def limit(self, value):
            self.limit_value = value
            return self

    frame = Frame()
    assert _apply_test_row_limit(frame, 0) is frame
    assert frame.limit_value is None
    assert _apply_test_row_limit(frame, 20_000) is frame
    assert frame.limit_value == 20_000
    assert _test_scoped_root(tmp_path, 20_000) == (
        tmp_path / "_temporary" / "test_row_limit=20000"
    )

    with pytest.raises(ValueError, match="0 이상"):
        _apply_test_row_limit(frame, -1)


@pytest.fixture(scope="module")
def spark():
    session = get_or_create_spark_session("test_synthetic_driver_trip_source")
    yield session
    session.stop()


def _driver_ids() -> list[str]:
    return [f"DRIVER_{index:06d}" for index in range(2_000)]


def _vehicle_master() -> pd.DataFrame:
    rows = [
        {"make_key": "A", "model_key": "BOTH", "platform": "uber", "product": "Comfort"},
        {"make_key": "A", "model_key": "BOTH", "platform": "lyft", "product": "Extra Comfort"},
        {"make_key": "B", "model_key": "STANDARD", "platform": "uber", "product": "UberX"},
        {"make_key": "C", "model_key": "UBER_ONLY", "platform": "uber", "product": "Comfort"},
        {"make_key": "D", "model_key": "LYFT_ONLY", "platform": "lyft", "product": "Extra Comfort"},
    ]
    prices = {"BOTH": 700.0, "STANDARD": 500.0, "UBER_ONLY": 600.0, "LYFT_ONLY": 650.0}
    return pd.DataFrame([
        {
            **row,
            "vendor": "fasttrack",
            "min_year": 2020,
            "weekly_lease_fee": prices[row["model_key"]],
        }
        for row in rows
    ])


def _bootstrap_pools() -> dict[str, np.ndarray]:
    return {
        "trip_miles": np.array([1.0, 3.0, 8.0]),
        "trip_time_min": np.array([10.0, 20.0, 40.0]),
    }


def test_월별_상태는_월초에_기존기사를_내보내고_같은수의_신규기사를_받는다(
    tmp_path, monkeypatch
):
    previous_date = date(2026, 8, 1)
    target_date = date(2026, 9, 1)
    vehicle_master = _vehicle_master()
    pool = build_vehicle_pool(vehicle_master)
    previous = build_company_snapshot(
        _driver_ids(), pool, snapshot_date=previous_date
    )
    previous_root = tmp_path / "previous"
    previous_dir = previous_root / f"snapshot_date={previous_date}"
    write_snapshot(previous, previous_root, previous_date)
    preferences = build_driver_preferences(
        _driver_ids(), _bootstrap_pools(), as_of_date=np.datetime64(previous_date)
    )
    previous_preferences = previous_dir / "driver_preferences.parquet"
    preferences.to_parquet(previous_preferences, index=False)
    monkeypatch.setattr(monthly, "load_bootstrap_pools", lambda **_: _bootstrap_pools())

    result = monthly.prepare_monthly_state(
        previous_snapshot_dir=previous_dir,
        previous_preferences_path=previous_preferences,
        hvfhv_input_dir=tmp_path / "source-input",
        output_dir=tmp_path / "state",
        snapshot_date=target_date,
        seed=42,
        change_rate=0.005,
    )
    rerun = monthly.prepare_monthly_state(
        previous_snapshot_dir=previous_dir,
        previous_preferences_path=previous_preferences,
        hvfhv_input_dir=tmp_path / "source-input",
        output_dir=tmp_path / "state",
        snapshot_date=target_date,
        seed=42,
        change_rate=0.005,
    )

    current = read_snapshot(result.snapshot_dir)
    ended = current.lease_contract[current.lease_contract["lease_ended_on"].notna()]
    active = current.lease_contract[current.lease_contract["lease_ended_on"].isna()]
    new_drivers = set(current.customer["synthetic_driver_id"]) - set(previous.customer["synthetic_driver_id"])
    assert len(ended) == len(new_drivers) == 10
    assert set(ended["lease_ended_on"]) == {target_date}
    assert set(active.loc[active["lease_started_on"] == target_date, "lease_started_on"]) == {target_date}
    assert all(driver.startswith("DRIVER_202609_") for driver in new_drivers)
    assert len(active) == 2_000
    assert result.snapshot_dir.name == "data_month=2026-09"
    assert rerun == result


def _raw_trip(pickup: datetime, **overrides) -> dict:
    row = {
        "hvfhs_license_num": "HV0003",
        "dispatching_base_num": "B1",
        "originating_base_num": "B1",
        "request_datetime": pickup,
        "on_scene_datetime": pickup,
        "pickup_datetime": pickup,
        "dropoff_datetime": pickup.replace(minute=pickup.minute + 10),
        "PULocationID": 1,
        "DOLocationID": 2,
        "trip_miles": 3.0,
        "trip_time": 600,
        "base_passenger_fare": 12.0,
        "driver_pay": 9.0,
    }
    row.update(overrides)
    return row


def test_배정결과를_HVFHV와_기사차량리스_원천으로_분리한다(spark):
    raw = spark.createDataFrame([
        _raw_trip(datetime(2026, 1, 2, 9)),
        _raw_trip(datetime(2026, 1, 2, 10)),
    ])
    keyed = add_trip_keys(raw)
    assignment = keyed.orderBy("pickup_datetime").limit(1).select("trip_key").withColumn(
        "taxi_id", lit("taxi-1")
    )
    snapshot_date = date(2026, 1, 1)
    customers = spark.createDataFrame([{
        "customer_id": "customer-1", "synthetic_driver_id": "driver-1",
        "snapshot_date": snapshot_date,
    }])
    leases = spark.createDataFrame([{
        "lease_id": "lease-1", "customer_id": "customer-1", "taxi_id": "taxi-1",
        "lease_started_on": snapshot_date, "lease_ended_on": date(2026, 2, 1),
        "snapshot_date": snapshot_date,
    }])
    taxis = spark.createDataFrame([{
        "taxi_id": "taxi-1", "make_key": "Toyota", "model_key": "Camry",
        "model_year": 2023, "snapshot_date": snapshot_date,
    }])

    trips = build_trip_source(raw, assignment)
    driver_leases = build_driver_vehicle_leases(
        customers, leases, taxis, snapshot_date=snapshot_date
    )

    trip = trips.first()
    lease = driver_leases.first()
    assert trip.taxi_id == "taxi-1" and "trip_key" not in trips.columns
    assert trip.request_datetime == datetime(2026, 1, 2, 9)
    assert (lease.driver_id, lease.taxi_id) == ("driver-1", "taxi-1")
    assert (lease.make_key, lease.model_key, lease.model_year) == ("Toyota", "Camry", 2023)


def test_보유차량은_이미지의_11개컬럼으로_차종별_재고를_집계한다(spark):
    snapshot_date = date(2026, 1, 1)
    taxis = spark.createDataFrame(
        [
            (taxi_id, "KIA", "SPORTAGE", 2023, 574.0, True, False, snapshot_date)
            for taxi_id in ("taxi-1", "taxi-2")
        ],
        "taxi_id string, make_key string, model_key string, model_year int, "
        "weekly_lease_fee double, uber_comfort_eligible boolean, "
        "lyft_extra_comfort_eligible boolean, snapshot_date date",
    )
    vehicle_master = spark.createDataFrame(
        [
            (
                "KIA",
                "SPORTAGE",
                "GAS",
                24.0,
                28.0,
                "https://example.com/sportage.png",
                product,
            )
            for product in ("UberX", "Comfort")
        ],
        "make_key string, model_key string, fuel_type string, combined_mpg_min double, "
        "combined_mpg_max double, image_url string, product string",
    )

    inventory = build_lease_vehicle_inventory(
        taxis, vehicle_master, snapshot_date=snapshot_date
    )
    row = inventory.first()

    assert inventory.columns == LEASE_VEHICLE_INVENTORY_SCHEMA.names
    assert inventory.count() == 1
    assert row.stock == 2
    assert row.fuel_efficiency == 26.0
    assert row.manufacturer == "KIA" and row.model_name == "SPORTAGE"
    assert row.comfort_eligible is True and row.extra_comfort_eligible is False
    assert row.weekly_lease_fee == 574.0
    assert row.image_url == "https://example.com/sportage.png"
    assert row.vehicle_model_id


def test_완결된_릴리스를_같은_입력으로_다시_써도_중복되지_않는다(spark, tmp_path):
    trips = spark.createDataFrame([{
        "pickup_datetime": datetime(2026, 1, 2, 9), "taxi_id": "taxi-1"
    }])
    leases = spark.createDataFrame([{
        "lease_id": "lease-1", "driver_id": "driver-1", "taxi_id": "taxi-1",
        "lease_started_on": date(2026, 1, 1), "lease_ended_on": date(2026, 2, 1),
    }])
    inventory = spark.createDataFrame(
        [
            (
                "vehicle-model-1",
                "KIA",
                "SPORTAGE",
                2023,
                "GAS",
                26.0,
                True,
                False,
                574.0,
                "https://example.com/sportage.png",
                1,
            )
        ],
        INVENTORY_COLUMNS,
    )

    first = write_source_release(
        trips, leases, inventory, output_dir=tmp_path, year_month="2026-01", seed=42
    )
    second = write_source_release(
        trips, leases, inventory, output_dir=tmp_path, year_month="2026-01", seed=42
    )

    assert first == second
    assert len(list(tmp_path.glob("year_month=2026-01"))) == 1
    assert spark.read.parquet(str(first / "hvfhv_taxi_trips.parquet")).count() == 1
    assert spark.read.parquet(str(first / "driver_vehicle_leases.parquet")).count() == 1
    assert spark.read.parquet(str(first / "lease_vehicle_inventory.parquet")).count() == 1
    assert (first / "manifest.json").is_file()
