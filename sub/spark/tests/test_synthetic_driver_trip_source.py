"""월별 가짜 기사-운행 원천 생성 시나리오. 이슈 #452.

1. 월별 상태 갱신 → 월초에 기존 기사 0.5~1% 이탈·동일 수 신규 유입
2. 배정 결과 분리 → HVFHV+taxi_id, 기사 리스, 보유 차량 데이터
3. 릴리스 재실행 → 완결된 기존 결과를 중복 생성하지 않음
4. 정제 코드 소유권 → source job의 HVFHV 정제는 shared/ 를 그대로 쓰고, sub/ 자체
   스키마(schema/source)는 shared/ 가 쓰는 main 쪽 스키마와 구조가 같아야 함
"""

from datetime import date, datetime
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import pytest
from pyspark.sql.functions import lit

from conftest import TEST_CONFIG_DATA

from schema.source.hvfhv import FINAL_SCHEMA
from schema.source import (
    DRIVER_VEHICLE_MONTHLY_SNAPSHOT_SCHEMA,
    LEASE_VEHICLE_INVENTORY_SCHEMA,
    MONTHLY_TAXI_TRIP_SCHEMA,
)
from shared.spark.common.session import get_or_create_spark_session
from shared.spark.hvfhv_clean_transformer import FINAL_SCHEMA as SOURCE_FINAL_SCHEMA
from sub.config import build_config
from sub.generators.synthetic_driver_trip_source import monthly
from sub.run_context import RunContext
from sub.spark.jobs.driver_assignment.source_job import (
    INVENTORY_COLUMNS,
    PRIOR_EXPERIENCE_MAX_YEARS,
    SNAPSHOT_SOURCE_COLUMNS,
    TRIP_SOURCE_COLUMNS,
    _apply_test_row_limit,
    _quality_report,
    _test_scoped_root,
    _write_one_parquet_s3,
    add_trip_keys,
    build_driver_vehicle_monthly_snapshot,
    build_lease_vehicle_inventory,
    build_trip_source,
    write_source_release,
)


def test_가짜원천_정제는_중앙_Silver_스키마와_구조가_같다():
    assert SOURCE_FINAL_SCHEMA == FINAL_SCHEMA
    assert SNAPSHOT_SOURCE_COLUMNS == DRIVER_VEHICLE_MONTHLY_SNAPSHOT_SCHEMA.names
    assert TRIP_SOURCE_COLUMNS == MONTHLY_TAXI_TRIP_SCHEMA.names
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


def _bootstrap_pools() -> dict[str, np.ndarray]:
    return {
        "trip_miles": np.array([1.0, 3.0, 8.0]),
        "trip_time_min": np.array([10.0, 20.0, 40.0]),
    }


def _vehicle_master_silver() -> pd.DataFrame:
    """`schema.source.VEHICLE_MASTER_SCHEMA` 모양 (제원은 min/max 범위)."""
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
            "combined_mpg_min": 28.0, "combined_mpg_max": 32.0,
            "combined_kwh_per_100mi_min": 0.0, "combined_kwh_per_100mi_max": 0.0,
        }
        for row in rows
    ])


def _monthly_config(initial_count: int, *, snapshot_date: str = "2026-08-01"):
    """`snapshot_date` 가 곧 **첫 달**입니다.

    전월 체크포인트가 없어도 되는 달은 이 값 하나뿐입니다. 그보다 뒤인 달을
    체크포인트 없이 돌리면 `CheckpointLineageError` 로 막힙니다 — 조용히 초기
    스냅샷을 만들어 기사 연속성을 끊는 것을 방지합니다(#763).
    """
    data = {k: (dict(v) if isinstance(v, dict) else v) for k, v in TEST_CONFIG_DATA.items()}
    data["driver"] = {**data["driver"], "initial_count": initial_count}
    data["bootstrap"] = {**data["bootstrap"], "snapshot_date": snapshot_date}
    return build_config(data)


def test_월별_상태는_체크포인트로_이어지고_기존_Spark_경로가_읽는_계약을_지킨다(
    tmp_path, monkeypatch
):
    """#628 — lifecycle 정본이 event-sourced 체크포인트로 옮겨간 뒤의 계약.

    월초에 기존기사 이탈·동수 신규 유입이라는 예전 규칙(evolve_company_snapshot)
    대신, join/exit/vehicle_change가 config 비율로 독립 적용된다(D14) — 그래서
    "이탈 수 == 신규 수"를 더는 단정하지 않는다.
    """
    monkeypatch.setattr(monthly, "load_bootstrap_pools", lambda **_: _bootstrap_pools())
    vehicle_master_path = tmp_path / "vehicle_master.parquet"
    _vehicle_master_silver().to_parquet(vehicle_master_path, index=False)
    config = _monthly_config(400)  # 400 x join_rate(0.008) = 3.2 기대 — 실제 발생을 담보

    first = monthly.prepare_monthly_state(
        hvfhv_input_dir=tmp_path / "source-input",
        output_dir=tmp_path / "state",
        snapshot_date=date(2026, 8, 1),
        config=config,
        vehicle_master_path=vehicle_master_path,
    )
    second = monthly.prepare_monthly_state(
        hvfhv_input_dir=tmp_path / "source-input",
        output_dir=tmp_path / "state",
        snapshot_date=date(2026, 9, 1),
        config=config,
        vehicle_master_path=vehicle_master_path,
    )
    rerun = monthly.prepare_monthly_state(
        hvfhv_input_dir=tmp_path / "source-input",
        output_dir=tmp_path / "state",
        snapshot_date=date(2026, 9, 1),
        config=config,
        vehicle_master_path=vehicle_master_path,
    )
    assert rerun == second

    first_cdv = pd.read_parquet(first.current_driver_vehicle_path)
    second_cdv = pd.read_parquet(second.current_driver_vehicle_path)
    assert len(first_cdv) == 400
    # 경로가 str 인 이유 — `s3://` 도 담습니다(#767). `Path` 로 감싸면 스킴이 깨집니다.
    assert first.snapshot_dir.endswith("data_month=2026-08")
    assert second.snapshot_dir.endswith("data_month=2026-09")

    new_drivers = set(second_cdv["driver_id"]) - set(first_cdv["driver_id"])
    ended = second_cdv[second_cdv["lease_ended_on"].notna()]
    assert len(new_drivers) >= 1, "join_rate 가 0이 아닌데 신규 유입이 없습니다"
    assert len(ended) >= 1, "exit_rate 가 0이 아닌데 유출이 없습니다"
    # D15: 유출 기사도 행이 남아 있습니다 — lease_ended_on 만 채워짐.


def test_전원_재직중이면_퇴사일_컬럼이_date32로_쓰인다(tmp_path):
    """`lease_ended_on`이 전부 NaT(아무도 퇴사 안 함)면 pandas의 dtype 추론이
    datetime64[ns]로 남아 pyarrow가 timestamp[ns]로 쓴다 — Spark의 Parquet
    리더는 그 물리 타입(`INT64 TIMESTAMP(NANOS)`)을 거부한다. 프로덕션 pandas
    2.1.4(Airflow 컨테이너)에서 실제로 재현된 회귀."""
    frame = pd.DataFrame({
        "driver_id": ["d1", "d2"],
        "taxi_id": ["t1", "t2"],
        "joined_on": [date(2024, 1, 1), date(2024, 1, 1)],
        "lease_started_on": [date(2024, 1, 1), date(2024, 1, 1)],
        "lease_ended_on": [None, None],
        "make_key": ["A", "A"],
        "model_key": ["B", "B"],
        "model_year": [2023, 2023],
        "weekly_lease_fee": [500.0, 500.0],
        "uber_comfort_eligible": [True, True],
        "lyft_extra_comfort_eligible": [False, False],
    })
    path = tmp_path / "current_driver_vehicle.parquet"

    monthly._write_current_driver_vehicle(frame, path)

    schema = pq.read_schema(path)
    for name in ("joined_on", "lease_started_on", "lease_ended_on"):
        assert str(schema.field(name).type) == "date32[day]"


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
        "tips": 1.5,
    }
    row.update(overrides)
    return row


def _clean_trips(spark, keyed):
    """정제 결과 중 공개 계약에 필요한 컬럼만 흉내 냅니다.

    zone·등급·platform_name 은 원본에 없고 `HVFHVCleanTransformer` 가 만듭니다.
    """
    return keyed.select("trip_key").withColumns(
        {
            "pickup_zone": lit("Midtown"),
            "dropoff_zone": lit("JFK"),
            "estimated_service_tier": lit("Comfort"),
            "platform_name": lit("Uber"),
        }
    )


def _current_driver_vehicle(spark, rows):
    """`adapters.to_current_driver_vehicle()` 산출물 모양의 Spark DataFrame(#609)."""
    return spark.createDataFrame(
        rows,
        "driver_id string, taxi_id string, joined_on date, lease_started_on date, "
        "lease_ended_on date, make_key string, model_key string, model_year int, "
        "weekly_lease_fee double, uber_comfort_eligible boolean, "
        "lyft_extra_comfort_eligible boolean",
    )


def test_배정결과를_공개_계약_두_데이터셋으로_분리한다(spark):
    raw = spark.createDataFrame([
        _raw_trip(datetime(2026, 1, 2, 9)),
        _raw_trip(datetime(2026, 1, 2, 10)),
    ])
    keyed = add_trip_keys(raw)
    assignment = keyed.orderBy("pickup_datetime").limit(1).select("trip_key").withColumn(
        "taxi_id", lit("taxi-1")
    )
    snapshot_date = date(2026, 1, 1)
    current_driver_vehicle = _current_driver_vehicle(spark, [
        ("driver-1", "taxi-1", date(2024, 3, 1), date(2024, 3, 1), None,
         "Toyota", "Camry", 2023, 500.0, True, False),
    ])
    vehicle_master = spark.createDataFrame(
        [("Toyota", "Camry", "GAS")], "make_key string, model_key string, fuel_type string"
    )

    trips = build_trip_source(raw, _clean_trips(spark, keyed), assignment)
    snapshots = build_driver_vehicle_monthly_snapshot(
        current_driver_vehicle, vehicle_master,
        snapshot_date=snapshot_date, year_month="2026-01", seed=42,
    )

    trip = trips.first()
    row = snapshots.first()
    assert trips.columns == TRIP_SOURCE_COLUMNS
    assert snapshots.columns == SNAPSHOT_SOURCE_COLUMNS
    assert trip.taxi_id == "taxi-1"
    assert (trip.pickup_zone, trip.dropoff_zone) == ("Midtown", "JFK")
    assert trip.estimated_service_tier == "Comfort"
    # platform_name 을 원본 라이선스 번호로 되돌립니다.
    assert trip.hvfhs_license_num == "HV0003"
    assert (row.driver_id, row.taxi_id) == ("driver-1", "taxi-1")
    assert (row.manufacturer, row.model_name, row.fuel_type) == ("Toyota", "Camry", "GAS")
    assert row.snapshot_month == "2026-01"
    # 진행 중 계약이라 퇴사일은 비어야 합니다.
    assert row.exit_date is None
    assert row.join_date == date(2024, 3, 1) == row.vehicle_since


def test_운행_기록은_원본_컬럼을_그대로_흘려보내지_않는다(spark):
    """예전에는 TLC 원본 26컬럼을 그대로 공개했습니다.

    원본이 컬럼을 추가하면 공개 계약이 조용히 따라 늘어납니다.
    """
    raw = spark.createDataFrame([_raw_trip(datetime(2026, 1, 2, 9))])
    keyed = add_trip_keys(raw)
    assignment = keyed.select("trip_key").withColumn("taxi_id", lit("taxi-1"))

    trips = build_trip_source(raw, _clean_trips(spark, keyed), assignment)

    assert "dispatching_base_num" not in trips.columns
    assert "base_passenger_fare" not in trips.columns
    assert "trip_key" not in trips.columns


def test_차량을_바꾼_기사는_입사일과_현재_차량_배정일이_다르다(spark):
    """#609 — `joined_on`(최초 입사일)과 `vehicle_since`(현재 차량 배정일)가
    같은 기사당-한-행 뷰 안에서 이미 분리돼 있어, 리스 이력을 재구성해
    최초 계약을 다시 찾을 필요가 없습니다. 예전 3-테이블 어댑터는 기사당
    행을 하나만 만들면서 이 둘을 늘 같은 값으로 퇴화시켰습니다."""
    snapshot_date = date(2026, 1, 1)
    current_driver_vehicle = _current_driver_vehicle(spark, [
        ("driver-1", "taxi-2", date(2023, 1, 1), date(2025, 6, 1), None,
         "Toyota", "Camry", 2023, 500.0, True, False),
    ])
    vehicle_master = spark.createDataFrame(
        [("Toyota", "Camry", "GAS")], "make_key string, model_key string, fuel_type string"
    )

    snapshots = build_driver_vehicle_monthly_snapshot(
        current_driver_vehicle, vehicle_master,
        snapshot_date=snapshot_date, year_month="2026-01", seed=42,
    )

    assert snapshots.count() == 1
    row = snapshots.first()
    assert row.taxi_id == "taxi-2"                 # 현재 차량
    assert row.join_date == date(2023, 1, 1)       # 최초 입사일
    assert row.vehicle_since == date(2025, 6, 1)   # 현재 차량 배정일
    assert row.exit_date is None                   # 재직 중


def test_모든_계약이_끝난_기사는_퇴사일이_찍힌다(spark):
    snapshot_date = date(2026, 1, 1)
    current_driver_vehicle = _current_driver_vehicle(spark, [
        ("driver-1", "taxi-1", date(2023, 1, 1), date(2023, 1, 1), date(2025, 12, 1),
         "Toyota", "Camry", 2023, 500.0, True, False),
    ])
    vehicle_master = spark.createDataFrame(
        [("Toyota", "Camry", "GAS")], "make_key string, model_key string, fuel_type string"
    )

    row = build_driver_vehicle_monthly_snapshot(
        current_driver_vehicle, vehicle_master,
        snapshot_date=snapshot_date, year_month="2026-01", seed=42,
    ).first()

    assert row.exit_date == date(2025, 12, 1)


def test_경력은_근속보다_짧을_수_없고_시드마다_재현된다(spark):
    """독립 난수로 두면 "근속 5년인데 경력 1년" 이 나옵니다."""
    snapshot_date = date(2026, 1, 1)
    # 근속 16년. 입사 전 경력 상한(10년)보다 커야 "근속을 뺐다"를 잡을 수 있습니다 —
    # 근속이 상한보다 작으면 난수만으로도 우연히 기준을 넘습니다.
    current_driver_vehicle = _current_driver_vehicle(spark, [
        ("driver-1", "taxi-1", date(2010, 1, 1), date(2010, 1, 1), None,
         "Toyota", "Camry", 2023, 500.0, True, False),
    ])
    vehicle_master = spark.createDataFrame(
        [("Toyota", "Camry", "GAS")], "make_key string, model_key string, fuel_type string"
    )

    def run(seed):
        return build_driver_vehicle_monthly_snapshot(
            current_driver_vehicle, vehicle_master,
            snapshot_date=snapshot_date, year_month="2026-01", seed=seed,
        ).first().experience_years

    tenure = 16                            # 2010-01 ~ 2026-01
    assert run(42) >= tenure               # 근속을 빼면 상한 10 이라 도달 불가
    assert run(42) <= tenure + PRIOR_EXPERIENCE_MAX_YEARS
    assert run(42) == run(42)              # 같은 시드면 재현


def test_보유차량은_이미지의_11개컬럼으로_차종별_재고를_집계한다(spark):
    current_driver_vehicle = _current_driver_vehicle(spark, [
        (f"driver-{i}", taxi_id, date(2023, 1, 1), date(2023, 1, 1), None,
         "KIA", "SPORTAGE", 2023, 574.0, True, False)
        for i, taxi_id in enumerate(("taxi-1", "taxi-2"))
    ])
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

    inventory = build_lease_vehicle_inventory(current_driver_vehicle, vehicle_master)
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


def test_보유차량_재고는_재배정된_taxi_id를_두_번_세지_않는다(spark):
    """#609 — 퇴사 기사와 신규 기사가 같은 달에 같은 taxi_id 를 나눠 갖고 있으면
    (재배정) `current_driver_vehicle`에 그 taxi_id 가 두 행으로 남습니다. taxi_id
    로 먼저 dedup 하지 않으면 물리적으로 한 대인 차량이 재고 두 대로 집계됩니다."""
    current_driver_vehicle = _current_driver_vehicle(spark, [
        ("driver-1", "taxi-1", date(2020, 1, 1), date(2020, 1, 1), date(2026, 1, 15),
         "KIA", "SPORTAGE", 2023, 574.0, True, False),
        ("driver-2", "taxi-1", date(2026, 1, 15), date(2026, 1, 15), None,
         "KIA", "SPORTAGE", 2023, 574.0, True, False),
    ])
    vehicle_master = spark.createDataFrame(
        [("KIA", "SPORTAGE", "GAS", 24.0, 28.0, "https://example.com/sportage.png", "UberX")],
        "make_key string, model_key string, fuel_type string, combined_mpg_min double, "
        "combined_mpg_max double, image_url string, product string",
    )

    inventory = build_lease_vehicle_inventory(current_driver_vehicle, vehicle_master)

    assert inventory.first().stock == 1


def test_품질리포트는_커버리지_천장_소진율_클리핑을_계산한다(spark):
    """#608 — coverage/ceiling/saturation/rejection_counts/clip_rate 다섯 지표."""
    trips = spark.createDataFrame([
        {"pickup_datetime": datetime(2026, 1, 5, 9)},   # 월요일
        {"pickup_datetime": datetime(2026, 1, 5, 10)},
        {"pickup_datetime": datetime(2026, 1, 6, 9)},   # 화요일
    ])
    preferences = spark.createDataFrame([
        {"target_drive_minutes": 100, "weekday_mask": 0b011},  # 월,화 활성
    ])
    assignments = spark.createDataFrame([
        {
            "pickup_datetime": datetime(2026, 1, 5, 9),
            "dropoff_datetime": datetime(2026, 1, 5, 9, 20),
            "deadhead_minutes": 5.0,
        }
    ])
    run = RunContext.create("2026-01", _monthly_config(1))

    report = _quality_report(
        run=run,
        trips=trips,
        preferences=preferences,
        assignments=assignments,
        assignment_count=1,
        rejected={"c1": 2},
        clip_rate=0.03,
    )

    assert report["trips_offered"] == 3
    assert report["trips_attributed"] == 1
    assert report["coverage_pct"] == pytest.approx(33.33, abs=0.01)
    assert report["capacity_drive_minutes"] == 200  # 100분 x 활성 요일 2일(월,화)
    assert report["saturation_pct"] == pytest.approx(100.0 * 25.0 / 200.0, abs=0.01)
    assert report["rejection_counts"] == {"c1": 2}
    assert report["clip_rate"] == 0.03


def test_완결된_릴리스를_같은_입력으로_다시_써도_중복되지_않는다(spark, tmp_path):
    trips = spark.createDataFrame([{
        "pickup_datetime": datetime(2026, 1, 2, 9), "taxi_id": "taxi-1"
    }])
    snapshots = spark.createDataFrame([{
        "driver_id": "driver-1", "taxi_id": "taxi-1",
        "vehicle_since": date(2026, 1, 1), "exit_date": None,
    }], "driver_id string, taxi_id string, vehicle_since date, exit_date date")
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

    run = RunContext.create("2026-01", _monthly_config(50))
    first = write_source_release(
        trips, snapshots, inventory, output_dir=tmp_path, run=run, input_scope="full"
    )
    second = write_source_release(
        trips, snapshots, inventory, output_dir=tmp_path, run=run, input_scope="full"
    )

    assert first == second
    assert len(list(tmp_path.glob("year_month=2026-01"))) == 1
    assert spark.read.parquet(str(first / "hvfhv_taxi_trips.parquet")).count() == 1
    assert spark.read.parquet(str(first / "driver_vehicle_monthly_snapshot.parquet")).count() == 1
    assert spark.read.parquet(str(first / "lease_vehicle_inventory.parquet")).count() == 1
    assert (first / "manifest.json").is_file()


def _fake_hadoop_path(uri, *, fs=None):
    """`org.apache.hadoop.fs.Path` 인스턴스 흉내. `fs`가 있으면 `getFileSystem()`이 그걸 돌려줌."""
    path = MagicMock(name=f"Path<{uri}>")
    path.getFileSystem.return_value = fs
    path.getName.return_value = uri.rstrip("/").rsplit("/", 1)[-1]
    return path


def test_write_one_parquet_s3는_유일한_part_파일만_최종_key로_옮기고_스테이징을_지운다():
    """`_write_one_parquet_s3()`는 JRE가 있어야 도는 실제 Spark 대신, 그게 건드리는
    Hadoop FileSystem 표면만 흉내내 로컬에서(JRE 없이) 검증한다."""
    fs = MagicMock(name="fs")
    frame = MagicMock(name="frame")
    written_uris = []
    frame.coalesce.return_value.write.mode.return_value.parquet.side_effect = written_uris.append

    path_registry: dict[str, MagicMock] = {}

    def path_factory(uri):
        return path_registry.setdefault(uri, _fake_hadoop_path(uri, fs=fs))

    frame.sparkSession._jvm.org.apache.hadoop.fs.Path.side_effect = path_factory
    frame.sparkSession._jsc.hadoopConfiguration.return_value = "hadoop-conf"

    success_status = MagicMock()
    success_status.getPath.return_value.getName.return_value = "_SUCCESS"
    part_status = MagicMock()
    part_status.getPath.return_value = _fake_hadoop_path("part-00000-abc.snappy.parquet")
    fs.listStatus.return_value = [success_status, part_status]

    _write_one_parquet_s3(
        frame, bucket="my-bucket", key="source/attribution/year_month=2026-08/attribution.parquet"
    )

    assert len(written_uris) == 1
    staging_uri = written_uris[0]
    assert staging_uri.startswith("s3a://my-bucket/.staging/")
    assert staging_uri.endswith("/")

    final_uri = "s3a://my-bucket/source/attribution/year_month=2026-08/attribution.parquet"
    fs.listStatus.assert_called_once_with(path_registry[staging_uri])
    # _SUCCESS는 걸러지고 part- 파일만 최종 key로 rename됩니다.
    fs.rename.assert_called_once_with(part_status.getPath.return_value, path_registry[final_uri])
    # 목적지를 **먼저** 지웁니다 — Hadoop rename 은 overwrite 옵션이 없어 목적지가
    # 있으면 던집니다(#791). 그다음 staging 을 정리합니다.
    assert fs.delete.call_args_list == [
        ((path_registry[final_uri], False),),
        ((path_registry[staging_uri], True),),
    ]


def test_write_one_parquet_s3는_part_파일이_하나가_아니면_실패한다():
    fs = MagicMock(name="fs")
    frame = MagicMock(name="frame")
    frame.sparkSession._jvm.org.apache.hadoop.fs.Path.side_effect = (
        lambda uri: _fake_hadoop_path(uri, fs=fs)
    )
    part_a = MagicMock()
    part_a.getPath.return_value = _fake_hadoop_path("part-00000-a.snappy.parquet")
    part_b = MagicMock()
    part_b.getPath.return_value = _fake_hadoop_path("part-00001-b.snappy.parquet")
    fs.listStatus.return_value = [part_a, part_b]

    with pytest.raises(ValueError, match="단일 Parquet 파일을 만들지 못했습니다"):
        _write_one_parquet_s3(frame, bucket="my-bucket", key="k")


def test_write_one_parquet_s3는_목적지가_있어도_덮어쓴다():
    """부분 산출물이 남아 있어도 같은 월을 다시 발행할 수 있어야 합니다(#791).

    Hadoop `rename()` 은 목적지가 있으면 `FileAlreadyExistsException` 을 던집니다.
    로컬판 `_write_one_parquet` 은 `Path.rename()` 이라 덮어쓰므로, 여기서만
    멱등성이 깨져 그 월이 영구히 발행 불가가 됐습니다.
    """
    fs = MagicMock(name="fs")
    frame = MagicMock(name="frame")
    path_registry: dict[str, MagicMock] = {}
    frame.sparkSession._jvm.org.apache.hadoop.fs.Path.side_effect = (
        lambda uri: path_registry.setdefault(uri, _fake_hadoop_path(uri, fs=fs))
    )
    part_status = MagicMock()
    part_status.getPath.return_value = _fake_hadoop_path("part-00000-abc.snappy.parquet")
    fs.listStatus.return_value = [part_status]

    def rename(_src, destination):
        # 실제 S3A 처럼, 목적지가 안 지워졌다면 던집니다.
        if (destination, False) not in [call.args for call in fs.delete.call_args_list]:
            raise AssertionError("목적지를 지우지 않고 rename 했습니다")
        return True

    fs.rename.side_effect = rename

    _write_one_parquet_s3(frame, bucket="my-bucket", key="source/published/x/data.parquet")

    fs.rename.assert_called_once()


def test_write_one_parquet_s3는_rename이_실패해도_스테이징을_지운다():
    """안 지우면 실패한 실행마다 `.staging/` 에 아무도 안 보는 잔여물이 쌓입니다."""
    fs = MagicMock(name="fs")
    frame = MagicMock(name="frame")
    written_uris = []
    frame.coalesce.return_value.write.mode.return_value.parquet.side_effect = written_uris.append
    path_registry: dict[str, MagicMock] = {}
    frame.sparkSession._jvm.org.apache.hadoop.fs.Path.side_effect = (
        lambda uri: path_registry.setdefault(uri, _fake_hadoop_path(uri, fs=fs))
    )
    part_status = MagicMock()
    part_status.getPath.return_value = _fake_hadoop_path("part-00000-abc.snappy.parquet")
    fs.listStatus.return_value = [part_status]
    fs.rename.side_effect = RuntimeError("FileAlreadyExistsException")

    with pytest.raises(RuntimeError, match="FileAlreadyExistsException"):
        _write_one_parquet_s3(frame, bucket="my-bucket", key="k")

    staging_uri = written_uris[0]
    assert ((path_registry[staging_uri], True),) in [
        (call.args,) for call in fs.delete.call_args_list
    ]
