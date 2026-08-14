"""기사 배정 운행 Silver 생성 시나리오. 이슈 #300.

1. 배정된 trip만 운행·기사 선호·고객·계약·택시와 결합
2. 운행일이 계약 기간에 포함되고 PK/FK·행 수 보존
3. 대상 월·스냅샷일·seed·버전 계보 기록
4. 빈 배정·월 불일치·누락 관계는 명시적 실패
5. 같은 월 재실행은 해당 파티션만 교체하고 다른 월 보존
"""

from datetime import date, datetime

import pytest
from pyspark.sql.functions import lit

from common.io import SparkParquetLoader
from common.session import get_or_create_spark_session
from jobs.driver_assignment.silver_job import build_driver_trip_silver


@pytest.fixture(scope="module")
def spark():
    session = get_or_create_spark_session("test_driver_trip_silver")
    yield session
    session.stop()


def _frames(spark):
    trips = spark.createDataFrame([{
        "trip_key": "t1", "pickup_datetime": datetime(2024, 3, 4, 9),
        "dropoff_datetime": datetime(2024, 3, 4, 9, 20), "PULocationID": 1,
        "DOLocationID": 2, "trip_miles": 5.0, "trip_time": 1200,
        "driver_pay": 20.0, "platform_name": "Uber",
        "estimated_service_tier": "Standard", "year_month": "2024-03",
    }, {
        "trip_key": "unassigned", "pickup_datetime": datetime(2024, 3, 4, 10),
        "dropoff_datetime": datetime(2024, 3, 4, 10, 20), "PULocationID": 2,
        "DOLocationID": 3, "trip_miles": 4.0, "trip_time": 1200,
        "driver_pay": 18.0, "platform_name": "Uber",
        "estimated_service_tier": "Standard", "year_month": "2024-03",
    }])
    # HVFHV Silver 는 이 셋을 NULL 자리표시로 들고 있습니다 — 채우는 것이 이 job
    # 입니다. 픽스처에도 있어야 배정 결과와의 이름 충돌을 재현할 수 있습니다.
    # 전량 NULL 이라 타입을 명시해야 createDataFrame 이 추론에 실패하지 않습니다.
    for placeholder in ("driver_id", "taxi_id", "taxi_model_id"):
        trips = trips.withColumn(placeholder, lit(None).cast("string"))
    assignments = spark.createDataFrame([{
        "trip_key": "t1", "driver_id": "d1", "taxi_id": "taxi1",
        "trip_sequence": 1, "deadhead_minutes": 0.0, "preference_score": 0.9,
    }])
    preferences = spark.createDataFrame([{
        "driver_id": "d1", "active_weekdays": ["MON"],
        "preferred_time_blocks": ["09-12"], "preferred_distance_miles": 5.0,
        "airport_preference": 0.2, "manhattan_preference": 0.8,
        "target_daily_trips": 10, "target_work_minutes": 480,
        "max_deadhead_minutes": 15,
    }])
    customers = spark.createDataFrame([{
        "customer_id": "c1", "synthetic_driver_id": "d1",
        "snapshot_date": date(2024, 3, 1),
    }])
    leases = spark.createDataFrame([{
        "lease_id": "l1", "customer_id": "c1", "taxi_id": "taxi1",
        "lease_started_on": date(2024, 1, 1), "lease_ended_on": date(2099, 1, 1),
        "snapshot_date": date(2024, 3, 1),
    }])
    taxis = spark.createDataFrame([{
        "taxi_id": "taxi1", "make_key": "Toyota", "model_key": "Camry",
        "model_year": 2023, "vehicle_group": "STANDARD",
        "uber_comfort_eligible": False, "lyft_extra_comfort_eligible": False,
        "snapshot_date": date(2024, 3, 1),
    }])
    return trips, assignments, preferences, customers, leases, taxis


def _build(spark, frames=None, **kwargs):
    return build_driver_trip_silver(
        *(frames or _frames(spark)), year_month="2024-03",
        snapshot_date=date(2024, 3, 1), seed=42, **kwargs,
    )


def test_배정된_trip만_모든_관계와_결합하고_계보를_기록한다(spark):
    row = _build(spark).first()

    assert row.trip_key == "t1" and row.driver_id == "d1"
    assert (row.customer_id, row.lease_id, row.taxi_id) == ("c1", "l1", "taxi1")
    assert (row.make_key, row.model_key, row.model_year) == ("Toyota", "Camry", 2023)
    assert row.active_weekdays == ["MON"] and row.target_work_minutes == 480
    assert (row.year_month, row.snapshot_date, row.assignment_seed) == ("2024-03", date(2024, 3, 1), 42)
    assert row.assignment_version == "v1"


@pytest.mark.parametrize("violation", ["empty", "month", "mixed_month", "missing_driver", "expired"])
def test_빈_배정_월불일치_관계누락_계약기간위반은_ValueError다(spark, violation):
    frames = list(_frames(spark))
    if violation == "empty":
        frames[1] = frames[1].limit(0)
    elif violation == "month":
        frames[0] = frames[0].withColumn("year_month", lit("2024-02"))
    elif violation == "mixed_month":
        frames[0] = frames[0].unionByName(frames[0].limit(1).withColumn("year_month", lit("2024-02")))
    elif violation == "missing_driver":
        frames[2] = frames[2].limit(0)
    else:
        frames[4] = frames[4].withColumn("lease_ended_on", frames[4].lease_started_on)

    with pytest.raises(ValueError):
        _build(spark, frames)


def test_같은월_재실행은_해당월만_교체하고_다른월은_보존한다(spark, tmp_path):
    loader = SparkParquetLoader(str(tmp_path), partition_by=["year_month"])
    march = _build(spark)
    february = march.withColumn("year_month", lit("2024-02"))
    loader.write(february)
    loader.write(march)
    loader.write(march)

    restored = spark.read.parquet(str(tmp_path))
    assert {row.year_month: row["count"] for row in restored.groupBy("year_month").count().collect()} == {
        "2024-02": 1, "2024-03": 1,
    }


# --- 파티션 컬럼 복원 -----------------------------------------------------
#
# DAG 는 `.../hvfhv/year_month=2026-06` 처럼 파티션 디렉터리를 직접 넘깁니다.
# 그 경로를 그냥 읽으면 `year_month` 가 디렉터리 이름에만 있고 parquet 안에는
# 없어서 컬럼이 사라지고, 검증과 출력 파티셔닝이 UNRESOLVED_COLUMN 으로 죽습니다.


def test_파티션_디렉터리를_직접_넘겨도_year_month_가_살아있다(spark, tmp_path):
    from jobs.driver_assignment.silver_job import read_trips

    root = tmp_path / "hvfhv"
    spark.createDataFrame([{"trip_key": "t1", "year_month": "2026-06"}]).write.partitionBy(
        "year_month"
    ).parquet(str(root))

    trips = read_trips(spark, str(root / "year_month=2026-06"))

    assert "year_month" in trips.columns
    assert [row.year_month for row in trips.collect()] == ["2026-06"]


def test_데이터셋_루트를_넘기면_그대로_읽는다(spark, tmp_path):
    from jobs.driver_assignment.silver_job import read_trips

    root = tmp_path / "hvfhv_root"
    spark.createDataFrame([{"trip_key": "t1", "year_month": "2026-06"}]).write.partitionBy(
        "year_month"
    ).parquet(str(root))

    trips = read_trips(spark, str(root))

    assert "year_month" in trips.columns


def test_결과에_같은_이름의_컬럼이_두_번_들어가지_않는다(spark):
    """`select` 는 중복 이름을 허용해 조용히 지나가고 쓰기에서야 죽습니다.

    HVFHV Silver 의 NULL 자리표시(`driver_id` 등)를 빼지 않으면 배정 결과의
    같은 이름 컬럼과 겹쳐 COLUMN_ALREADY_EXISTS 로 적재가 실패합니다.
    """
    frames = _frames(spark)

    columns = build_driver_trip_silver(
        *frames, year_month="2024-03", snapshot_date=date(2024, 3, 1), seed=42
    ).columns

    duplicated = sorted({name for name in columns if columns.count(name) > 1})
    assert not duplicated, f"중복 컬럼: {duplicated}"
