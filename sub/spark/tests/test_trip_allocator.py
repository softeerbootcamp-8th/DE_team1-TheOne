"""결정적 시공간 운행 배정 시나리오. 이슈 #293.

1. 운행 겹침·공차 도착 불가 후보 제외
2. 일일 운행 수·첫 승차~마지막 하차 근무시간 상한 적용
3. 운행별 단일 기사와 점수·tie-break 우선순위 보장
4. 날짜별 상태 분리와 입력 순서 무관 결정성
5. 이동시간 결측·입력 계약 위반·빈 후보 처리
"""

from datetime import datetime

import pytest

from shared.spark.common.session import get_or_create_spark_session
from sub.spark.jobs.driver_assignment.allocator import ASSIGNMENT_SCHEMA, allocate_trips


@pytest.fixture(scope="module")
def spark():
    session = get_or_create_spark_session("test_trip_allocator")
    yield session
    session.stop()


def _candidate(key, driver, pickup, dropoff, pu=1, do=2, score=0.8, tie="a", trips=10, minutes=480):
    return {
        "trip_key": key, "driver_id": driver, "taxi_id": f"taxi-{driver}",
        "pickup_datetime": pickup, "dropoff_datetime": dropoff,
        "PULocationID": pu, "DOLocationID": do,
        "preference_score": score, "tie_break": tie,
        "target_daily_trips": trips, "target_work_minutes": minutes,
        "max_deadhead_minutes": 15,
    }


def _travel(spark, rows=((2, 1, 10.0),)):
    return spark.createDataFrame(rows, "from_location_id int, to_location_id int, travel_minutes double")


def test_겹치거나_공차시간_안에_도착할_수_없는_운행은_배정하지_않는다(spark):
    rows = [
        _candidate("t1", "d1", datetime(2024, 3, 4, 9), datetime(2024, 3, 4, 9, 30)),
        _candidate("overlap", "d1", datetime(2024, 3, 4, 9, 20), datetime(2024, 3, 4, 9, 40)),
        _candidate("too-soon", "d1", datetime(2024, 3, 4, 9, 35), datetime(2024, 3, 4, 10)),
        _candidate("reachable", "d1", datetime(2024, 3, 4, 9, 40), datetime(2024, 3, 4, 10)),
    ]

    result = allocate_trips(spark.createDataFrame(rows), _travel(spark))

    assert [row.trip_key for row in result.orderBy("trip_sequence").collect()] == ["t1", "reachable"]
    assert result.orderBy("trip_sequence").collect()[1].deadhead_minutes == pytest.approx(10.0)


@pytest.mark.parametrize("limit", ["trips", "minutes"])
def test_일일_운행수와_근무시간_상한을_넘지_않는다(spark, limit):
    first = _candidate("t1", "d1", datetime(2024, 3, 4, 9), datetime(2024, 3, 4, 9, 20))
    second = _candidate("t2", "d1", datetime(2024, 3, 4, 9, 30), datetime(2024, 3, 4, 10), pu=2)
    if limit == "trips":
        first["target_daily_trips"] = second["target_daily_trips"] = 1
    else:
        first["target_work_minutes"] = second["target_work_minutes"] = 50

    result = allocate_trips(spark.createDataFrame([first, second]), _travel(spark))

    assert result.count() == 1


def test_한_운행은_점수가_높은_기사_한_명에게만_배정된다(spark):
    pickup, dropoff = datetime(2024, 3, 4, 9), datetime(2024, 3, 4, 9, 20)
    rows = [
        _candidate("t1", "d1", pickup, dropoff, score=0.8),
        _candidate("t1", "d2", pickup, dropoff, score=0.9),
    ]

    result = allocate_trips(spark.createDataFrame(rows), _travel(spark))

    assert [(row.trip_key, row.driver_id) for row in result.collect()] == [("t1", "d2")]


def test_점수가_같으면_tie_break가_작은_기사를_선택한다(spark):
    pickup, dropoff = datetime(2024, 3, 4, 9), datetime(2024, 3, 4, 9, 20)
    rows = [
        _candidate("t1", "d1", pickup, dropoff, tie="z"),
        _candidate("t1", "d2", pickup, dropoff, tie="a"),
    ]

    assert allocate_trips(spark.createDataFrame(rows), _travel(spark)).first().driver_id == "d2"


def test_날짜별_상태는_분리되고_입력_순서와_무관하다(spark):
    rows = [
        _candidate("day1", "d1", datetime(2024, 3, 4, 23), datetime(2024, 3, 4, 23, 50)),
        _candidate("day2", "d1", datetime(2024, 3, 5, 0), datetime(2024, 3, 5, 0, 20)),
    ]
    first = allocate_trips(spark.createDataFrame(rows), _travel(spark)).orderBy("trip_key").collect()
    second = allocate_trips(spark.createDataFrame(list(reversed(rows))), _travel(spark)).orderBy("trip_key").collect()

    assert first == second
    assert [row.trip_sequence for row in first] == [1, 1]


def test_이동시간이_없는_서로_다른_구역은_연결하지_않는다(spark):
    rows = [
        _candidate("t1", "d1", datetime(2024, 3, 4, 9), datetime(2024, 3, 4, 9, 20), do=9),
        _candidate("t2", "d1", datetime(2024, 3, 4, 10), datetime(2024, 3, 4, 10, 20), pu=8),
    ]

    assert allocate_trips(spark.createDataFrame(rows), _travel(spark)).count() == 1


@pytest.mark.parametrize("violation", ["duplicate_candidate", "negative_travel", "bad_time"])
def test_입력_계약_위반은_ValueError다(spark, violation):
    row = _candidate("t1", "d1", datetime(2024, 3, 4, 9), datetime(2024, 3, 4, 9, 20))
    candidates = spark.createDataFrame([row, row] if violation == "duplicate_candidate" else [row])
    travel = _travel(spark, ((2, 1, -1.0),) if violation == "negative_travel" else ((2, 1, 10.0),))
    if violation == "bad_time":
        candidates = candidates.withColumnRenamed("pickup_datetime", "_pickup").withColumnRenamed("dropoff_datetime", "pickup_datetime").withColumnRenamed("_pickup", "dropoff_datetime")

    with pytest.raises(ValueError):
        allocate_trips(candidates, travel)


def test_빈_후보는_고정_스키마의_빈_결과다(spark):
    schema = "trip_key string, driver_id string, taxi_id string, pickup_datetime timestamp, dropoff_datetime timestamp, PULocationID int, DOLocationID int, preference_score double, tie_break string, target_daily_trips int, target_work_minutes int, max_deadhead_minutes int"

    result = allocate_trips(spark.createDataFrame([], schema), _travel(spark))

    assert result.count() == 0
    assert result.schema == ASSIGNMENT_SCHEMA


# --- 재계산 방지 (#360) ----------------------------------------------------
#
# 캐시가 없으면 action 마다 원본부터 다시 계산합니다. 기사 배정은 검증에서
# 여러 번 읽히는 구조라, 캐시가 빠지면 실패하지 않고 **몇 배 느려지기만** 합니다.
# 시간은 테스트로 못 잡으니 캐시 여부를 직접 확인합니다.


def test_이동시간을_캐시해_원본을_반복_스캔하지_않는다(spark):
    travel_times = _travel(spark)
    assert not travel_times.is_cached  # 전제 확인

    allocate_trips(spark.createDataFrame([_candidate("t1", "d1",
        datetime(2024, 3, 4, 9), datetime(2024, 3, 4, 9, 30))]), travel_times)

    # 검증 2회 + collect 1회 = 3회 읽습니다. 캐시가 없으면 매번 Parquet 스캔입니다.
    assert travel_times.is_cached


def test_배정에_필요한_컬럼만_파이썬으로_넘긴다(spark):
    """쓰지 않는 컬럼을 실어 보내면 실패하지 않고 **더 큰 데이터에서만** 터집니다."""
    from sub.spark.jobs.driver_assignment.allocator import CANDIDATE_COLUMNS, allocation_input

    row = _candidate("t1", "d1", datetime(2024, 3, 4, 9), datetime(2024, 3, 4, 9, 30))
    # 실제 후보에 딸려오는 무거운 컬럼들 — 배정 로직은 쓰지 않습니다.
    row.update({"time_block_weights": [0.1] * 8, "pickup_zone": "x" * 64})

    columns = set(allocation_input(spark.createDataFrame([row])).columns)

    assert columns == CANDIDATE_COLUMNS | {"_service_date", "_bucket"}
