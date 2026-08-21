"""결정적 시공간 운행 배정 시나리오. 이슈 #293.

1. 운행 겹침·공차 도착 불가 후보 제외
2. 일일 운행 수·첫 승차~마지막 하차 근무시간 상한 적용
3. 운행별 단일 기사와 점수·tie-break 우선순위 보장
4. 날짜별 상태 분리와 입력 순서 무관 결정성
5. 이동시간 결측·입력 계약 위반·빈 후보 처리
6. 제약별 탈락 건수 계측 (#644)
"""

from datetime import datetime

import pytest

from shared.spark.common.session import get_or_create_spark_session
from sub.spark.jobs.driver_assignment.allocator import (
    ASSIGNMENT_SCHEMA,
    C3_DRIVE_MINUTES,
    C3_WORK_MINUTES,
    C4_NO_ROUTE,
    C4_OVERLAP,
    C4_TOO_FAR,
    C4_TOO_LATE,
    C5_VEHICLE_CONFLICT,
    allocate_trips,
)


@pytest.fixture(scope="module")
def spark():
    session = get_or_create_spark_session("test_trip_allocator")
    yield session
    session.stop()


def _candidate(key, driver, pickup, dropoff, pu=1, do=2, score=0.8, tie="a", drive_minutes=480, minutes=480):
    return {
        "trip_key": key, "driver_id": driver, "taxi_id": f"taxi-{driver}",
        "pickup_datetime": pickup, "dropoff_datetime": dropoff,
        "PULocationID": pu, "DOLocationID": do,
        "preference_score": score, "tie_break": tie,
        "target_drive_minutes": drive_minutes, "target_work_minutes": minutes,
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

    result, rejected = allocate_trips(spark.createDataFrame(rows), _travel(spark))

    assert [row.trip_key for row in result.orderBy("trip_sequence").collect()] == ["t1", "reachable"]
    assert result.orderBy("trip_sequence").collect()[1].deadhead_minutes == pytest.approx(10.0)
    # overlap·too-soon 둘 다 "도착 전에 이미 출발" 사유로 떨어집니다.
    assert rejected[C4_OVERLAP] == 2
    assert rejected[C4_TOO_LATE] == 2


@pytest.mark.parametrize("limit", ["drive_minutes", "work_minutes"])
def test_운행분_예산과_근무시간_상한을_넘지_않는다(spark, limit):
    """상한은 트립 수(`target_daily_trips`)가 아니라 누적 운행분(`target_drive_minutes`)입니다.

    첫 트립 20분 + 둘째 30분 = 50분. 예산을 25분으로 두면 둘째에서 끊깁니다.
    """
    first = _candidate("t1", "d1", datetime(2024, 3, 4, 9), datetime(2024, 3, 4, 9, 20))
    second = _candidate("t2", "d1", datetime(2024, 3, 4, 9, 30), datetime(2024, 3, 4, 10), pu=2)
    if limit == "drive_minutes":
        first["target_drive_minutes"] = second["target_drive_minutes"] = 25
    else:
        first["target_work_minutes"] = second["target_work_minutes"] = 50

    result, rejected = allocate_trips(spark.createDataFrame([first, second]), _travel(spark))

    assert result.count() == 1
    assert rejected[C3_DRIVE_MINUTES if limit == "drive_minutes" else C3_WORK_MINUTES] == 1


def test_한_운행은_점수가_높은_기사_한_명에게만_배정된다(spark):
    pickup, dropoff = datetime(2024, 3, 4, 9), datetime(2024, 3, 4, 9, 20)
    rows = [
        _candidate("t1", "d1", pickup, dropoff, score=0.8),
        _candidate("t1", "d2", pickup, dropoff, score=0.9),
    ]

    result, _ = allocate_trips(spark.createDataFrame(rows), _travel(spark))

    assert [(row.trip_key, row.driver_id) for row in result.collect()] == [("t1", "d2")]


def test_점수가_같으면_tie_break가_작은_기사를_선택한다(spark):
    pickup, dropoff = datetime(2024, 3, 4, 9), datetime(2024, 3, 4, 9, 20)
    rows = [
        _candidate("t1", "d1", pickup, dropoff, tie="z"),
        _candidate("t1", "d2", pickup, dropoff, tie="a"),
    ]

    result, _ = allocate_trips(spark.createDataFrame(rows), _travel(spark))
    assert result.first().driver_id == "d2"


def test_날짜별_상태는_분리되고_입력_순서와_무관하다(spark):
    rows = [
        _candidate("day1", "d1", datetime(2024, 3, 4, 23), datetime(2024, 3, 4, 23, 50)),
        _candidate("day2", "d1", datetime(2024, 3, 5, 0), datetime(2024, 3, 5, 0, 20)),
    ]
    first, _ = allocate_trips(spark.createDataFrame(rows), _travel(spark))
    second, _ = allocate_trips(spark.createDataFrame(list(reversed(rows))), _travel(spark))
    first = first.orderBy("trip_key").collect()
    second = second.orderBy("trip_key").collect()

    assert first == second
    assert [row.trip_sequence for row in first] == [1, 1]


def test_이동시간이_없는_서로_다른_구역은_연결하지_않는다(spark):
    rows = [
        _candidate("t1", "d1", datetime(2024, 3, 4, 9), datetime(2024, 3, 4, 9, 20), do=9),
        _candidate("t2", "d1", datetime(2024, 3, 4, 10), datetime(2024, 3, 4, 10, 20), pu=8),
    ]

    result, rejected = allocate_trips(spark.createDataFrame(rows), _travel(spark))
    assert result.count() == 1
    assert rejected[C4_OVERLAP] == 1
    assert rejected[C4_NO_ROUTE] == 1


def test_공차가_기사_한도를_넘으면_c4b로_떨어진다(spark):
    # t1 하차 구역(2) -> t2 승차 구역(1, 기본값) 이동시간은 10분(_travel 기본).
    # 한도를 5분으로 낮춰 그 10분을 넘게 만듭니다.
    rows = [
        _candidate("t1", "d1", datetime(2024, 3, 4, 9), datetime(2024, 3, 4, 9, 20)),
        _candidate("t2", "d1", datetime(2024, 3, 4, 10), datetime(2024, 3, 4, 10, 20)),
    ]
    rows[1]["max_deadhead_minutes"] = 5

    result, rejected = allocate_trips(spark.createDataFrame(rows), _travel(spark))
    assert result.count() == 1
    assert rejected[C4_OVERLAP] == 1
    assert rejected[C4_TOO_FAR] == 1


def test_같은_차량이_겹치면_c5로_떨어진다(spark):
    """기사:차량이 1:1 이라 시간 겹침이 없으면 원래 안 생기는 경우지만, 계측
    자체는 taxi_id 가 겹치는 두 후보를 직접 만들어 검증합니다."""
    pickup, dropoff = datetime(2024, 3, 4, 9), datetime(2024, 3, 4, 9, 20)
    same_taxi = _candidate("t1", "d1", pickup, dropoff)
    same_taxi["taxi_id"] = "shared-taxi"
    other = _candidate("t2", "d2", pickup, dropoff)
    other["taxi_id"] = "shared-taxi"

    result, rejected = allocate_trips(spark.createDataFrame([same_taxi, other]), _travel(spark))
    assert result.count() == 1
    assert rejected[C5_VEHICLE_CONFLICT] == 1


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
    schema = "trip_key string, driver_id string, taxi_id string, pickup_datetime timestamp, dropoff_datetime timestamp, PULocationID int, DOLocationID int, preference_score double, tie_break string, target_drive_minutes int, target_work_minutes int, max_deadhead_minutes int"

    result, rejected = allocate_trips(spark.createDataFrame([], schema), _travel(spark))

    assert result.count() == 0
    assert result.schema == ASSIGNMENT_SCHEMA
    assert all(count == 0 for count in rejected.values())


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
