"""구역쌍 이동시간 Silver 시나리오. 이슈 #348.

1. 구역쌍별 중앙값을 분 단위로 계산
2. 대표값이 평균이 아니라 중앙값 — 이상치가 이동시간을 부풀리면 배정이 조용히 줄어듦
3. 관측이 적은 구역쌍은 버림
4. 비정상 트립(결측·0 이하·과도하게 긴 것)은 표본에서 제외
5. 출력이 allocator 의 입력 계약(컬럼·유일성·0 이상)을 만족
6. 필수 컬럼이 없으면 즉시 실패
"""

import pytest

from shared.spark.common.session import get_or_create_spark_session
from sub.spark.jobs.driver_assignment.allocator import TRAVEL_COLUMNS
from sub.spark.jobs.travel_times.transformer import MAX_TRIP_MINUTES, build_travel_times


@pytest.fixture(scope="module")
def spark():
    session = get_or_create_spark_session("test_travel_times")
    yield session
    session.stop()


def trips(spark, rows):
    """(PULocationID, DOLocationID, trip_time 초) 목록을 HVFHV Silver 모양으로."""
    return spark.createDataFrame(
        [
            {"PULocationID": pu, "DOLocationID": do, "trip_time": trip_time}
            for pu, do, trip_time in rows
        ]
    )


def as_dict(frame):
    return {
        (row.from_location_id, row.to_location_id): (row.travel_minutes, row.trip_count)
        for row in frame.collect()
    }


def test_구역쌍별_이동시간_중앙값을_분_단위로_낸다(spark):
    rows = [(1, 2, 600)] * 3 + [(1, 3, 1200)] * 3  # 10분 / 20분

    result = as_dict(build_travel_times(trips(spark, rows), min_trips=3))

    assert result[(1, 2)] == (10.0, 3)
    assert result[(1, 3)] == (20.0, 3)


def test_이상치가_있어도_중앙값이라_끌려가지_않는다(spark):
    """평균이면 24분이 됩니다. 이동시간이 길게 잡히면 배정 후보가 과하게 걸러집니다."""
    rows = [(1, 2, 600), (1, 2, 600), (1, 2, 600), (1, 2, 600), (1, 2, 6000)]

    result = as_dict(build_travel_times(trips(spark, rows), min_trips=3))

    assert result[(1, 2)][0] == 10.0  # 평균 24.0 이 아님
    assert result[(1, 2)][1] == 5


def test_관측이_적은_구역쌍은_버린다(spark):
    """1건짜리 이동시간은 그 트립의 사정일 뿐인데, 크면 그 구역쌍 배정을 전부 막습니다."""
    rows = [(1, 2, 600)] * 5 + [(9, 9, 600)]

    result = as_dict(build_travel_times(trips(spark, rows), min_trips=5))

    assert (1, 2) in result
    assert (9, 9) not in result


@pytest.mark.parametrize(
    ("bad_trip_time", "reason"),
    [
        (None, "결측"),
        (0, "0 이하"),
        (int(MAX_TRIP_MINUTES * 60) + 60, "과도하게 긴 트립"),
    ],
)
def test_비정상_트립은_표본에서_뺀다(spark, bad_trip_time, reason):
    rows = [(1, 2, 600)] * 3 + [(1, 2, bad_trip_time)]

    result = as_dict(build_travel_times(trips(spark, rows), min_trips=3))

    assert result[(1, 2)] == (10.0, 3), reason


def test_결과가_allocator_입력_계약을_만족한다(spark):
    rows = [(1, 2, 600)] * 5 + [(2, 1, 900)] * 5

    result = build_travel_times(trips(spark, rows), min_trips=5)

    assert TRAVEL_COLUMNS <= set(result.columns)
    collected = result.collect()
    keys = [(row.from_location_id, row.to_location_id) for row in collected]
    assert len(keys) == len(set(keys))  # allocator 가 유일성을 요구
    assert all(row.travel_minutes >= 0 for row in collected)
    assert not any(row.travel_minutes is None for row in collected)


def test_필수_컬럼이_없으면_즉시_실패한다(spark):
    frame = spark.createDataFrame([{"PULocationID": 1, "DOLocationID": 2}])

    with pytest.raises(ValueError, match="trip_time"):
        build_travel_times(frame)
