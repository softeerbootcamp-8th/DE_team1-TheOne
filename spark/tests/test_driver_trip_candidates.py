"""기사-운행 후보 생성 시나리오. 이슈 #292.

1. 계약 기간·활동 요일·활동 시간대 밖의 후보 제외
2. Comfort/Extra Comfort 운행은 자격 차량만 허용
3. 시간·거리·공항·맨해튼 선호가 점수에 반영
4. 같은 seed와 입력은 동일 후보·tie-break 생성
5. 운행별 후보 수는 pool_size 이하로 제한
6. null·중복 ID와 잘못된 시간 가중치 계약은 명시적 실패
7. 슬롯이 겹치는 크기에서도 한 운행에 같은 기사가 두 번 후보로 들어가지 않음 (이슈 #362)
"""

from datetime import date, datetime

import pytest
from pyspark.sql.functions import array, concat, lit, slice

from common.session import get_or_create_spark_session
from jobs.driver_assignment.candidates import build_trip_candidates
@pytest.fixture(scope="module")
def spark():
    session = get_or_create_spark_session("test_driver_trip_candidates")
    yield session
    session.stop()
def _frames(spark, *, tier="Standard", pickup_zone="Queens", pool_size=1):
    trips = spark.createDataFrame([{
        "trip_key": "trip-1", "pickup_datetime": datetime(2024, 3, 4, 9),
        "dropoff_datetime": datetime(2024, 3, 4, 9, 20), "trip_miles": 5.0,
        "platform_name": "Lyft" if tier == "Extra Comfort" else "Uber",
        "estimated_service_tier": tier,
        "pickup_borough": pickup_zone, "dropoff_borough": pickup_zone,
        "pickup_service_zone": "Airports" if pickup_zone == "Airport" else "Boro Zone",
        "dropoff_service_zone": "Boro Zone",
    }])
    preferences = spark.createDataFrame([{
        "driver_id": "driver-1", "active_weekdays": ["MON"],
        "preferred_time_blocks": ["09-12"], "time_block_weights": [0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0],
        "preferred_distance_miles": 5.0, "airport_preference": 0.9,
        "manhattan_preference": 0.8, "target_daily_trips": 10,
        "target_work_minutes": 480, "max_deadhead_minutes": 10,
    }])
    customers = spark.createDataFrame([{
        "customer_id": "customer-1", "synthetic_driver_id": "driver-1",
    }])
    leases = spark.createDataFrame([{
        "lease_id": "lease-1", "customer_id": "customer-1", "taxi_id": "taxi-1",
        "lease_started_on": date(2024, 1, 1), "lease_ended_on": date(2099, 1, 1),
    }])
    taxis = spark.createDataFrame([{
        "taxi_id": "taxi-1", "uber_comfort_eligible": True,
        "lyft_extra_comfort_eligible": True,
    }])
    return trips, preferences, customers, leases, taxis, pool_size


def _with_drivers(frames, count):
    for index in range(2, count + 1):
        suffix = lit(str(index))
        frames[1] = frames[1].unionByName(
            frames[1].limit(1).withColumn("driver_id", concat(lit("driver-"), suffix))
        )
        frames[2] = frames[2].unionByName(frames[2].limit(1).select(
            concat(lit("customer-"), suffix).alias("customer_id"),
            concat(lit("driver-"), suffix).alias("synthetic_driver_id"),
        ))
        frames[3] = frames[3].unionByName(frames[3].limit(1).select(
            concat(lit("lease-"), suffix).alias("lease_id"),
            concat(lit("customer-"), suffix).alias("customer_id"),
            concat(lit("taxi-"), suffix).alias("taxi_id"),
            "lease_started_on", "lease_ended_on",
        ))
        frames[4] = frames[4].unionByName(frames[4].limit(1).select(
            concat(lit("taxi-"), suffix).alias("taxi_id"),
            "uber_comfort_eligible", "lyft_extra_comfort_eligible",
        ))
    return frames


@pytest.mark.parametrize("change", ["contract", "weekday", "time_block"])
def test_계약_요일_시간대가_맞지_않으면_후보에서_제외한다(spark, change):
    frames = list(_frames(spark))
    if change == "contract":
        frames[3] = frames[3].withColumn("lease_ended_on", frames[3].lease_started_on)
    elif change == "weekday":
        frames[1] = frames[1].withColumn("active_weekdays", array(lit("TUE")))
    else:
        frames[1] = frames[1].withColumn("preferred_time_blocks", array(lit("12-15")))

    assert build_trip_candidates(*frames[:-1], pool_size=frames[-1]).count() == 0


@pytest.mark.parametrize(
    ("tier", "eligibility"),
    [("Comfort", "uber_comfort_eligible"), ("Extra Comfort", "lyft_extra_comfort_eligible")],
)
def test_프리미엄_운행은_해당_자격_차량만_후보다(spark, tier, eligibility):
    frames = list(_frames(spark, tier=tier))
    frames[4] = frames[4].withColumn(eligibility, ~frames[4][eligibility])

    assert build_trip_candidates(*frames[:-1], pool_size=frames[-1]).count() == 0


def test_시간_거리_지역_선호가_점수에_반영된다(spark):
    result = build_trip_candidates(*_frames(spark)[:-1], pool_size=1).first()

    assert result.time_score == pytest.approx(1.0)
    assert result.distance_score == pytest.approx(1.0)
    assert result.airport_score == pytest.approx(0.1)
    assert result.manhattan_score == pytest.approx(0.2)
    assert result.preference_score == pytest.approx(0.7)


def test_같은_seed는_입력_순서와_무관하게_같은_후보를_만든다(spark):
    frames = list(_frames(spark))
    frames = _with_drivers(frames, 2)
    first = build_trip_candidates(*frames[:-1], seed=7, pool_size=2)
    second = build_trip_candidates(frames[0], frames[1].orderBy("driver_id", ascending=False), *frames[2:-1], seed=7, pool_size=2)

    assert sorted(first.select("driver_id", "tie_break").collect()) == sorted(
        second.select("driver_id", "tie_break").collect()
    )


def test_운행별_후보수는_pool_size를_넘지_않는다(spark):
    frames = list(_frames(spark))
    frames = _with_drivers(frames, 7)

    assert build_trip_candidates(*frames[:-1], pool_size=3).count() <= 3


def test_한_운행에_같은_기사가_두_번_후보로_들어가지_않는다(spark):
    # 슬롯 수는 min(pool_size, 기사 수) 라서 기사가 적으면 슬롯도 같이 잘리고, 그러면
    # 해시가 겹칠 일이 없어 중복 제거가 아예 실행되지 않습니다. pool_size 를 키우고
    # 기사를 7명 두면 슬롯 7개가 7명에 뿌려져 겹칩니다 (전부 다를 확률 7!/7^7 = 0.6%,
    # seed 고정이라 실제로는 결정적 — 이 픽스처에서 7개 슬롯이 4명으로 줄어듭니다).
    frames = _with_drivers(list(_frames(spark, pool_size=64)), 7)

    pairs = [
        (row.trip_key, row.driver_id)
        for row in build_trip_candidates(*frames[:-1], pool_size=frames[-1])
        .select("trip_key", "driver_id")
        .collect()
    ]

    assert len(pairs) == len(set(pairs))
    # 겹침이 실제로 일어났는지 확인합니다. 이 assert 가 없으면 픽스처가 바뀌어 충돌이
    # 사라져도 위 assert 가 조용히 통과해 테스트가 아무것도 막지 못합니다.
    assert len(pairs) < 7


@pytest.mark.parametrize("violation", ["null_trip", "duplicate_driver", "weights"])
def test_입력_계약_위반은_ValueError다(spark, violation):
    frames = list(_frames(spark))
    if violation == "null_trip":
        frames[0] = frames[0].withColumn("trip_key", lit(None).cast("string"))
    elif violation == "duplicate_driver":
        frames[1] = frames[1].unionByName(frames[1])
    else:
        frames[1] = frames[1].withColumn("time_block_weights", slice("time_block_weights", 1, 7))

    with pytest.raises(ValueError):
        build_trip_candidates(*frames[:-1], pool_size=frames[-1])
