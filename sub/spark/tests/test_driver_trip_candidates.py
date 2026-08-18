"""기사-운행 후보 생성 시나리오. 이슈 #292.

1. 계약 기간·활동 요일·활동 시간대 밖의 후보 제외
2. Comfort/Extra Comfort 운행은 자격 차량만 허용
3. 시간·거리·공항·맨해튼 선호가 점수에 반영
4. 같은 seed와 입력은 동일 후보·tie-break 생성
5. 운행별 후보 수는 버킷 인원수 이하로 제한
6. null·중복 ID와 잘못된 시간 가중치 계약은 명시적 실패
7. 슬롯이 겹치는 크기에서도 한 운행에 같은 기사가 두 번 후보로 들어가지 않음 (이슈 #362)
"""

from datetime import date, datetime

import pytest
from pyspark.sql.functions import array, concat, lit, slice

from shared.spark.common.session import get_or_create_spark_session
from sub.spark.jobs.driver_assignment.candidates import SCORE_WEIGHTS, build_trip_candidates
@pytest.fixture(scope="module")
def spark():
    session = get_or_create_spark_session("test_driver_trip_candidates")
    yield session
    session.stop()
def _frames(spark, *, tier="Standard", pickup_zone="Queens", bucket_size=1):
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
        "manhattan_preference": 0.8, "tier_preference": 0.7,
        "target_daily_trips": 10,
        "target_work_minutes": 480, "max_deadhead_minutes": 10,
    }])
    # 실제 회사 스냅샷은 세 테이블이 모두 `snapshot_date` 를 갖습니다. 빼놓으면
    # 조인 후 컬럼이 3개로 겹치는 상황을 재현하지 못합니다.
    customers = spark.createDataFrame([{
        "customer_id": "customer-1", "synthetic_driver_id": "driver-1",
        "snapshot_date": date(2026, 8, 12),
    }])
    leases = spark.createDataFrame([{
        "lease_id": "lease-1", "customer_id": "customer-1", "taxi_id": "taxi-1",
        "lease_started_on": date(2024, 1, 1), "lease_ended_on": date(2099, 1, 1),
        "snapshot_date": date(2026, 8, 12),
    }])
    taxis = spark.createDataFrame([{
        "taxi_id": "taxi-1", "uber_comfort_eligible": True,
        "lyft_extra_comfort_eligible": True,
        "snapshot_date": date(2026, 8, 12),
    }])
    return trips, preferences, customers, leases, taxis, bucket_size


def _with_drivers(frames, count):
    for index in range(2, count + 1):
        suffix = lit(str(index))
        frames[1] = frames[1].unionByName(
            frames[1].limit(1).withColumn("driver_id", concat(lit("driver-"), suffix))
        )
        frames[2] = frames[2].unionByName(frames[2].limit(1).select(
            concat(lit("customer-"), suffix).alias("customer_id"),
            concat(lit("driver-"), suffix).alias("synthetic_driver_id"),
            "snapshot_date",
        ))
        frames[3] = frames[3].unionByName(frames[3].limit(1).select(
            concat(lit("lease-"), suffix).alias("lease_id"),
            concat(lit("customer-"), suffix).alias("customer_id"),
            concat(lit("taxi-"), suffix).alias("taxi_id"),
            "lease_started_on", "lease_ended_on", "snapshot_date",
        ))
        frames[4] = frames[4].unionByName(frames[4].limit(1).select(
            concat(lit("taxi-"), suffix).alias("taxi_id"),
            "uber_comfort_eligible", "lyft_extra_comfort_eligible", "snapshot_date",
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

    assert build_trip_candidates(*frames[:-1], bucket_size=frames[-1]).count() == 0


@pytest.mark.parametrize(
    ("tier", "eligibility"),
    [("Comfort", "uber_comfort_eligible"), ("Extra Comfort", "lyft_extra_comfort_eligible")],
)
def test_프리미엄_운행은_해당_자격_차량만_후보다(spark, tier, eligibility):
    frames = list(_frames(spark, tier=tier))
    frames[4] = frames[4].withColumn(eligibility, ~frames[4][eligibility])

    assert build_trip_candidates(*frames[:-1], bucket_size=frames[-1]).count() == 0


def test_시간_거리_지역_등급_선호가_점수에_반영된다(spark):
    result = build_trip_candidates(*_frames(spark)[:-1], bucket_size=1).first()

    assert result.time_score == pytest.approx(1.0)
    assert result.distance_score == pytest.approx(1.0)
    assert result.airport_score == pytest.approx(0.1)
    assert result.manhattan_score == pytest.approx(0.2)
    # 픽스처 운행이 Standard 라 등급 점수는 1 - tier_preference(0.7) 입니다.
    assert result.tier_score == pytest.approx(0.3)
    assert result.preference_score == pytest.approx(
        1.0 * SCORE_WEIGHTS["time"] + 1.0 * SCORE_WEIGHTS["distance"]
        + 0.1 * SCORE_WEIGHTS["airport"] + 0.2 * SCORE_WEIGHTS["manhattan"]
        + 0.3 * SCORE_WEIGHTS["tier"]
    )


@pytest.mark.parametrize(
    ("tier", "eligibility"),
    [("Comfort", "uber_comfort_eligible"), ("Extra Comfort", "lyft_extra_comfort_eligible")],
)
def test_자격되는_프리미엄_운행은_Standard_보다_등급점수가_높다(spark, tier, eligibility):
    """같은 기사·같은 조건이면 프리미엄 쪽 점수가 더 높아야 배정에서 우선됩니다."""
    standard = build_trip_candidates(*_frames(spark)[:-1], bucket_size=1).first()
    premium = build_trip_candidates(*_frames(spark, tier=tier)[:-1], bucket_size=1).first()

    assert standard.tier_score == pytest.approx(0.3)   # 1 - 0.7
    assert premium.tier_score == pytest.approx(0.7)    # tier_preference 그대로
    assert premium.preference_score > standard.preference_score


def test_같은_seed는_입력_순서와_무관하게_같은_후보를_만든다(spark):
    frames = list(_frames(spark))
    frames = _with_drivers(frames, 2)
    first = build_trip_candidates(*frames[:-1], seed=7, bucket_size=2)
    second = build_trip_candidates(frames[0], frames[1].orderBy("driver_id", ascending=False), *frames[2:-1], seed=7, bucket_size=2)

    assert sorted(first.select("driver_id", "tie_break").collect()) == sorted(
        second.select("driver_id", "tie_break").collect()
    )


def test_운행별_후보수는_버킷_인원수를_넘지_않는다(spark):
    frames = list(_frames(spark))
    frames = _with_drivers(frames, 7)

    assert build_trip_candidates(*frames[:-1], bucket_size=3).count() <= 3


def test_한_운행에_같은_기사가_두_번_후보로_들어가지_않는다(spark):
    """버킷 분할에서는 중복이 **구조적으로** 불가능합니다.

    운행 하나가 버킷 하나에만 들어가고 그 버킷의 기사와 한 번씩 짝지어지므로,
    예전 해시 슬롯 방식과 달리 중복 제거 단계가 아예 필요 없습니다.
    """
    # 기사 7명, bucket_size 64 -> 버킷 수 max(1, 7//64) = 1. 전원이 한 버킷입니다.
    frames = _with_drivers(list(_frames(spark, bucket_size=64)), 7)

    pairs = [
        (row.trip_key, row.driver_id)
        for row in build_trip_candidates(*frames[:-1], bucket_size=frames[-1])
        .select("trip_key", "driver_id")
        .collect()
    ]

    assert len(pairs) == len(set(pairs))
    # 같은 버킷의 기사 전원과 정확히 한 번씩 — 빠지지도 겹치지도 않아야 합니다.
    assert len(pairs) == 7


def test_운행은_버킷_하나에만_들어간다(spark):
    """두 버킷이 같은 운행을 다투면 중복 배정이 생깁니다."""
    frames = _with_drivers(list(_frames(spark, bucket_size=2)), 6)

    rows = (
        build_trip_candidates(*frames[:-1], bucket_size=frames[-1])
        .select("trip_key", "_bucket")
        .distinct()
        .collect()
    )

    buckets_per_trip = {}
    for row in rows:
        buckets_per_trip.setdefault(row.trip_key, set()).add(row._bucket)
    assert all(len(b) == 1 for b in buckets_per_trip.values()), buckets_per_trip


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
        build_trip_candidates(*frames[:-1], bucket_size=frames[-1])


# --- 컬럼 이름 중복 (#365) -------------------------------------------------
#
# 고객·계약·택시가 각자 `snapshot_date` 를 들고 있어 조인 3번이면 같은 이름의 컬럼이
# 3개가 됩니다. 그 상태로 배정의 `applyInPandas` 에 넘기면 `df["snapshot_date"]` 가
# AMBIGUOUS_REFERENCE 로 죽습니다 — 후보 생성 단계에서는 아무 증상이 없어서,
# 배정까지 가봐야 드러납니다.


def test_후보에_같은_이름의_컬럼이_두_번_들어가지_않는다(spark):
    """중복 컬럼은 실패하지 않고 다음 단계로 흘러가 거기서 터집니다."""
    frames = _frames(spark)

    columns = build_trip_candidates(*frames[:5], seed=42, bucket_size=frames[5]).columns

    duplicated = sorted({name for name in columns if columns.count(name) > 1})
    assert not duplicated, f"중복 컬럼: {duplicated}"
