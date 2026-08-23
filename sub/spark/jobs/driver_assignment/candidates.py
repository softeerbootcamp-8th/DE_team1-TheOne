"""운행별 소규모 기사 후보군에 hard constraint와 선호 점수를 적용합니다."""

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    abs as spark_abs,
    col,
    concat_ws,
    count,
    dayofweek,
    element_at,
    floor,
    greatest,
    hour,
    lit,
    pmod,
    pow as spark_pow,
    row_number,
    sha2,
    size,
    to_date,
    when,
    xxhash64,
)
TIME_BLOCKS = ["00-03", "03-06", "06-09", "09-12", "12-15", "15-18", "18-21", "21-24"]

# 제약 이름. `sub/prototype/attribution.py`와 같은 이름을 씁니다 — 보고서와 코드가
# 같은 이름을 써야 대조가 됩니다(#644).
C1_ROSTER = "c1_roster_period"        # 트립 시각에 기사가 명부(활성 계약)에 존재
C2_TIER = "c2_vehicle_tier"           # 배정 가능 차량 등급 = 트립 서비스 등급
C6_PROFILE = "c6_profile_fit"         # 근무 요일·시간대 적합
C_NO_CANDIDATE = "no_bucket_candidate"  # 버킷에 후보 기사가 아예 없음
# 선호 점수 가중치는 `config/generation.json` 의 `allocation.score_weights` 가
# 소유하고 `build_trip_candidates` 인자로 들어옵니다. 합이 1.0 이어야
# preference_score 가 0~1 범위를 유지하며, 그 검증은 설정 로더가 합니다
# (`sub/config.py`). tier 0.15 는 실측이 아니라 가정입니다 — 근거는 preference.py 의
# tier_preference 주석.

REQUIRED = {
    "trips": {
        "trip_key", "pickup_datetime", "dropoff_datetime", "trip_miles",
        "platform_name", "estimated_service_tier", "pickup_borough", "dropoff_borough",
        "pickup_service_zone", "dropoff_service_zone",
    },
    "preferences": {
        "driver_id", "weekday_mask", "time_block_mask", "time_block_weights",
        "preferred_distance_miles", "airport_preference", "manhattan_preference",
        "tier_preference",
        "target_work_minutes", "target_drive_minutes", "max_deadhead_minutes",
    },
    "current_driver_vehicle": {
        "driver_id", "taxi_id", "lease_started_on", "lease_ended_on",
        "uber_comfort_eligible", "lyft_extra_comfort_eligible",
    },
}
def _validate(frame: DataFrame, name: str, key: str) -> None:
    missing = REQUIRED[name] - set(frame.columns)
    if missing:
        raise ValueError(f"{name} 필수 컬럼 누락: {sorted(missing)}")
    if frame.filter(col(key).isNull()).limit(1).count():
        raise ValueError(f"{name}.{key}는 null일 수 없습니다")
    if frame.groupBy(key).count().filter(col("count") > 1).limit(1).count():
        raise ValueError(f"{name}.{key}는 중복될 수 없습니다")
def build_trip_candidates(
    trips: DataFrame,
    preferences: DataFrame,
    current_driver_vehicle: DataFrame,
    *,
    seed: int,
    bucket_size: int,
    score_weights: dict[str, float],
) -> tuple[DataFrame, dict[str, int]]:
    """기사를 버킷으로 묶고, 각 운행을 한 버킷에만 배정해 후보를 만듭니다.

    두 번째 반환값은 제약별 탈락 건수입니다(#644) — 매칭률이 낮을 때 "기사가
    부족한가, 프로필이 안 맞는가, 등급이 안 맞는가"를 구분하기 위한 진단용이고
    릴리스 계보에는 싣지 않습니다. 후보 행에는 boolean/문자열 사유 컬럼을 남기지
    않고 한 번의 집계로만 셉니다 — 후보가 수백만 행이라 사유 컬럼을 실어 보내면
    그 자체가 메모리 비용입니다.

    기사가 병목입니다 — 기사 2,000명이 한 달에 소화할 수 있는 운행은 약 117만 건으로
    전체 트립의 5.7% 뿐입니다. 그래서 "운행마다 기사를 뽑는" 방향으로 후보를 만들면
    어차피 버려질 94.3% 에 대해서도 후보를 만들게 됩니다.

    대신 **운행을 기사 버킷에 미리 나눠 줍니다.** 버킷 하나가 자기 몫의 운행만 보므로

    - 같은 운행을 두 버킷이 다툴 일이 없습니다 (중복 배정 원천 차단)
    - 배정 단위가 (버킷 × 날짜) 로 잘게 쪼개져 그룹 하나가 3만 행대가 됩니다
      (운행 전체를 날짜로만 나누면 그룹 하나가 1,100만 행이라 Arrow 버퍼가 터집니다)

    기사 하나가 아니라 `bucket_size` 명을 묶는 이유는 **여유의 재분배** 입니다.
    선호 시간대가 드문 기사는 자기 몫만으로는 목표 운행 수를 못 채웁니다 — 실측
    여유 배수가 최소 1.3배, 2배 미만이 10명 있습니다. 같은 버킷의 여유 있는 기사와
    풀을 공유하면 그 편차가 메워집니다.
    """
    if bucket_size < 1:
        raise ValueError("bucket_size는 1 이상이어야 합니다")
    for frame, name, key in [
        (trips, "trips", "trip_key"),
        (preferences, "preferences", "driver_id"),
        (current_driver_vehicle, "current_driver_vehicle", "driver_id"),
    ]:
        _validate(frame, name, key)
    invalid_weights = preferences.filter(
        col("time_block_weights").isNull() | (size("time_block_weights") != len(TIME_BLOCKS))
    ).limit(1).count()
    if invalid_weights:
        raise ValueError(f"time_block_weights는 {len(TIME_BLOCKS)}개여야 합니다")

    drivers = (
        preferences.join(current_driver_vehicle, "driver_id", "inner")
        .withColumn("_driver_index", row_number().over(Window.orderBy("driver_id")))
        # 2,000행이지만 파티션 없는 윈도우가 붙어 있어, 캐시하지 않으면 아래 조인과
        # 이후 모든 action 에서 조인 + 윈도우가 통째로 다시 돕니다. 로그에
        # `WindowExec: No Partition Defined` 경고가 반복되는 것이 그 흔적입니다 (#360).
        .cache()
    )
    driver_count = drivers.count()
    if driver_count == 0:
        raise ValueError("기사·차량을 결합한 후보 차원이 비어 있습니다")

    # 기사를 버킷에 돌아가며 배치합니다. `_driver_index` 가 1..N 연속이라 나머지
    # 연산만으로 각 버킷에 bucket_size 명씩 고르게 들어갑니다.
    bucket_count = max(1, driver_count // bucket_size)
    drivers = drivers.withColumn("_bucket", pmod(col("_driver_index") - lit(1), lit(bucket_count)))

    trip_base = trips.drop("driver_id", "taxi_id", "taxi_model_id")
    # 운행도 같은 버킷 수로 해시해 **한 버킷에만** 들어갑니다. 해시 입력이
    # (trip_key, seed) 라 같은 seed 는 같은 분할을 냅니다.
    candidates = trip_base.withColumn(
        "_bucket", pmod(xxhash64("trip_key", lit(seed)), lit(bucket_count))
    ).join(drivers, "_bucket", "inner")
    # dropDuplicates(["trip_key", "driver_id"]) 를 두지 않습니다. 한 기사가 계약을
    # 여러 건 가진 경우에만 남는 중복인데, 아래 active_contract 필터가 서비스일에
    # 유효한 계약 하나만 남깁니다(생성기가 동시 활성 계약을 금지 — snapshot.py:219).
    # 임의의 계약을 고르던 예전 동작보다 오히려 정확합니다. 혹시 남더라도
    # allocator._validate 가 (trip_key, driver_id) 유일성을 검사해 잡습니다.

    time_block_index = floor(hour("pickup_datetime") / lit(3)).cast("int")
    # Spark `dayofweek()` 는 1=일~7=토 입니다. `weekday_mask` 는 전처리 쪽(D9 파생
    # 시드 없는 곳, preference.py/traits.py)이 이미 쓰는 파이썬 `datetime.weekday()`
    # 규칙(0=월~6=일)이라 여기서 그 규칙으로 맞춰 비트 인덱스를 만듭니다.
    weekday_index = pmod(dayofweek("pickup_datetime") + lit(5), lit(7))
    candidates = (
        candidates.withColumn("_service_date", to_date("pickup_datetime"))
        .withColumn("_weekday_index", weekday_index)
        .withColumn("_time_block_index", time_block_index)
    )
    active_contract = (
        (col("lease_started_on") <= col("_service_date"))
        & (col("lease_ended_on").isNull() | (col("_service_date") < col("lease_ended_on")))
    )
    vehicle_eligible = (
        (col("estimated_service_tier") == "Standard")
        | ((col("platform_name") == "Uber") & (col("estimated_service_tier") == "Comfort")
           & col("uber_comfort_eligible"))
        | ((col("platform_name") == "Lyft") & (col("estimated_service_tier") == "Extra Comfort")
           & col("lyft_extra_comfort_eligible"))
    )
    # `shiftleft` 는 이동 비트수로 정수 리터럴만 받아 행마다 다른 인덱스에 못 씁니다
    # — `2**index` 를 bigint 로 캐스팅해 같은 비트값을 만듭니다.
    weekday_bit = spark_pow(lit(2), col("_weekday_index")).cast("bigint")
    time_block_bit = spark_pow(lit(2), col("_time_block_index")).cast("bigint")
    profile_ok = (
        (col("weekday_mask").bitwiseAND(weekday_bit) != 0)
        & (col("time_block_mask").bitwiseAND(time_block_bit) != 0)
    )
    # 탈락 사유별 건수를 한 번의 집계로 셉니다(#644) — 후보 행에 boolean/문자열
    # 사유 컬럼을 남기지 않습니다. `sub/prototype/attribution.py::candidates_for()`
    # 와 같은 순서(로스터 -> 등급 -> 프로필)로 세되, 순서는 최종 후보 집합에는
    # 영향이 없습니다(넷 다 AND 로 묶여 필터링되므로).
    candidates = (
        candidates.withColumn("_c1_roster_ok", active_contract)
        .withColumn("_c2_tier_ok", vehicle_eligible)
        .withColumn("_c6_profile_ok", profile_ok)
    )
    stats = candidates.agg(
        count(when(~col("_c1_roster_ok"), 1)).alias(C1_ROSTER),
        count(when(col("_c1_roster_ok") & ~col("_c2_tier_ok"), 1)).alias(C2_TIER),
        count(
            when(col("_c1_roster_ok") & col("_c2_tier_ok") & ~col("_c6_profile_ok"), 1)
        ).alias(C6_PROFILE),
    ).first()
    rejected = {
        # bucket_count <= driver_count이고 기사를 modulo로 배치하므로 모든 버킷에
        # 기사가 최소 한 명 있습니다. 모든 트립도 그 버킷 중 하나에 들어가므로
        # 전체 trip_key를 countDistinct할 필요 없이 이 값은 항상 0입니다.
        C_NO_CANDIDATE: 0,
        C1_ROSTER: stats[C1_ROSTER],
        C2_TIER: stats[C2_TIER],
        C6_PROFILE: stats[C6_PROFILE],
    }
    candidates = candidates.filter(
        col("_c1_roster_ok") & col("_c2_tier_ok") & col("_c6_profile_ok")
    ).drop("_c1_roster_ok", "_c2_tier_ok", "_c6_profile_ok")

    is_airport = (
        (col("pickup_service_zone") == "Airports") | (col("dropoff_service_zone") == "Airports")
    )
    is_manhattan = (col("pickup_borough") == "Manhattan") | (col("dropoff_borough") == "Manhattan")
    # 위 `vehicle_eligible` 필터를 통과한 뒤라 Standard 가 아닌 행은 전부 "자격이 되는
    # 프리미엄 운행"입니다. 플랫폼별 등급명(Comfort / Extra Comfort)을 다시 나눌 필요가
    # 없는 이유는 한 플랫폼 안에서 등급이 2개뿐이기 때문입니다.
    is_premium = col("estimated_service_tier") != "Standard"
    result = (
        candidates.withColumn("time_score", element_at("time_block_weights", col("_time_block_index") + 1))
        .withColumn(
            "distance_score",
            greatest(lit(0.0), lit(1.0) - spark_abs(col("trip_miles") - col("preferred_distance_miles"))
                     / greatest(col("preferred_distance_miles"), lit(1.0))),
        )
        .withColumn("airport_score", when(is_airport, col("airport_preference")).otherwise(1.0 - col("airport_preference")))
        .withColumn("manhattan_score", when(is_manhattan, col("manhattan_preference")).otherwise(1.0 - col("manhattan_preference")))
        .withColumn("tier_score", when(is_premium, col("tier_preference")).otherwise(1.0 - col("tier_preference")))
        .withColumn(
            "preference_score",
            col("time_score") * score_weights["time"]
            + col("distance_score") * score_weights["distance"]
            + col("airport_score") * score_weights["airport"]
            + col("manhattan_score") * score_weights["manhattan"]
            + col("tier_score") * score_weights["tier"],
        )
        .withColumn("tie_break", sha2(concat_ws(":", lit(seed), "trip_key", "driver_id"), 256))
        # `_bucket` 은 남깁니다 — 배정이 (버킷 × 날짜) 로 묶어 처리합니다.
        .drop("_driver_index", "_service_date", "_weekday_index", "_time_block_index")
    )
    return result, rejected
