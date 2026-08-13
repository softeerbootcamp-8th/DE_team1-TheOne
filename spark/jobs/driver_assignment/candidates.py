"""운행별 소규모 기사 후보군에 hard constraint와 선호 점수를 적용합니다."""

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    abs as spark_abs,
    array,
    array_contains,
    array_distinct,
    col,
    concat_ws,
    dayofweek,
    element_at,
    explode,
    floor,
    greatest,
    hour,
    lit,
    pmod,
    row_number,
    sequence,
    sha2,
    size,
    to_date,
    transform,
    when,
    xxhash64,
)
TIME_BLOCKS = ["00-03", "03-06", "06-09", "09-12", "12-15", "15-18", "18-21", "21-24"]
WEEKDAYS = ["SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"]
SCORE_WEIGHTS = {"time": 0.35, "distance": 0.30, "airport": 0.20, "manhattan": 0.15}

REQUIRED = {
    "trips": {
        "trip_key", "pickup_datetime", "dropoff_datetime", "trip_miles",
        "platform_name", "estimated_service_tier", "pickup_borough", "dropoff_borough",
        "pickup_service_zone", "dropoff_service_zone",
    },
    "preferences": {
        "driver_id", "active_weekdays", "preferred_time_blocks", "time_block_weights",
        "preferred_distance_miles", "airport_preference", "manhattan_preference",
        "target_daily_trips", "target_work_minutes", "max_deadhead_minutes",
    },
    "customers": {"customer_id", "synthetic_driver_id"},
    "leases": {"lease_id", "customer_id", "taxi_id", "lease_started_on", "lease_ended_on"},
    "taxis": {"taxi_id", "uber_comfort_eligible", "lyft_extra_comfort_eligible"},
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
    customers: DataFrame,
    leases: DataFrame,
    taxis: DataFrame,
    *,
    seed: int = 42,
    pool_size: int = 64,
) -> DataFrame:
    """운행별 결정적 후보군을 만든 뒤 자격 조건과 선호 점수를 계산합니다."""
    if pool_size < 1:
        raise ValueError("pool_size는 1 이상이어야 합니다")
    for frame, name, key in [
        (trips, "trips", "trip_key"),
        (preferences, "preferences", "driver_id"),
        (customers, "customers", "customer_id"),
        (leases, "leases", "lease_id"),
        (taxis, "taxis", "taxi_id"),
    ]:
        _validate(frame, name, key)
    invalid_weights = preferences.filter(
        col("time_block_weights").isNull() | (size("time_block_weights") != len(TIME_BLOCKS))
    ).limit(1).count()
    if invalid_weights:
        raise ValueError(f"time_block_weights는 {len(TIME_BLOCKS)}개여야 합니다")

    drivers = (
        preferences.join(customers, preferences.driver_id == customers.synthetic_driver_id, "inner")
        .drop("synthetic_driver_id")
        .join(leases, "customer_id", "inner")
        .join(taxis.withColumnRenamed("taxi_id", "_candidate_taxi_id"),
              leases.taxi_id == col("_candidate_taxi_id"), "inner")
        .drop(leases.taxi_id)
        .withColumn("_driver_index", row_number().over(Window.orderBy("driver_id", "lease_id")))
        # 2,000행이지만 파티션 없는 윈도우가 붙어 있어, 캐시하지 않으면 아래 조인과
        # 이후 모든 action 에서 조인 3개 + 윈도우가 통째로 다시 돕니다. 로그에
        # `WindowExec: No Partition Defined` 경고가 반복되는 것이 그 흔적입니다 (#360).
        .cache()
    )
    driver_count = drivers.count()
    if driver_count == 0:
        raise ValueError("기사·계약·택시를 결합한 후보 차원이 비어 있습니다")

    slot_count = min(pool_size, driver_count)
    trip_base = trips.drop("driver_id", "taxi_id", "taxi_model_id")
    # 슬롯을 먼저 explode 하면 안 됩니다. 슬롯 64개를 기사 2,000명에 해시로 뿌리는
    # 구조라 한 trip 안에서 같은 기사가 평균 1명 겹치는데, 예전 코드는 20.9M x 64 =
    # 13.4억 행을 만든 뒤 dropDuplicates 로 그 1.6% 를 지웠습니다. 그 셔플 하나가
    # local[1] 1GB 힙을 OutOfMemoryError 로 터뜨립니다.
    #
    # 배열 안에서 중복을 먼저 없애면 결과는 같고 셔플은 0회입니다. 해시 입력이
    # (trip_key, seed, slot) 로 동일하므로 같은 seed 는 같은 후보 집합을 냅니다.
    candidates = trip_base.withColumn(
        "_driver_index",
        explode(
            array_distinct(
                transform(
                    sequence(lit(0), lit(slot_count - 1)),
                    lambda slot: pmod(xxhash64("trip_key", lit(seed), slot), lit(driver_count)) + lit(1),
                )
            )
        ),
    ).join(drivers, "_driver_index", "inner")
    # dropDuplicates(["trip_key", "driver_id"]) 를 두지 않습니다. 한 기사가 계약을
    # 여러 건 가진 경우에만 남는 중복인데, 아래 active_contract 필터가 서비스일에
    # 유효한 계약 하나만 남깁니다(생성기가 동시 활성 계약을 금지 — snapshot.py:219).
    # 임의의 계약을 고르던 예전 동작보다 오히려 정확합니다. 혹시 남더라도
    # allocator._validate 가 (trip_key, driver_id) 유일성을 검사해 잡습니다.

    time_block_index = floor(hour("pickup_datetime") / lit(3)).cast("int")
    candidates = (
        candidates.withColumn("_service_date", to_date("pickup_datetime"))
        .withColumn("_weekday", element_at(array(*(lit(v) for v in WEEKDAYS)), dayofweek("pickup_datetime")))
        .withColumn("_time_block_index", time_block_index)
        .withColumn("_time_block", element_at(array(*(lit(v) for v in TIME_BLOCKS)), time_block_index + 1))
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
    candidates = candidates.filter(
        active_contract
        & array_contains("active_weekdays", col("_weekday"))
        & array_contains("preferred_time_blocks", col("_time_block"))
        & vehicle_eligible
    )

    is_airport = (
        (col("pickup_service_zone") == "Airports") | (col("dropoff_service_zone") == "Airports")
    )
    is_manhattan = (col("pickup_borough") == "Manhattan") | (col("dropoff_borough") == "Manhattan")
    result = (
        candidates.withColumn("time_score", element_at("time_block_weights", col("_time_block_index") + 1))
        .withColumn(
            "distance_score",
            greatest(lit(0.0), lit(1.0) - spark_abs(col("trip_miles") - col("preferred_distance_miles"))
                     / greatest(col("preferred_distance_miles"), lit(1.0))),
        )
        .withColumn("airport_score", when(is_airport, col("airport_preference")).otherwise(1.0 - col("airport_preference")))
        .withColumn("manhattan_score", when(is_manhattan, col("manhattan_preference")).otherwise(1.0 - col("manhattan_preference")))
        .withColumn(
            "preference_score",
            col("time_score") * SCORE_WEIGHTS["time"]
            + col("distance_score") * SCORE_WEIGHTS["distance"]
            + col("airport_score") * SCORE_WEIGHTS["airport"]
            + col("manhattan_score") * SCORE_WEIGHTS["manhattan"],
        )
        .withColumn("tie_break", sha2(concat_ws(":", lit(seed), "trip_key", "driver_id"), 256))
        .withColumnRenamed("_candidate_taxi_id", "taxi_id")
        .drop("_driver_index", "_service_date", "_weekday", "_time_block_index", "_time_block")
    )
    return result
