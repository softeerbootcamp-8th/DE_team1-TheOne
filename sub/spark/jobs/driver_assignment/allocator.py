"""점수화된 후보를 날짜별로 시공간 제약에 맞춰 결정적으로 배정합니다."""

from datetime import timedelta

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, to_date
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

CANDIDATE_COLUMNS = {
    "trip_key", "driver_id", "taxi_id", "pickup_datetime", "dropoff_datetime",
    "PULocationID", "DOLocationID", "preference_score", "tie_break",
    "target_work_minutes", "target_drive_minutes", "max_deadhead_minutes",
}
TRAVEL_COLUMNS = {"from_location_id", "to_location_id", "travel_minutes"}
ASSIGNMENT_SCHEMA = StructType([
    StructField("trip_key", StringType(), False),
    StructField("driver_id", StringType(), False),
    StructField("taxi_id", StringType(), False),
    StructField("service_date", DateType(), False),
    StructField("trip_sequence", IntegerType(), False),
    StructField("pickup_datetime", TimestampType(), False),
    StructField("dropoff_datetime", TimestampType(), False),
    StructField("PULocationID", IntegerType(), False),
    StructField("DOLocationID", IntegerType(), False),
    StructField("preference_score", DoubleType(), False),
    StructField("tie_break", StringType(), False),
    StructField("deadhead_minutes", DoubleType(), False),
])
OUTPUT_COLUMNS = [field.name for field in ASSIGNMENT_SCHEMA]


def _require_columns(frame: DataFrame, required: set[str], name: str) -> None:
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"{name} 필수 컬럼 누락: {sorted(missing)}")


def _validate(candidates: DataFrame, travel_times: DataFrame) -> None:
    _require_columns(candidates, CANDIDATE_COLUMNS, "candidates")
    _require_columns(travel_times, TRAVEL_COLUMNS, "travel_times")
    if candidates.filter(
        col("trip_key").isNull() | col("driver_id").isNull() | col("taxi_id").isNull()
        | col("pickup_datetime").isNull() | col("dropoff_datetime").isNull()
        | (col("pickup_datetime") >= col("dropoff_datetime"))
        | (col("target_drive_minutes") < 1) | (col("target_work_minutes") < 1)
        | (col("max_deadhead_minutes") < 0)
    ).limit(1).count():
        raise ValueError("후보의 ID·시간·일일 한도 계약이 올바르지 않습니다")
    if candidates.groupBy("trip_key", "driver_id").count().filter(col("count") > 1).limit(1).count():
        raise ValueError("trip_key와 driver_id 후보 쌍은 중복될 수 없습니다")
    if travel_times.filter(
        col("from_location_id").isNull() | col("to_location_id").isNull()
        | col("travel_minutes").isNull() | (col("travel_minutes") < 0)
    ).limit(1).count():
        raise ValueError("구역 이동시간은 null이 아닌 0 이상의 값이어야 합니다")
    if travel_times.groupBy("from_location_id", "to_location_id").count().filter(
        col("count") > 1
    ).limit(1).count():
        raise ValueError("구역 이동시간 키는 중복될 수 없습니다")


def allocation_input(candidates: DataFrame) -> DataFrame:
    """파이썬으로 넘길 컬럼만 남깁니다.

    `applyInPandas` 는 날짜 그룹 하나를 **통째로 Arrow 배치로 만들어** 보냅니다.
    그 버퍼는 JVM 힙이 아니라 직접(off-heap) 메모리라 `--spark_memory` 로 늘릴 수
    없고, 넘치면 Arrow 의 UnpooledDirectByteBuf 할당에서 죽습니다.

    후보에는 51개 컬럼이 실려 있는데 `_allocate_day` 가 보는 건 12개뿐입니다.
    나머지는 존 이름·요금 8종·선호 배열(`time_block_weights` 는 행마다 8개)처럼
    배정과 무관하면서 Arrow 에서 자리를 많이 먹는 것들입니다.
    """
    frame = candidates.withColumn("_service_date", to_date("pickup_datetime"))
    # 후보 생성이 버킷을 붙여 줍니다. 이 함수만 따로 쓰는 경우(테스트 등)에는
    # 전체를 버킷 하나로 봅니다.
    if "_bucket" not in frame.columns:
        frame = frame.withColumn("_bucket", lit(0))
    return frame.select("_bucket", "_service_date", *sorted(CANDIDATE_COLUMNS))


def _allocate_day(frame: pd.DataFrame, travel: dict[tuple[int, int], float]) -> pd.DataFrame:
    """제약 3·4·5 를 순차 상태로 지키며 greedy 배정.

    이 세 제약은 벡터화할 수 없습니다. "이 기사가 이 트립을 받을 수 있는가"가
    그 기사가 **이미 받은 트립들**에 의존하기 때문입니다. 그래서 픽업 시각 순으로
    훑습니다 — 여기 들어오는 `frame` 은 이미 (버킷, 서비스일) 하나 몫뿐이라
    (`allocate_trips` 의 `groupBy` 가 나눠 줌) 그룹 경계를 다시 추적할 필요가
    없습니다.

    벡터화가 안 되는 것과 **pandas 객체를 행마다 만드는 것**은 다른 문제입니다.
    예전에는 트립마다 `groupby("trip_key")` + `itertuples` 를 불렀는데, 트립
    하나당 namedtuple 생성 + 컬럼별 `.iloc` 가 붙습니다. 482k 트립 한 part 에서
    그것만 494초 — 전체 실행의 93% 였습니다(`docs/blog/03_speed_optimization_reference.md`
    7.3). 지금은 정렬한 뒤 컬럼을 파이썬 리스트로 뽑아 한 번만 훑고, 트립 경계에서
    "이미 배정됨" 플래그로 다음 후보로 넘어갑니다 — `sub/prototype/attribution.py::allocate()`
    와 같은 알고리즘입니다.

    하루 상한은 트립 수(`target_daily_trips`)가 아니라 누적 운행분(승객 태운 시간 +
    공차, `target_drive_minutes`)입니다 — 첫 트립도 예산을 넘을 수 있어서
    `previous` 유무와 무관하게 매 트립에서 봅니다.
    """
    if frame.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    ordered = frame.sort_values(
        ["pickup_datetime", "trip_key", "preference_score", "tie_break", "driver_id"],
        ascending=[True, True, False, True, True],
        kind="stable",
    )

    trip_keys = ordered["trip_key"].tolist()
    driver_ids = ordered["driver_id"].tolist()
    pickups = ordered["pickup_datetime"].tolist()
    dropoffs = ordered["dropoff_datetime"].tolist()
    pickup_zones = ordered["PULocationID"].tolist()
    dropoff_zones = ordered["DOLocationID"].tolist()
    work_minute_caps = ordered["target_work_minutes"].tolist()
    drive_budgets = ordered["target_drive_minutes"].tolist()
    deadhead_caps = ordered["max_deadhead_minutes"].tolist()

    picked: list[int] = []
    sequences: list[int] = []
    deadheads: list[float] = []

    # driver_id -> (첫 픽업, 막 하차, 막 하차 구역, 트립 수, 누적 운행분)
    driver_state: dict[str, tuple] = {}
    current_trip = None
    trip_taken = False

    for row in range(len(trip_keys)):
        trip_key = trip_keys[row]
        if trip_key != current_trip:
            current_trip = trip_key
            trip_taken = False
        elif trip_taken:
            continue  # 이 트립은 이미 배정됐습니다 (예전의 `break`)

        driver_id = driver_ids[row]
        previous = driver_state.get(driver_id)
        pickup = pickups[row]
        dropoff = dropoffs[row]
        pickup_zone = int(pickup_zones[row])
        deadhead = 0.0
        sequence = 1
        first_pickup = pickup
        used = 0.0
        if previous is not None:
            first_pickup, previous_dropoff, previous_zone, count, used = previous
            if previous_zone != pickup_zone:
                pair = (previous_zone, pickup_zone)
                if pair not in travel:
                    continue
                deadhead = travel[pair]
            if deadhead > deadhead_caps[row]:
                continue
            if previous_dropoff + timedelta(minutes=deadhead) > pickup:
                continue
            if (dropoff - first_pickup).total_seconds() / 60 > work_minute_caps[row]:
                continue
            sequence = count + 1
        drive = (dropoff - pickup).total_seconds() / 60 + deadhead
        if used + drive > drive_budgets[row]:
            continue
        driver_state[driver_id] = (
            first_pickup, dropoff, int(dropoff_zones[row]), sequence, used + drive
        )
        picked.append(row)
        sequences.append(sequence)
        deadheads.append(deadhead)
        trip_taken = True

    if not picked:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)
    assigned = ordered.iloc[picked][[
        "trip_key", "driver_id", "taxi_id", "pickup_datetime", "dropoff_datetime",
        "PULocationID", "DOLocationID", "preference_score", "tie_break",
    ]].reset_index(drop=True)
    assigned["service_date"] = [pickups[row].date() for row in picked]
    assigned["trip_sequence"] = sequences
    assigned["deadhead_minutes"] = deadheads
    # 존 ID 는 Arrow 로 올라올 때 int32 일 수 있습니다. 예전 경로(파이썬 int)와
    # 같은 int64 로 맞춰 산출물 스키마가 바뀌지 않게 합니다.
    assigned["PULocationID"] = assigned["PULocationID"].astype("int64")
    assigned["DOLocationID"] = assigned["DOLocationID"].astype("int64")
    assigned["preference_score"] = assigned["preference_score"].astype("float64")
    return assigned[OUTPUT_COLUMNS]


def allocate_trips(candidates: DataFrame, travel_times: DataFrame) -> DataFrame:
    """날짜별 greedy 배정으로 운행 단일성과 기사별 시공간 연결을 보장합니다."""
    # 검증에서 두 번, 아래 collect 에서 한 번 읽습니다. 캐시하지 않으면 원본
    # Parquet 을 세 번 스캔합니다 (#360).
    travel_times = travel_times.cache()
    _validate(candidates, travel_times)
    if candidates.isEmpty():
        return candidates.sparkSession.createDataFrame([], ASSIGNMENT_SCHEMA)
    travel = {
        (int(row.from_location_id), int(row.to_location_id)): float(row.travel_minutes)
        for row in travel_times.collect()
    }

    def allocate_group(frame: pd.DataFrame) -> pd.DataFrame:
        return _allocate_day(frame, travel)

    # (버킷 × 날짜) 로 묶습니다. 날짜로만 묶으면 그룹 하나가 하루치 전체(1,100만 행)
    # 라 Arrow 가 파이썬으로 넘길 배치를 만들다 직접 메모리에서 죽습니다. 버킷을
    # 함께 넣으면 그룹이 3만 행대로 떨어지고 병렬성은 200배가 됩니다.
    #
    # `_allocate_day` 의 상태는 (기사, 하루) 단위인데, 기사는 버킷 하나에만 속하므로
    # 그룹 안에 그 기사의 그날 후보가 모두 들어옵니다 — 로직을 바꿀 필요가 없습니다.
    return (
        allocation_input(candidates)
        .groupBy("_bucket", "_service_date")
        .applyInPandas(allocate_group, schema=ASSIGNMENT_SCHEMA)
    )
