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
    "target_daily_trips", "target_work_minutes", "max_deadhead_minutes",
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
        | (col("target_daily_trips") < 1) | (col("target_work_minutes") < 1)
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
    state: dict[str, dict] = {}
    assigned: list[dict] = []
    ordered = frame.sort_values(
        ["pickup_datetime", "trip_key", "preference_score", "tie_break", "driver_id"],
        ascending=[True, True, False, True, True],
        kind="stable",
    )
    for _, trip_candidates in ordered.groupby("trip_key", sort=False):
        for candidate in trip_candidates.itertuples(index=False):
            previous = state.get(candidate.driver_id)
            deadhead = 0.0
            sequence = 1
            if previous:
                if previous["count"] >= candidate.target_daily_trips:
                    continue
                pair = (int(previous["dropoff_zone"]), int(candidate.PULocationID))
                if pair[0] != pair[1] and pair not in travel:
                    continue
                deadhead = 0.0 if pair[0] == pair[1] else travel[pair]
                if deadhead > candidate.max_deadhead_minutes:
                    continue
                if previous["dropoff"] + timedelta(minutes=deadhead) > candidate.pickup_datetime:
                    continue
                if (candidate.dropoff_datetime - previous["first_pickup"]).total_seconds() / 60 > candidate.target_work_minutes:
                    continue
                sequence = previous["count"] + 1
            state[candidate.driver_id] = {
                "first_pickup": previous["first_pickup"] if previous else candidate.pickup_datetime,
                "dropoff": candidate.dropoff_datetime,
                "dropoff_zone": candidate.DOLocationID,
                "count": sequence,
            }
            assigned.append({
                "trip_key": candidate.trip_key, "driver_id": candidate.driver_id,
                "taxi_id": candidate.taxi_id, "service_date": candidate.pickup_datetime.date(),
                "trip_sequence": sequence, "pickup_datetime": candidate.pickup_datetime,
                "dropoff_datetime": candidate.dropoff_datetime,
                "PULocationID": int(candidate.PULocationID), "DOLocationID": int(candidate.DOLocationID),
                "preference_score": float(candidate.preference_score), "tie_break": candidate.tie_break,
                "deadhead_minutes": float(deadhead),
            })
            break
    return pd.DataFrame(assigned, columns=OUTPUT_COLUMNS)


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
