"""기사 후보 생성·배정 결과를 회사 관계와 결합해 별도 Silver에 적재합니다."""

import argparse
from datetime import date
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, countDistinct, lit, to_date

from common.io import SparkParquetLoader
from common.session import get_or_create_spark_session
from jobs.driver_assignment.allocator import allocate_trips
from jobs.driver_assignment.candidates import build_trip_candidates

# v2: 기사 선호 생성 규칙이 바뀌었습니다 (#372 — 선호 시간블록을 연속 구간으로,
# 공차 상한 5~15분 -> 10~25분). seed 가 같아도 v1 과 다른 결과가 나오므로, 표식을
# 올리지 않으면 두 벌의 결과를 파티션만 보고 구분할 수 없습니다.
# v3: 등급 선호(tier_preference)를 배정 점수에 넣었습니다 (#399). 같은 이유로 올립니다.
ASSIGNMENT_VERSION = "v3"


def build_driver_trip_silver(
    trips: DataFrame,
    assignments: DataFrame,
    preferences: DataFrame,
    customers: DataFrame,
    leases: DataFrame,
    taxis: DataFrame,
    *,
    year_month: str,
    snapshot_date: date,
    seed: int,
) -> DataFrame:
    """배정된 운행만 사실·기사·회사 스냅샷과 일대일로 결합합니다."""
    if assignments.isEmpty():
        raise ValueError("기사 배정 결과가 0건입니다")
    if assignments.filter(col("trip_key").isNull()).limit(1).count() or assignments.groupBy(
        "trip_key"
    ).count().filter(col("count") > 1).limit(1).count():
        raise ValueError("배정 trip_key는 null 없이 고유해야 합니다")
    if trips.filter(col("trip_key").isNull() | (col("year_month") != year_month)).limit(1).count():
        raise ValueError(f"HVFHV 운행 월이 요청 월과 다릅니다: {year_month}")
    # HVFHV Silver 는 `driver_id` · `taxi_id` · `taxi_model_id` 를 NULL 자리표시로
    # 들고 있습니다 — 채우는 것이 바로 이 job 입니다. 빼지 않으면 아래 `t.*` 가
    # 그 빈 컬럼을 가져오고, 배정 결과의 `a.driver_id` 와 이름이 겹칩니다.
    # `select` 는 중복 이름을 허용해서 조용히 지나가고, **쓰기 단계에서야**
    # COLUMN_ALREADY_EXISTS 로 죽습니다. `candidates.py` 도 같은 이유로 뺍니다.
    month_trips = trips.filter(col("year_month") == year_month).drop(
        "driver_id", "taxi_id", "taxi_model_id"
    )
    if month_trips.isEmpty():
        raise ValueError(f"대상 월 HVFHV 운행이 없습니다: {year_month}")

    c = customers.filter(col("snapshot_date") == lit(snapshot_date)).alias("c")
    l = leases.filter(col("snapshot_date") == lit(snapshot_date)).alias("l")
    x = taxis.filter(col("snapshot_date") == lit(snapshot_date)).alias("x")
    a, t, p = assignments.alias("a"), month_trips.alias("t"), preferences.alias("p")
    joined = (
        a.join(t, col("a.trip_key") == col("t.trip_key"), "inner")
        .join(p, col("a.driver_id") == col("p.driver_id"), "inner")
        .join(c, col("a.driver_id") == col("c.synthetic_driver_id"), "inner")
        .join(l, (col("c.customer_id") == col("l.customer_id")) & (col("a.taxi_id") == col("l.taxi_id")), "inner")
        .join(x, col("a.taxi_id") == col("x.taxi_id"), "inner")
        .filter(
            (col("l.lease_started_on") <= to_date(col("t.pickup_datetime")))
            & (col("l.lease_ended_on").isNull() | (to_date(col("t.pickup_datetime")) < col("l.lease_ended_on")))
        )
        .select(
            "t.*", col("a.driver_id"), col("a.taxi_id"), col("a.trip_sequence"),
            col("a.deadhead_minutes"), col("a.preference_score"),
            col("c.customer_id"), col("l.lease_id"), col("l.lease_started_on"), col("l.lease_ended_on"),
            col("x.make_key"), col("x.model_key"), col("x.model_year"), col("x.vehicle_group"),
            col("x.uber_comfort_eligible"), col("x.lyft_extra_comfort_eligible"),
            col("p.active_weekdays"), col("p.preferred_time_blocks"), col("p.preferred_distance_miles"),
            col("p.airport_preference"), col("p.manhattan_preference"), col("p.tier_preference"),
            col("p.target_daily_trips"),
            col("p.target_work_minutes"), col("p.max_deadhead_minutes"),
        )
        .withColumn("snapshot_date", lit(snapshot_date).cast("date"))
        .withColumn("assignment_seed", lit(seed))
        .withColumn("assignment_version", lit(ASSIGNMENT_VERSION))
        # 검증 두 번과 적재에서 각각 다시 읽힙니다. 캐시하지 않으면 위 5중 조인이
        # 그때마다 통째로 재실행됩니다 — 이 파이프라인에서 가장 비싼 단계입니다.
        .persist(StorageLevel.DISK_ONLY)
    )
    assignment_count = assignments.count()
    # 행 수와 trip_key 유일성을 **한 번의 스캔**으로 봅니다. 예전에는
    # `joined.count()` 와 `joined.select("trip_key").distinct().count()` 를 따로
    # 불러 5중 조인이 두 번 더 돌았습니다. 검사 내용은 동일합니다.
    stats = joined.agg(
        count(lit(1)).alias("rows"),
        countDistinct("trip_key").alias("distinct_trips"),
    ).first()
    if stats["rows"] != assignment_count or stats["distinct_trips"] != assignment_count:
        raise ValueError("배정 행 수 또는 기사·고객·계약·택시 관계가 보존되지 않았습니다")
    return joined


def read_trips(spark, trips_path: str) -> DataFrame:
    """HVFHV Silver 를 읽되 `year_month` 파티션 컬럼을 살립니다.

    DAG 는 `.../hvfhv/year_month=2026-06` 처럼 파티션 디렉터리를 직접 넘깁니다.
    그 경로를 그대로 읽으면 `year_month` 는 **디렉터리 이름에만 있고 parquet
    안에는 없어서** 컬럼이 사라집니다. 아래 검증과 출력 파티셔닝이 그 컬럼을
    쓰므로 UNRESOLVED_COLUMN 으로 죽습니다.

    `basePath` 로 부모를 알려주면 Spark 가 디렉터리 이름에서 값을 되살립니다.
    대상 월 하나만 읽는다는 성질은 그대로라, "요청 월 외 데이터가 섞이면 실패"
    하는 검증도 의미를 유지합니다.
    """
    path = Path(trips_path)
    if path.name.startswith("year_month="):
        return spark.read.option("basePath", str(path.parent)).parquet(str(path))
    return spark.read.parquet(trips_path)


def main(args_list: list[str] | None = None):
    parser = argparse.ArgumentParser(description="기사 배정 운행 Silver Spark job")
    for name in ("trips", "preferences", "customers", "leases", "taxis", "travel_times", "output"):
        parser.add_argument(f"--{name}_path", required=True)
    parser.add_argument("--year_month", required=True)
    parser.add_argument("--snapshot_date", required=True)
    parser.add_argument("--seed", type=int, default=42)
    # 기사 몇 명을 한 버킷으로 묶을지. 이 값이 곧 후보 행 수(트립 × bucket_size)이자
    # 배정 그룹 크기입니다. 작을수록 가볍지만, 선호가 드문 기사가 자기 버킷 안에서
    # 목표 운행 수를 못 채울 위험이 커집니다.
    parser.add_argument("--bucket_size", type=int, default=5)
    # 없으면 Spark 기본값 1g 로 돕니다. 이 job 은 bronze_to_silver 보다 무거운데
    # (트립 x 후보 슬롯) 인자만 빠져 있어서 로컬에서 항상 힙이 터졌습니다.
    # bronze_to_silver/hvfhv/job.py 와 같은 기본값으로 맞춥니다.
    parser.add_argument("--spark_memory", default="4g", help="Spark driver memory")
    args = parser.parse_args(args_list)
    spark = get_or_create_spark_session("hvfhv_driver_trip_silver", driver_memory=args.spark_memory)
    # 후보 생성이 트립 한 행을 버킷 인원수만큼 늘립니다. 읽기 파티션을 기본값
    # (128MB)으로 두면 파티션 하나가 그 배수로 부풀어, 측정상 1.7GB 짜리 persist
    # 블록이 생깁니다. MEMORY_AND_DISK 는 디스크로 흘린 블록을 다시 읽을 때 통째로
    # 메모리에 올리려 하므로(BlockManager.maybeCacheDiskBytesInMemory) 거기서
    # OutOfMemoryError 가 납니다.
    #
    # 미리 그 배수만큼 잘게 읽으면 explode 후 파티션이 다시 128MB 언저리가 됩니다.
    # 셔플이 아니라 파일 스캔 분할이라 추가 비용이 없습니다.
    spark.conf.set(
        "spark.sql.files.maxPartitionBytes", str(128 * 1024 * 1024 // max(1, args.bucket_size))
    )
    read = spark.read.parquet
    trips, preferences = read_trips(spark, args.trips_path), read(args.preferences_path)
    customers, leases, taxis = read(args.customers_path), read(args.leases_path), read(args.taxis_path)
    # 캐시가 없으면 action 마다 트립 2천만 행부터 다시 계산합니다. `candidates` 는
    # 검증에서 두 번, `assignments` 는 검증에서 세 번 더 읽히는데 그때마다 슬롯
    # 전개와 greedy 배정(Python UDF)이 통째로 재실행됩니다 (#360).
    #
    # MEMORY_AND_DISK 인 이유: 후보가 수억 행이라 메모리만으로는 축출되고,
    # 축출되면 캐시가 없는 것과 같아집니다.
    candidates = build_trip_candidates(
        trips, preferences, customers, leases, taxis, seed=args.seed, bucket_size=args.bucket_size,
    ).persist(StorageLevel.DISK_ONLY) 
    assignments = allocate_trips(candidates, read(args.travel_times_path)).persist(
        StorageLevel.MEMORY_AND_DISK
    )
    silver = build_driver_trip_silver(
        trips, assignments, preferences, customers, leases, taxis,
        year_month=args.year_month, snapshot_date=date.fromisoformat(args.snapshot_date), seed=args.seed,
    )  # build_driver_trip_silver 안에서 이미 persist 합니다

    try:
        # Loader 가 쓰기 직후 row_count 를 세려고 한 번 더 읽습니다(common/io.py).
        # `silver` 캐시가 없으면 그 한 줄에 전체 파이프라인이 다시 돕니다.
        return SparkParquetLoader(args.output_path, partition_by=["year_month"]).write(silver)
    finally:
        for frame in (silver, assignments, candidates):
            frame.unpersist()


if __name__ == "__main__":
    main()
