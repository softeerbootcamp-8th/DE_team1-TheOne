"""기사 후보 생성·배정 결과를 회사 관계와 결합해 별도 Silver에 적재합니다."""

import argparse
from datetime import date

from pyspark import StorageLevel
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, to_date

from common.io import SparkParquetLoader
from common.session import get_or_create_spark_session
from jobs.driver_assignment.allocator import allocate_trips
from jobs.driver_assignment.candidates import build_trip_candidates

ASSIGNMENT_VERSION = "v1"


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
    month_trips = trips.filter(col("year_month") == year_month)
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
            col("p.airport_preference"), col("p.manhattan_preference"), col("p.target_daily_trips"),
            col("p.target_work_minutes"), col("p.max_deadhead_minutes"),
        )
        .withColumn("snapshot_date", lit(snapshot_date).cast("date"))
        .withColumn("assignment_seed", lit(seed))
        .withColumn("assignment_version", lit(ASSIGNMENT_VERSION))
    )
    assignment_count = assignments.count()
    if joined.count() != assignment_count or joined.select("trip_key").distinct().count() != assignment_count:
        raise ValueError("배정 행 수 또는 기사·고객·계약·택시 관계가 보존되지 않았습니다")
    return joined


def main(args_list: list[str] | None = None):
    parser = argparse.ArgumentParser(description="기사 배정 운행 Silver Spark job")
    for name in ("trips", "preferences", "customers", "leases", "taxis", "travel_times", "output"):
        parser.add_argument(f"--{name}_path", required=True)
    parser.add_argument("--year_month", required=True)
    parser.add_argument("--snapshot_date", required=True)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--pool_size", type=int, default=64)
    args = parser.parse_args(args_list)
    spark = get_or_create_spark_session("hvfhv_driver_trip_silver")
    read = spark.read.parquet
    trips, preferences = read(args.trips_path), read(args.preferences_path)
    customers, leases, taxis = read(args.customers_path), read(args.leases_path), read(args.taxis_path)
    # 캐시가 없으면 action 마다 트립 2천만 행부터 다시 계산합니다. `candidates` 는
    # 검증에서 두 번, `assignments` 는 검증에서 세 번 더 읽히는데 그때마다 슬롯
    # 전개와 greedy 배정(Python UDF)이 통째로 재실행됩니다 (#360).
    #
    # MEMORY_AND_DISK 인 이유: 후보가 수억 행이라 메모리만으로는 축출되고,
    # 축출되면 캐시가 없는 것과 같아집니다.
    candidates = build_trip_candidates(
        trips, preferences, customers, leases, taxis, seed=args.seed, pool_size=args.pool_size,
    ).persist(StorageLevel.MEMORY_AND_DISK)
    assignments = allocate_trips(candidates, read(args.travel_times_path)).persist(
        StorageLevel.MEMORY_AND_DISK
    )
    silver = build_driver_trip_silver(
        trips, assignments, preferences, customers, leases, taxis,
        year_month=args.year_month, snapshot_date=date.fromisoformat(args.snapshot_date), seed=args.seed,
    ).persist(StorageLevel.MEMORY_AND_DISK)

    try:
        # Loader 가 쓰기 직후 row_count 를 세려고 한 번 더 읽습니다(common/io.py).
        # `silver` 캐시가 없으면 그 한 줄에 전체 파이프라인이 다시 돕니다.
        return SparkParquetLoader(args.output_path, partition_by=["year_month"]).write(silver)
    finally:
        for frame in (silver, assignments, candidates):
            frame.unpersist()


if __name__ == "__main__":
    main()
