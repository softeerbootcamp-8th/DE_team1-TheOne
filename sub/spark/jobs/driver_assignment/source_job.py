"""월별 기사 배정 결과를 운행·리스·보유 차량 데이터로 분리합니다."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import uuid
from datetime import date
from pathlib import Path

import pyarrow.parquet as pq
from pyspark import StorageLevel
from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    col,
    concat_ws,
    count,
    countDistinct,
    lit,
    monotonically_increasing_id,
    row_number,
    sha2,
    struct,
    to_date,
    to_json,
)

from schema.source import (
    DRIVER_VEHICLE_LEASE_SCHEMA,
    LEASE_VEHICLE_INVENTORY_SCHEMA,
)
from shared.spark.common.session import get_or_create_spark_session
from shared.spark.hvfhv_clean_transformer import (
    TRIP_KEY_COLUMNS,
    HVFHVCleanTransformer,
)
from sub.generators.synthetic_driver_trip_source.monthly import prepare_monthly_state
from sub.spark.jobs.driver_assignment.allocator import allocate_trips
from sub.spark.jobs.driver_assignment.candidates import build_trip_candidates
from sub.spark.jobs.travel_times.transformer import build_travel_times

LEASE_SOURCE_COLUMNS = DRIVER_VEHICLE_LEASE_SCHEMA.names
INVENTORY_COLUMNS = LEASE_VEHICLE_INVENTORY_SCHEMA.names


def _test_scoped_root(path: str | Path, test_row_limit: int) -> Path:
    if test_row_limit < 0:
        raise ValueError("test_row_limit는 0 이상이어야 합니다")
    root = Path(path)
    if test_row_limit == 0:
        return root
    return root / "_temporary" / f"test_row_limit={test_row_limit}"


def _apply_test_row_limit(frame: DataFrame, test_row_limit: int) -> DataFrame:
    """TEMPORARY(#452): 전체 월 DAG 검증이 끝나면 제거할 smoke-test 제한입니다."""
    if test_row_limit < 0:
        raise ValueError("test_row_limit는 0 이상이어야 합니다")
    return frame if test_row_limit == 0 else frame.limit(test_row_limit)


def add_trip_keys(raw_trips: DataFrame) -> DataFrame:
    """HVFHV Silver와 같은 자연키·중복 순번 규칙으로 원본 행에 임시 키를 붙입니다."""
    missing = set(TRIP_KEY_COLUMNS) - set(raw_trips.columns)
    if missing:
        raise ValueError(f"HVFHV 원천 키 컬럼 누락: {sorted(missing)}")
    occurrence = Window.partitionBy(*TRIP_KEY_COLUMNS).orderBy(lit(1))
    keyed = raw_trips.withColumn("_trip_occurrence", row_number().over(occurrence))
    canonical = to_json(
        struct(
            *(col(name).alias(name) for name in TRIP_KEY_COLUMNS),
            col("_trip_occurrence"),
        ),
        options={"ignoreNullFields": "false"},
    )
    return keyed.withColumn("trip_key", sha2(canonical, 256)).drop("_trip_occurrence")


def build_trip_source(raw_trips: DataFrame, assignments: DataFrame) -> DataFrame:
    """배정된 원본 HVFHV 행에 taxi_id만 붙이고 내부 trip_key는 제거합니다."""
    if "taxi_id" in raw_trips.columns:
        raise ValueError("HVFHV 원천에는 배정 전 taxi_id가 없어야 합니다")
    required = {"trip_key", "taxi_id"}
    missing = required - set(assignments.columns)
    if missing:
        raise ValueError(f"배정 결과 컬럼 누락: {sorted(missing)}")
    stats = assignments.agg(
        count(lit(1)).alias("rows"),
        countDistinct("trip_key").alias("distinct_trips"),
    ).first()
    if not stats or stats["rows"] == 0 or stats["rows"] != stats["distinct_trips"]:
        raise ValueError("배정 trip_key는 1건 이상이며 고유해야 합니다")

    raw_columns = raw_trips.columns
    selected = assignments.select(
        "trip_key", col("taxi_id").alias("_assigned_taxi_id")
    )
    source = (
        add_trip_keys(raw_trips)
        .join(selected, "trip_key", "inner")
        .select(
            *(col(name) for name in raw_columns),
            col("_assigned_taxi_id").alias("taxi_id"),
        )
    )
    return source


def build_driver_vehicle_leases(
    customers: DataFrame,
    leases: DataFrame,
    taxis: DataFrame,
    *,
    snapshot_date: date,
) -> DataFrame:
    """월별 회사 스냅샷을 기사·차량 리스 이력 한 테이블로 접습니다."""
    c = customers.filter(col("snapshot_date") == lit(snapshot_date)).alias("c")
    l = leases.filter(col("snapshot_date") == lit(snapshot_date)).alias("l")
    x = taxis.filter(col("snapshot_date") == lit(snapshot_date)).alias("x")
    source = (
        l.join(c, col("l.customer_id") == col("c.customer_id"), "inner")
        .join(x, col("l.taxi_id") == col("x.taxi_id"), "inner")
        .select(
            col("l.lease_id"),
            col("l.customer_id"),
            col("c.synthetic_driver_id").alias("driver_id"),
            col("l.taxi_id"),
            col("x.make_key"),
            col("x.model_key"),
            col("x.model_year"),
            col("l.lease_started_on"),
            col("l.lease_ended_on"),
        )
    )
    expected = l.count()
    stats = source.agg(
        count(lit(1)).alias("rows"),
        countDistinct("lease_id").alias("distinct_leases"),
    ).first()
    if not stats or stats["rows"] != expected or stats["distinct_leases"] != expected:
        raise ValueError("기사·계약·차량 관계가 일대일로 보존되지 않았습니다")
    return source.select(*LEASE_SOURCE_COLUMNS)


def build_lease_vehicle_inventory(
    taxis: DataFrame,
    vehicle_master: DataFrame,
    *,
    snapshot_date: date,
) -> DataFrame:
    """보유 차량을 차종·연식별 API 재고로 집계합니다."""
    fleet = (
        taxis.filter(col("snapshot_date") == lit(snapshot_date)).groupBy(
            "make_key",
            "model_key",
            "model_year",
            "weekly_price_usd",
            "uber_comfort_eligible",
            "lyft_extra_comfort_eligible",
        )
        .agg(count(lit(1)).cast("int").alias("stock"))
    )
    metadata = vehicle_master.select(
        "make_key",
        "model_key",
        "fuel_type",
        "combined_mpg_min",
        "combined_mpg_max",
        "image_url",
    ).distinct()

    inventory = (
        fleet.join(metadata, ["make_key", "model_key"], "left")
        .withColumn("manufacturer", col("make_key"))
        .withColumn("model_name", col("model_key"))
        .withColumn(
            "vehicle_model_id",
            sha2(
                concat_ws(
                    ":",
                    col("manufacturer"),
                    col("model_name"),
                    col("model_year").cast("string"),
                ),
                256,
            ),
        )
        .withColumn(
            "fuel_efficiency",
            (col("combined_mpg_min") + col("combined_mpg_max")) / 2,
        )
        .select(
            col("vehicle_model_id"),
            col("manufacturer"),
            col("model_name"),
            col("model_year").cast("smallint").alias("model_year"),
            col("fuel_type"),
            col("fuel_efficiency").cast("double").alias("fuel_efficiency"),
            col("uber_comfort_eligible").alias("comfort_eligible"),
            col("lyft_extra_comfort_eligible").alias("extra_comfort_eligible"),
            col("weekly_price_usd").cast("double").alias("weekly_price_usd"),
            col("image_url"),
            col("stock"),
        )
    )
    stats = inventory.agg(
        count(lit(1)).alias("rows"),
        countDistinct("vehicle_model_id").alias("distinct_models"),
    ).first()
    invalid = inventory.filter(
        col("vehicle_model_id").isNull()
        | col("fuel_type").isNull()
        | col("fuel_efficiency").isNull()
        | (col("fuel_efficiency") <= 0)
        | col("image_url").isNull()
        | (col("image_url") == "")
        | (col("weekly_price_usd") <= 0)
        | (col("stock") <= 0)
    ).limit(1)
    if (
        not stats
        or stats["rows"] == 0
        or stats["rows"] != stats["distinct_models"]
        or invalid.count()
    ):
        raise ValueError("보유 차량 API 데이터의 ID·연료·가격·이미지·재고가 올바르지 않습니다")
    return inventory.select(*INVENTORY_COLUMNS)


def _validate_temporal_links(trips: DataFrame, leases: DataFrame) -> None:
    rows = trips.withColumn("_source_row_id", monotonically_increasing_id()).alias("t")
    matched = rows.join(
        leases.alias("l"),
        (col("t.taxi_id") == col("l.taxi_id"))
        & (col("l.lease_started_on") <= to_date(col("t.pickup_datetime")))
        & (
            col("l.lease_ended_on").isNull()
            | (to_date(col("t.pickup_datetime")) < col("l.lease_ended_on"))
        ),
        "left",
    )
    invalid = (
        matched.groupBy("_source_row_id")
        .agg(count("l.lease_id").alias("matches"))
        .filter(col("matches") != 1)
        .limit(1)
        .count()
    )
    if invalid:
        raise ValueError("모든 HVFHV 행은 운행 시점의 리스 한 건과 연결돼야 합니다")


def _existing_release(path: Path, year_month: str, seed: int) -> bool:
    manifest_path = path / "manifest.json"
    if not path.exists():
        return False
    if not manifest_path.is_file():
        raise ValueError(f"완료되지 않은 릴리스 경로가 남아 있습니다: {path}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("year_month") != year_month or manifest.get("seed") != seed:
        raise ValueError(f"기존 릴리스 계보가 요청과 다릅니다: {manifest}")
    for name in (
        "hvfhv_taxi_trips",
        "driver_vehicle_leases",
        "lease_vehicle_inventory",
    ):
        dataset = manifest.get("datasets", {}).get(name, {})
        file_path = path / str(dataset.get("file", ""))
        if not file_path.is_file():
            raise ValueError(f"기존 릴리스 데이터셋이 없습니다: {file_path}")
    return True


def _write_one_parquet(frame: DataFrame, path: Path) -> None:
    # ponytail: 현재 월 릴리스는 약 100MB. 1GB를 넘으면 manifest 다중 part로 전환합니다.
    temporary = path.parent / f".{path.name}.spark"
    frame.coalesce(1).write.mode("overwrite").parquet(str(temporary))
    parts = list(temporary.glob("part-*.parquet"))
    if len(parts) != 1:
        raise ValueError(f"단일 Parquet 파일을 만들지 못했습니다: {temporary}")
    parts[0].rename(path)
    shutil.rmtree(temporary)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_source_release(
    trips: DataFrame,
    leases: DataFrame,
    inventory: DataFrame,
    *,
    output_dir: str | Path,
    year_month: str,
    seed: int,
) -> Path:
    """세 데이터셋과 manifest를 staging에 쓴 뒤 디렉터리 rename으로 공개합니다."""
    final = Path(output_dir) / f"year_month={year_month}"
    if _existing_release(final, year_month, seed):
        return final

    _validate_temporal_links(trips, leases)
    output_root = Path(output_dir)
    output_root.mkdir(parents=True, exist_ok=True)
    staging = output_root / f".year_month={year_month}.staging-{uuid.uuid4().hex}"
    try:
        staging.mkdir()
        trip_file = staging / "hvfhv_taxi_trips.parquet"
        lease_file = staging / "driver_vehicle_leases.parquet"
        inventory_file = staging / "lease_vehicle_inventory.parquet"
        _write_one_parquet(trips, trip_file)
        _write_one_parquet(leases, lease_file)
        _write_one_parquet(inventory, inventory_file)
        manifest = {
            "release_id": f"{year_month}-seed-{seed}",
            "year_month": year_month,
            "seed": seed,
            "datasets": {
                "hvfhv_taxi_trips": {
                    "file": trip_file.name,
                    "row_count": pq.ParquetFile(trip_file).metadata.num_rows,
                    "sha256": _sha256(trip_file),
                },
                "driver_vehicle_leases": {
                    "file": lease_file.name,
                    "row_count": pq.ParquetFile(lease_file).metadata.num_rows,
                    "sha256": _sha256(lease_file),
                },
                "lease_vehicle_inventory": {
                    "file": inventory_file.name,
                    "row_count": pq.ParquetFile(inventory_file).metadata.num_rows,
                    "sha256": _sha256(inventory_file),
                },
            },
        }
        (staging / "manifest.json").write_text(
            json.dumps(manifest, ensure_ascii=False, sort_keys=True),
            encoding="utf-8",
        )
        staging.rename(final)
        return final
    finally:
        shutil.rmtree(staging, ignore_errors=True)


def main(args_list: list[str] | None = None) -> Path:
    parser = argparse.ArgumentParser(description="월별 가짜 기사-운행 원천 릴리스 생성")
    parser.add_argument("--hvfhv_input_path", required=True)
    parser.add_argument("--zone_lookup_path", required=True)
    parser.add_argument("--previous_preferences_path", default=None)
    parser.add_argument("--previous_snapshot_dir", required=True)
    parser.add_argument("--vehicle_master_path", required=True)
    parser.add_argument("--state_output_dir", required=True)
    parser.add_argument("--release_output_dir", required=True)
    parser.add_argument("--year_month", required=True)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--change_rate", type=float, default=None)
    parser.add_argument("--bucket_size", type=int, default=5)
    parser.add_argument("--spark_memory", default="4g")
    parser.add_argument("--test_row_limit", type=int, default=0)
    args = parser.parse_args(args_list)

    snapshot_date = date.fromisoformat(f"{args.year_month}-01")
    state_output_dir = _test_scoped_root(
        args.state_output_dir, args.test_row_limit
    )
    release_output_dir = _test_scoped_root(
        args.release_output_dir, args.test_row_limit
    )
    if args.test_row_limit:
        print(
            "TEMPORARY smoke test: "
            f"test_row_limit={args.test_row_limit}, output={release_output_dir}"
        )
    state = prepare_monthly_state(
        previous_snapshot_dir=args.previous_snapshot_dir,
        previous_preferences_path=args.previous_preferences_path,
        hvfhv_input_dir=Path(args.hvfhv_input_path).parent.parent,
        output_dir=state_output_dir,
        snapshot_date=snapshot_date,
        seed=args.seed,
        change_rate=args.change_rate,
    )

    spark = get_or_create_spark_session(
        "synthetic_driver_trip_source", driver_memory=args.spark_memory
    )
    spark.conf.set(
        "spark.sql.files.maxPartitionBytes",
        str(128 * 1024 * 1024 // max(1, args.bucket_size)),
    )
    read = spark.read.parquet
    raw_trips = _apply_test_row_limit(
        read(args.hvfhv_input_path), args.test_row_limit
    )
    zones = spark.read.option("header", "true").csv(args.zone_lookup_path)
    trips = HVFHVCleanTransformer(
        df_zone=zones,
        error_threshold=0.2,
    ).transform(raw_trips).persist(StorageLevel.DISK_ONLY)
    preferences = read(str(state.preferences_path))
    customers = read(str(state.snapshot_dir / "customer.parquet"))
    leases = read(str(state.snapshot_dir / "lease_contract.parquet"))
    taxis = read(str(state.snapshot_dir / "taxi.parquet"))
    vehicle_master = read(args.vehicle_master_path)
    candidates = build_trip_candidates(
        trips,
        preferences,
        customers,
        leases,
        taxis,
        seed=args.seed,
        bucket_size=args.bucket_size,
    ).persist(StorageLevel.DISK_ONLY)
    assignments = allocate_trips(
        candidates, build_travel_times(trips)
    ).persist(StorageLevel.MEMORY_AND_DISK)
    assignment_count = assignments.count()
    candidates.unpersist(blocking=True)
    trip_source = build_trip_source(
        raw_trips,
        assignments,
    ).persist(StorageLevel.DISK_ONLY)
    if trip_source.count() != assignment_count:
        raise ValueError("배정 결과와 HVFHV 원천 행이 일대일로 연결되지 않습니다")
    lease_source = build_driver_vehicle_leases(
        customers, leases, taxis, snapshot_date=snapshot_date
    ).persist(StorageLevel.DISK_ONLY)
    inventory_source = build_lease_vehicle_inventory(
        taxis, vehicle_master, snapshot_date=snapshot_date
    ).persist(StorageLevel.DISK_ONLY)
    try:
        return write_source_release(
            trip_source,
            lease_source,
            inventory_source,
            output_dir=release_output_dir,
            year_month=args.year_month,
            seed=args.seed,
        )
    finally:
        for frame in (
            inventory_source,
            lease_source,
            trip_source,
            assignments,
            candidates,
            trips,
        ):
            frame.unpersist()


if __name__ == "__main__":
    main()
