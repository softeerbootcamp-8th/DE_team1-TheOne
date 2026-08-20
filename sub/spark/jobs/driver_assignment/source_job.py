"""월별 기사 배정 결과를 운행·리스·보유 차량 데이터로 분리합니다."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import uuid
from dataclasses import replace
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
    datediff,
    floor,
    hash as spark_hash,
    lit,
    max as spark_max,
    min as spark_min,
    monotonically_increasing_id,
    pmod,
    row_number,
    sha2,
    struct,
    to_date,
    to_json,
    to_timestamp,
    when,
)

from schema.source import (
    DRIVER_VEHICLE_MONTHLY_SNAPSHOT_SCHEMA,
    LEASE_VEHICLE_INVENTORY_SCHEMA,
    MONTHLY_TAXI_TRIP_SCHEMA,
)
from shared.spark.common.session import get_or_create_spark_session
from shared.spark.hvfhv_clean_transformer import (
    TRIP_KEY_COLUMNS,
    HVFHVCleanTransformer,
)
from sub.config import load_config
from sub.generators.synthetic_driver_trip_source.monthly import prepare_monthly_state
from sub.run_context import RunContext
from sub.spark.jobs.driver_assignment.allocator import allocate_trips
from sub.spark.jobs.driver_assignment.candidates import build_trip_candidates
from sub.spark.jobs.travel_times.transformer import build_travel_times

SNAPSHOT_SOURCE_COLUMNS = DRIVER_VEHICLE_MONTHLY_SNAPSHOT_SCHEMA.names
TRIP_SOURCE_COLUMNS = MONTHLY_TAXI_TRIP_SCHEMA.names

# 입사 전 경력의 상한(년). `experience_years` 는 회사 근속에 이 값을 더해 만듭니다 —
# 독립 난수로 두면 "근속 5년인데 경력 1년" 같은 모순이 생깁니다.
PRIOR_EXPERIENCE_MAX_YEARS = 10

# platform_name -> 원본 라이선스 번호. 정제 단계에서 사람이 읽는 이름으로 바꾼 것을
# 되돌립니다 (`shared/spark/hvfhv_clean_transformer.py`).
PLATFORM_LICENSE = {"Uber": "HV0003", "Lyft": "HV0005"}

# pyarrow 타입 -> Spark cast 문자열. 산출물이 `schema/source` 와 **정확히** 같은 타입이
# 되도록 여기서 한 번에 옮깁니다. 손으로 `.cast("int")` 를 흩어 놓으면 스키마가 바뀔 때
# 따라가지 못합니다.
_SPARK_TYPES = {
    "string": "string",
    "bool": "boolean",
    "double": "double",
    "int32": "int",
    "int64": "bigint",
    "int16": "smallint",
    "date32[day]": "date",
    "timestamp[us]": "timestamp",
}


def _as_schema(frame: DataFrame, schema) -> DataFrame:
    """`schema` 의 컬럼만 그 순서·타입으로 고릅니다."""
    return frame.select(
        *(
            col(field.name).cast(_SPARK_TYPES[str(field.type)]).alias(field.name)
            for field in schema
        )
    )
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


def build_trip_source(
    raw_trips: DataFrame, clean_trips: DataFrame, assignments: DataFrame
) -> DataFrame:
    """배정된 운행을 `MONTHLY_TAXI_TRIP_SCHEMA` 14컬럼으로 좁힙니다.

    예전에는 TLC 원본 26컬럼을 그대로 내보냈습니다 — 공개 계약이 원본 형태에
    묶여 있었고, 원본이 컬럼을 추가하면 하류가 조용히 따라 늘어났습니다.
    """
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

    selected = assignments.select(
        "trip_key", col("taxi_id").alias("_assigned_taxi_id")
    )
    # 원본에만 있는 것(on_scene_datetime)과 정제본에만 있는 것(zone·등급·platform_name)이
    # 갈려 있어 둘 다 붙입니다. FINAL_SCHEMA 에 on_scene_datetime 이 없습니다.
    clean = clean_trips.select(
        "trip_key",
        col("pickup_zone"),
        col("dropoff_zone"),
        col("estimated_service_tier"),
        col("platform_name"),
    )
    license_number = None
    for name, number in PLATFORM_LICENSE.items():
        branch = when if license_number is None else license_number.when
        license_number = branch(col("platform_name") == lit(name), lit(number))

    source = (
        add_trip_keys(raw_trips)
        .join(selected, "trip_key", "inner")
        .join(clean, "trip_key", "inner")
        .withColumn("taxi_id", col("_assigned_taxi_id"))
        .withColumn("hvfhs_license_num", license_number)
    )
    typed = _as_schema(source, MONTHLY_TAXI_TRIP_SCHEMA)
    _require_non_null(
        typed,
        set(MONTHLY_TAXI_TRIP_SCHEMA.names) - {"on_scene_datetime"},
        "운행 기록",
    )
    return typed


def _vehicle_model_id(manufacturer, model_name, model_year):
    """차종 식별자.

    `lease_vehicle_inventory` 와 `driver_vehicle_monthly_snapshot` 이 **같은 규칙**으로
    만들어야 두 데이터셋이 조인됩니다. 한쪽만 바꾸면 조인이 전부 빗나가는데 행 수는
    그대로라 조용히 틀립니다.
    """
    return sha2(
        concat_ws(":", manufacturer, model_name, model_year.cast("string")), 256
    )


def build_driver_vehicle_monthly_snapshot(
    customers: DataFrame,
    leases: DataFrame,
    taxis: DataFrame,
    vehicle_master: DataFrame,
    *,
    snapshot_date: date,
    year_month: str,
    seed: int,
) -> DataFrame:
    """(기사, 대상 월) 한 행짜리 월별 스냅샷을 만듭니다.

    리스 계약 단위가 아니라 **기사 단위**입니다. 기사당 계약이 여러 건일 수 있으므로
    (`evolve_company_snapshot` 이 계약을 종료시키고 새로 맺습니다) 1:1 을 가정하지 않고
    윈도우로 고릅니다.

        join_date     그 기사의 최초 계약 시작일
        vehicle_since 현재 계약의 시작일
        exit_date     진행 중 계약이 하나도 없을 때만 마지막 종료일, 아니면 NULL
    """
    c = customers.filter(col("snapshot_date") == lit(snapshot_date)).alias("c")
    l = leases.filter(col("snapshot_date") == lit(snapshot_date)).alias("l")
    x = taxis.filter(col("snapshot_date") == lit(snapshot_date)).alias("x")

    joined = l.join(c, col("l.customer_id") == col("c.customer_id"), "inner").select(
        col("c.synthetic_driver_id").alias("driver_id"),
        col("l.taxi_id").alias("taxi_id"),
        col("l.lease_started_on").alias("lease_started_on"),
        col("l.lease_ended_on").alias("lease_ended_on"),
    )

    by_driver = Window.partitionBy("driver_id")
    # 진행 중 계약을 먼저, 그다음 시작일이 늦은 순. 첫 행이 "현재 차량"입니다.
    current = Window.partitionBy("driver_id").orderBy(
        col("lease_ended_on").isNotNull().asc(), col("lease_started_on").desc()
    )
    ranked = (
        joined.withColumn("_join_date", spark_min("lease_started_on").over(by_driver))
        .withColumn("_open", count(when(col("lease_ended_on").isNull(), 1)).over(by_driver))
        .withColumn("_last_end", spark_max("lease_ended_on").over(by_driver))
        .withColumn("_rank", row_number().over(current))
        .filter(col("_rank") == 1)
    )

    fleet = x.select(
        col("x.taxi_id").alias("_taxi_id"),
        col("x.make_key").alias("manufacturer"),
        col("x.model_key").alias("model_name"),
        col("x.model_year").alias("_model_year"),
        col("x.weekly_lease_fee").alias("weekly_lease_fee"),
        col("x.uber_comfort_eligible").alias("comfort_eligible"),
        col("x.lyft_extra_comfort_eligible").alias("extra_comfort_eligible"),
    )
    fuel = vehicle_master.select(
        col("make_key").alias("_mk"), col("model_key").alias("_mo"), col("fuel_type")
    ).distinct()

    snapshot = (
        ranked.join(fleet, col("taxi_id") == col("_taxi_id"), "inner")
        .join(
            fuel,
            (col("manufacturer") == col("_mk")) & (col("model_name") == col("_mo")),
            "left",
        )
        .withColumn("snapshot_month", lit(year_month))
        .withColumn(
            "vehicle_model_id",
            _vehicle_model_id(col("manufacturer"), col("model_name"), col("_model_year")),
        )
        .withColumn("join_date", col("_join_date"))
        .withColumn(
            "exit_date", when(col("_open") == 0, col("_last_end")).otherwise(lit(None))
        )
        .withColumn("vehicle_since", col("lease_started_on"))
        .withColumn(
            "experience_years",
            (
                floor(datediff(lit(snapshot_date), col("_join_date")) / lit(365.25))
                + pmod(
                    spark_hash(col("driver_id")) + lit(seed),
                    lit(PRIOR_EXPERIENCE_MAX_YEARS + 1),
                )
            ).cast("int"),
        )
        .withColumn("snapshot_created_at", to_timestamp(lit(snapshot_date)))
    )

    expected = joined.select("driver_id").distinct().count()
    stats = snapshot.agg(
        count(lit(1)).alias("rows"),
        countDistinct("driver_id").alias("distinct_drivers"),
    ).first()
    if not stats or stats["rows"] != expected or stats["distinct_drivers"] != expected:
        raise ValueError("기사 스냅샷은 기사당 정확히 한 행이어야 합니다")

    typed = _as_schema(snapshot, DRIVER_VEHICLE_MONTHLY_SNAPSHOT_SCHEMA)
    _require_non_null(
        typed,
        set(DRIVER_VEHICLE_MONTHLY_SNAPSHOT_SCHEMA.names) - {"exit_date"},
        "기사 스냅샷",
    )
    return typed


def _require_non_null(frame: DataFrame, columns: set[str], label: str) -> None:
    """계약상 비면 안 되는 컬럼을 확인합니다.

    상류 컬럼명이 바뀌면 조인이 통째로 빗나가는데 행 수는 그대로라, 값을 안 보면
    조용히 NULL 만 남습니다.
    """
    condition = None
    for name in sorted(columns):
        missing = col(name).isNull()
        condition = missing if condition is None else (condition | missing)
    if condition is not None and frame.filter(condition).limit(1).count():
        raise ValueError(f"{label}에 비면 안 되는 컬럼이 비었습니다: {sorted(columns)}")


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
            "weekly_lease_fee",
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
            col("weekly_lease_fee").cast("double").alias("weekly_lease_fee"),
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
        | (col("weekly_lease_fee") <= 0)
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


def _validate_temporal_links(trips: DataFrame, snapshots: DataFrame) -> None:
    """모든 운행이 그 시점에 그 차를 몰던 기사 한 명과 연결되는지 봅니다.

    예전에는 리스 계약(`lease_started_on`/`lease_ended_on`)으로 봤습니다. 공개 계약이
    월별 기사 스냅샷으로 바뀌면서 같은 사실을 `vehicle_since`/`exit_date` 로 봅니다.
    한 건도 못 붙거나 두 건에 붙으면 하류 조인이 조용히 행을 잃거나 늘립니다.
    """
    rows = trips.withColumn("_source_row_id", monotonically_increasing_id()).alias("t")
    matched = rows.join(
        snapshots.alias("s"),
        (col("t.taxi_id") == col("s.taxi_id"))
        & (col("s.vehicle_since") <= to_date(col("t.pickup_datetime")))
        & (
            col("s.exit_date").isNull()
            | (to_date(col("t.pickup_datetime")) < col("s.exit_date"))
        ),
        "left",
    )
    invalid = (
        matched.groupBy("_source_row_id")
        .agg(count("s.driver_id").alias("matches"))
        .filter(col("matches") != 1)
        .limit(1)
        .count()
    )
    if invalid:
        raise ValueError("모든 HVFHV 행은 운행 시점의 기사 스냅샷 한 건과 연결돼야 합니다")


def _existing_release(path: Path, run: RunContext) -> bool:
    manifest_path = path / "manifest.json"
    if not path.exists():
        return False
    if not manifest_path.is_file():
        raise ValueError(f"완료되지 않은 릴리스 경로가 남아 있습니다: {path}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    # `seed` 가 아니라 `run_id` 로 판정합니다. seed 만 보면 설정을 바꿔도 낡은
    # 릴리스를 그대로 재사용해서 "설정을 바꿨는데 결과가 안 바뀐다" 가 됩니다.
    if "run_id" not in manifest:
        raise ValueError(
            f"설정 통합 이전에 만든 릴리스입니다 (manifest 에 run_id 가 없습니다): {manifest_path}\n"
            f"이 릴리스는 어느 설정으로 만들었는지 확인할 수 없어 재사용할 수 없습니다. "
            f"해당 파티션을 지우고 다시 발행하세요: rm -rf {path}"
        )
    if manifest.get("year_month") != run.target_month or manifest.get("run_id") != run.run_id:
        raise ValueError(
            f"기존 릴리스 계보가 요청과 다릅니다: "
            f"기존={{'year_month': {manifest.get('year_month')!r}, 'run_id': {manifest.get('run_id')!r}}}, "
            f"요청={{'year_month': {run.target_month!r}, 'run_id': {run.run_id!r}}}. "
            f"설정을 바꿔 다시 발행하려면 {path} 를 지우고 실행하세요."
        )
    for name in (
        "hvfhv_taxi_trips",
        "driver_vehicle_monthly_snapshot",
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
    snapshots: DataFrame,
    inventory: DataFrame,
    *,
    output_dir: str | Path,
    run: RunContext,
) -> Path:
    """세 데이터셋과 manifest를 staging에 쓴 뒤 디렉터리 rename으로 공개합니다."""
    year_month = run.target_month
    final = Path(output_dir) / f"year_month={year_month}"
    if _existing_release(final, run):
        return final

    _validate_temporal_links(trips, snapshots)
    output_root = Path(output_dir)
    output_root.mkdir(parents=True, exist_ok=True)
    staging = output_root / f".year_month={year_month}.staging-{uuid.uuid4().hex}"
    try:
        staging.mkdir()
        trip_file = staging / "hvfhv_taxi_trips.parquet"
        snapshot_file = staging / "driver_vehicle_monthly_snapshot.parquet"
        inventory_file = staging / "lease_vehicle_inventory.parquet"
        _write_one_parquet(trips, trip_file)
        _write_one_parquet(snapshots, snapshot_file)
        _write_one_parquet(inventory, inventory_file)
        manifest = {
            "release_id": f"{year_month}-seed-{run.config.global_seed}",
            "year_month": year_month,
            "seed": run.config.global_seed,
            "run_id": run.run_id,
            "config_hash": run.config_hash,
            "created_at": run.created_at,
            "datasets": {
                "hvfhv_taxi_trips": {
                    "file": trip_file.name,
                    "row_count": pq.ParquetFile(trip_file).metadata.num_rows,
                    "sha256": _sha256(trip_file),
                },
                "driver_vehicle_monthly_snapshot": {
                    "file": snapshot_file.name,
                    "row_count": pq.ParquetFile(snapshot_file).metadata.num_rows,
                    "sha256": _sha256(snapshot_file),
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
    parser.add_argument("--bucket_size", type=int, default=5)
    parser.add_argument("--spark_memory", default="4g")
    parser.add_argument("--test_row_limit", type=int, default=0)
    args = parser.parse_args(args_list)

    # lifecycle(join/exit/vehicle_change) 비율은 이제 `--change_rate` 가 아니라
    # config의 driver.{join,exit,vehicle_change}_rate 가 소유합니다 (#605/#628).
    config = replace(load_config(), global_seed=args.seed)
    run = RunContext.create(args.year_month, config)

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
        config=config,
        vehicle_master_path=args.vehicle_master_path,
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
        score_weights=config.allocation.score_weights,
    ).persist(StorageLevel.DISK_ONLY)
    assignments = allocate_trips(
        candidates, build_travel_times(trips)
    ).persist(StorageLevel.MEMORY_AND_DISK)
    assignment_count = assignments.count()
    candidates.unpersist(blocking=True)
    trip_source = build_trip_source(
        raw_trips,
        trips,
        assignments,
    ).persist(StorageLevel.DISK_ONLY)
    if trip_source.count() != assignment_count:
        raise ValueError("배정 결과와 HVFHV 원천 행이 일대일로 연결되지 않습니다")
    snapshot_source = build_driver_vehicle_monthly_snapshot(
        customers,
        leases,
        taxis,
        vehicle_master,
        snapshot_date=snapshot_date,
        year_month=args.year_month,
        seed=args.seed,
    ).persist(StorageLevel.DISK_ONLY)
    inventory_source = build_lease_vehicle_inventory(
        taxis, vehicle_master, snapshot_date=snapshot_date
    ).persist(StorageLevel.DISK_ONLY)
    try:
        return write_source_release(
            trip_source,
            snapshot_source,
            inventory_source,
            output_dir=release_output_dir,
            run=run,
        )
    finally:
        for frame in (
            inventory_source,
            snapshot_source,
            trip_source,
            assignments,
            candidates,
            trips,
        ):
            frame.unpersist()


if __name__ == "__main__":
    main()
