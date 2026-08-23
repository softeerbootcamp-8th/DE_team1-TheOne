"""월별 기사 배정 결과를 운행·리스·보유 차량 데이터로 분리합니다."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import uuid
from collections import Counter
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
    monotonically_increasing_id,
    pmod,
    row_number,
    sha2,
    struct,
    sum as spark_sum,
    to_date,
    to_json,
    to_timestamp,
    unix_timestamp,
    when,
)

from schema.source import (
    DRIVER_VEHICLE_MONTHLY_SNAPSHOT_SCHEMA,
    LEASE_VEHICLE_INVENTORY_SCHEMA,
    MONTHLY_TAXI_TRIP_SCHEMA,
)
from shared.common.s3_reader import is_s3_uri, parent_uri
from shared.spark.common.session import get_or_create_spark_session
from shared.spark.hvfhv_clean_transformer import (
    TRIP_KEY_COLUMNS,
    HVFHVCleanTransformer,
)
from sub.config import GenerationConfig, load_config
from sub.generators.synthetic_driver_trip_source.monthly import prepare_monthly_state
from sub.run_context import RunContext
from sub.spark.jobs.driver_assignment.allocator import allocate_trips
from sub.spark.jobs.driver_assignment.candidates import build_trip_candidates
from sub.spark.jobs.travel_times.transformer import build_travel_times

SNAPSHOT_SOURCE_COLUMNS = DRIVER_VEHICLE_MONTHLY_SNAPSHOT_SCHEMA.names
TRIP_SOURCE_COLUMNS = MONTHLY_TAXI_TRIP_SCHEMA.names

# manifest 계약 버전. `run_id`/`config_hash`는 "어느 설정으로" 를 답하는데, 이 값은
# "manifest·산출물 구조 자체가 바뀌었는가" 를 답합니다 — 설정을 안 바꿔도 계약이
# 바뀌면 낡은 릴리스를 재사용하면 안 되므로 별도 필드로 둡니다(#608).
SCHEMA_VERSION = "1"
# 데이터셋별 실측 대 합성 비중. monthly_taxi_trip은 실측 TLC 운행에 합성 신원만
# 얹은 것이고, driver_vehicle_monthly_snapshot은 기사·차량 자체가 합성이며,
# lease_vehicle_inventory는 실측 렌탈 카탈로그에 보유 대수(stock)만 가정값입니다 —
# 소비자가 "이 숫자가 실측인가" 를 API 응답만 보고 오판하지 않도록 명시합니다.
PROVENANCE = {
    "monthly_taxi_trip": "real_facts+synthetic_identity",
    "driver_vehicle_monthly_snapshot": "synthetic",
    "lease_vehicle_inventory": "real_catalog+assumed_stock",
}

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
    current_driver_vehicle: DataFrame,
    vehicle_master: DataFrame,
    *,
    snapshot_date: date,
    year_month: str,
    seed: int,
) -> DataFrame:
    """(기사, 대상 월) 한 행짜리 월별 스냅샷을 만듭니다.

    `current_driver_vehicle` 은 기사당 정확히 한 행(이벤트소싱 `driver_vehicle_current`
    뷰, #609)이라 리스 이력에서 최초/최근을 윈도우로 골라낼 필요가 없습니다 —
    `joined_on`이 이미 그 기사의 최초 입사일입니다.

    예전에는 customer/taxi/lease_contract 3-테이블에서 기사당 리스 이력을
    한 행으로 재구성했는데, 그 재구성이 매달 리스 1건만 만들어서 `join_date`가
    항상 "현재 차량 배정일"로 퇴화하는 조용한 버그가 있었습니다(#609) —
    `joined_on`을 그대로 흘려보내 그 재구성 자체를 없앴습니다.

        join_date     joined_on(최초 입사일)
        vehicle_since lease_started_on(현재 차량 배정일)
        exit_date     lease_ended_on(퇴사했으면 그 시각, 아니면 NULL)
    """
    fleet = current_driver_vehicle.select(
        col("driver_id"),
        col("taxi_id"),
        col("joined_on"),
        col("lease_started_on"),
        col("lease_ended_on"),
        col("make_key").alias("manufacturer"),
        col("model_key").alias("model_name"),
        col("model_year").alias("_model_year"),
        col("weekly_lease_fee"),
        col("uber_comfort_eligible").alias("comfort_eligible"),
        col("lyft_extra_comfort_eligible").alias("extra_comfort_eligible"),
    )
    fuel = vehicle_master.select(
        col("make_key").alias("_mk"), col("model_key").alias("_mo"), col("fuel_type")
    ).distinct()

    snapshot = (
        fleet.join(
            fuel,
            (col("manufacturer") == col("_mk")) & (col("model_name") == col("_mo")),
            "left",
        )
        .withColumn("snapshot_month", lit(year_month))
        .withColumn(
            "vehicle_model_id",
            _vehicle_model_id(col("manufacturer"), col("model_name"), col("_model_year")),
        )
        .withColumn("join_date", col("joined_on"))
        .withColumn("exit_date", col("lease_ended_on"))
        .withColumn("vehicle_since", col("lease_started_on"))
        .withColumn(
            "experience_years",
            (
                floor(datediff(lit(snapshot_date), col("joined_on")) / lit(365.25))
                + pmod(
                    spark_hash(col("driver_id")) + lit(seed),
                    lit(PRIOR_EXPERIENCE_MAX_YEARS + 1),
                )
            ).cast("int"),
        )
        .withColumn("snapshot_created_at", to_timestamp(lit(snapshot_date)))
    )

    expected = current_driver_vehicle.select("driver_id").distinct().count()
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
    current_driver_vehicle: DataFrame,
    vehicle_master: DataFrame,
) -> DataFrame:
    """보유 차량을 차종·연식별 API 재고로 집계합니다.

    `taxi_id` 로 먼저 dedup 합니다 — 이번 달 안에 기사가 바뀐 차량(퇴사 기사의
    차량이 신규 기사에게 재배정된 경우)이 `current_driver_vehicle`에 두 행으로
    남아 있으면 그 차량이 재고에 두 번 집계됩니다.
    """
    fleet = (
        current_driver_vehicle.dropDuplicates(["taxi_id"]).groupBy(
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


def _existing_release(path: Path, run: RunContext, *, input_scope: str) -> bool:
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
    # run_id 가 같아도 manifest·산출물 계약(schema_version) 이나 입력 범위
    # (input_scope, 예: 표본 vs 전체 달) 가 다르면 재사용하지 않습니다(#608) —
    # 설정은 안 바뀌었는데 계약만 바뀐 낡은 릴리스를 그대로 쓰게 되는 사고를 막습니다.
    if manifest.get("schema_version") != SCHEMA_VERSION or manifest.get("input_scope") != input_scope:
        raise ValueError(
            f"기존 릴리스의 계약 버전·입력 범위가 다릅니다: "
            f"기존={{'schema_version': {manifest.get('schema_version')!r}, "
            f"'input_scope': {manifest.get('input_scope')!r}}}, "
            f"요청={{'schema_version': {SCHEMA_VERSION!r}, 'input_scope': {input_scope!r}}}. "
            f"다시 발행하려면 {path} 를 지우고 실행하세요."
        )
    for name in (
        "monthly_taxi_trip",
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


def _write_one_parquet_s3(frame: DataFrame, *, bucket: str, key: str) -> None:
    """`_write_one_parquet()`의 S3 대응. Hadoop S3A의 `rename()`으로 공개합니다.

    S3에는 디렉터리 rename이 없어 로컬의 `Path.rename()` 트릭이 안 통합니다.
    이미 SparkSession이 있으므로 boto3 대신 Spark가 쓰는 Hadoop FileSystem의
    `rename()`(내부적으로 copy+delete)을 그대로 씁니다 — Spark/Hadoop 세계 밖으로
    안 나가는 유일한 방법입니다.

    목적지를 먼저 지우는 이유 (#791)
    ------------------------------
    Hadoop `FileSystem.rename()` 에는 overwrite 옵션이 없어 목적지가 있으면
    `FileAlreadyExistsException` 을 던집니다. POSIX `Path.rename()` 과 다릅니다.
    발행 도중 죽으면 데이터셋 일부만 남고 manifest 는 없는 상태가 되는데,
    manifest 가 없으니 다음 실행은 재생성을 시도하고 그 남은 객체에 막힙니다.
    사람이 S3 를 손으로 지우지 않으면 그 월은 영구히 발행 불가였습니다.

    목적지 삭제와 rename 사이에 객체가 없는 짧은 구간이 생깁니다. `source_api` 는
    manifest 없이 Parquet 을 직접 읽으므로(#547) 그 순간 404 가 날 수 있습니다.
    재발행은 월 1회 배치라 감수합니다 — 없애려면 Hadoop rename 대신 boto3
    `copy_object` 로 제자리 덮어써야 하고, 그건 별도 판단 사항입니다.
    """
    staging_uri = f"s3a://{bucket}/.staging/{uuid.uuid4().hex}/"
    final_uri = f"s3a://{bucket}/{key}"
    frame.coalesce(1).write.mode("overwrite").parquet(staging_uri)

    jvm = frame.sparkSession._jvm
    hadoop_path = jvm.org.apache.hadoop.fs.Path
    staging_path = hadoop_path(staging_uri)
    fs = staging_path.getFileSystem(frame.sparkSession._jsc.hadoopConfiguration())

    # rename 이 던져도 staging 을 지웁니다. 안 지우면 실패한 실행마다 `.staging/` 에
    # 잔여물이 쌓이고, 그건 누구도 다시 보지 않는 데이터입니다.
    try:
        parts = [
            status.getPath()
            for status in fs.listStatus(staging_path)
            if status.getPath().getName().startswith("part-")
        ]
        if len(parts) != 1:
            raise ValueError(f"단일 Parquet 파일을 만들지 못했습니다: {staging_uri}")
        # 없으면 false 를 돌려줄 뿐 예외를 던지지 않습니다 (Hadoop FS 계약).
        fs.delete(hadoop_path(final_uri), False)
        fs.rename(parts[0], hadoop_path(final_uri))
    finally:
        fs.delete(staging_path, True)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_attribution(assignments: DataFrame, *, output_dir: str | Path, year_month: str) -> Path:
    """배정 결과(`assignments`)를 그대로 영속화합니다.

    PUBLISHED(`build_trip_source`)는 여기서 taxi_id만 가져다 쓰고 driver_id는
    빼므로, "누가 배정됐는가"의 원자료는 이것뿐입니다 — 전에는 어디에도 저장되지
    않고 메모리에서만 존재하다 사라졌습니다.
    """
    final_dir = Path(output_dir) / f"year_month={year_month}"
    final_dir.mkdir(parents=True, exist_ok=True)
    _write_one_parquet(assignments, final_dir / "attribution.parquet")
    return final_dir


def write_attribution_s3(assignments: DataFrame, *, bucket: str, year_month: str) -> str:
    """`write_attribution()`의 S3 대응. `source/attribution/year_month=.../attribution.parquet`."""
    key = f"source/attribution/year_month={year_month}/attribution.parquet"
    _write_one_parquet_s3(assignments, bucket=bucket, key=key)
    return f"s3://{bucket}/{key}"


def write_source_release(
    trips: DataFrame,
    snapshots: DataFrame,
    inventory: DataFrame,
    *,
    output_dir: str | Path,
    run: RunContext,
    input_scope: str,
) -> Path:
    """세 데이터셋과 manifest를 staging에 쓴 뒤 디렉터리 rename으로 공개합니다."""
    year_month = run.target_month
    final = Path(output_dir) / f"year_month={year_month}"
    if _existing_release(final, run, input_scope=input_scope):
        return final

    _validate_temporal_links(trips, snapshots)
    output_root = Path(output_dir)
    output_root.mkdir(parents=True, exist_ok=True)
    staging = output_root / f".year_month={year_month}.staging-{uuid.uuid4().hex}"
    try:
        staging.mkdir()
        trip_file = staging / "monthly_taxi_trip.parquet"
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
            "schema_version": SCHEMA_VERSION,
            "input_scope": input_scope,
            "provenance": PROVENANCE,
            "datasets": {
                "monthly_taxi_trip": {
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


def write_source_release_s3(
    trips: DataFrame,
    snapshots: DataFrame,
    inventory: DataFrame,
    *,
    bucket: str,
    run: RunContext,
    input_scope: str,
) -> str:
    """`write_source_release()`의 S3 대응 — PUBLISHED 3종을 `source/published/`에 공개합니다.

    로컬의 "staging 디렉터리 + rename" 같은 디렉터리 단위 원자성이 S3에는 없습니다.
    대신 데이터셋마다 `_write_one_parquet_s3()`가 개별 원자성(rename)을 갖고,
    manifest는 셋을 다 쓴 뒤 마지막에 씁니다.

    ★ `source_api`의 S3 서빙(`S3DatasetStorage`)은 이미 manifest 없이 Parquet을
      직접 읽도록 단순화돼 있습니다(#547) — 이 manifest는 읽기 게이트가 아니라
      계보 기록용입니다. 그래서 로컬 manifest와 달리 row_count만 남기고
      sha256은 뺐습니다.
    # ponytail: checksum 없이 row_count만 기록. 계보 대조가 필요해지면 각
    # `_write_one_parquet_s3()` 호출 뒤 해당 키를 다시 읽어 sha256을 붙이세요.
    """
    import boto3

    _validate_temporal_links(trips, snapshots)
    year_month = run.target_month
    prefix = "source/published"
    datasets = {
        "monthly_taxi_trip": trips,
        "driver_vehicle_monthly_snapshot": snapshots,
        "lease_vehicle_inventory": inventory,
    }
    manifest_datasets = {}
    for name, frame in datasets.items():
        key = f"{prefix}/{name}/year_month={year_month}/data.parquet"
        _write_one_parquet_s3(frame, bucket=bucket, key=key)
        manifest_datasets[name] = {"key": key, "row_count": frame.count()}

    manifest = {
        "release_id": f"{year_month}-seed-{run.config.global_seed}",
        "year_month": year_month,
        "seed": run.config.global_seed,
        "run_id": run.run_id,
        "config_hash": run.config_hash,
        "created_at": run.created_at,
        "schema_version": SCHEMA_VERSION,
        "input_scope": input_scope,
        "provenance": PROVENANCE,
        "datasets": manifest_datasets,
    }
    manifest_key = f"{prefix}/_manifests/year_month={year_month}.json"
    boto3.client("s3").put_object(
        Bucket=bucket,
        Key=manifest_key,
        Body=json.dumps(manifest, ensure_ascii=False, sort_keys=True).encode("utf-8"),
        ServerSideEncryption="AES256",
    )
    return f"s3://{bucket}/{prefix}"


def _capacity_drive_minutes(preferences: DataFrame, service_dates: list[date]) -> int:
    """기사 정원의 운행분 예산 합계(`sub/prototype/metrics.py::capacity_ceiling` 축소판).

    요일 선호(`weekday_mask`)가 있어 그 달의 요일 분포에 따라 값이 달라집니다.
    """
    weekday_counts = Counter(d.weekday() for d in service_dates)
    prefs = preferences.select("target_drive_minutes", "weekday_mask").toPandas()
    return int(
        sum(
            int(row.target_drive_minutes)
            * sum(
                days
                for weekday, days in weekday_counts.items()
                if int(row.weekday_mask) & (1 << weekday)
            )
            for row in prefs.itertuples()
        )
    )


def _quality_report(
    *,
    run: RunContext,
    trips: DataFrame,
    preferences: DataFrame,
    assignments: DataFrame,
    assignment_count: int,
    rejected: dict[str, int],
    clip_rate: float,
) -> dict:
    """coverage/ceiling/saturation/탈락 사유/클리핑. 릴리스 계보가 아니라 진단용이라
    manifest 와 분리된 quality_report.json 에 씁니다(#608)."""
    trips_offered = trips.count()
    service_dates = [
        row[0] for row in trips.select(to_date("pickup_datetime").alias("d")).distinct().collect()
    ]
    capacity_drive_minutes = _capacity_drive_minutes(preferences, service_dates)
    budget_minutes = max(1, capacity_drive_minutes)
    drive_minutes = float(
        assignments.select(
            spark_sum(
                (unix_timestamp("dropoff_datetime") - unix_timestamp("pickup_datetime")) / 60.0
                + col("deadhead_minutes")
            ).alias("drive_minutes")
        ).first()["drive_minutes"]
        or 0.0
    )
    minutes_per_trip = drive_minutes / assignment_count if assignment_count else 0.0
    return {
        "target_month": run.target_month,
        "run_id": run.run_id,
        "trips_offered": trips_offered,
        "trips_attributed": assignment_count,
        "coverage_pct": round(100.0 * assignment_count / max(1, trips_offered), 2),
        "capacity_drive_minutes": capacity_drive_minutes,
        "ceiling_pct": (
            round(100.0 * (budget_minutes / minutes_per_trip) / max(1, trips_offered), 2)
            if minutes_per_trip
            else 0.0
        ),
        "saturation_pct": round(100.0 * drive_minutes / budget_minutes, 2),
        "rejection_counts": rejected,
        "clip_rate": round(clip_rate, 4),
    }


def _optional_config_int(value: str) -> int | None:
    """EMR의 선택 인자에서 `config`를 설정 파일 사용 표식으로 해석합니다."""
    return None if value == "config" else int(value)


def _config_with_overrides(
    config: GenerationConfig,
    *,
    seed: int | None,
    bucket_size: int | None,
) -> GenerationConfig:
    """CLI 재정의를 실제 실행 설정과 config_hash 입력에 함께 반영합니다."""
    return replace(
        config,
        global_seed=config.global_seed if seed is None else seed,
        allocation=replace(
            config.allocation,
            bucket_size=(
                config.allocation.bucket_size
                if bucket_size is None
                else bucket_size
            ),
        ),
    )


def main(args_list: list[str] | None = None) -> Path | str:
    parser = argparse.ArgumentParser(description="월별 가짜 기사-운행 원천 릴리스 생성")
    parser.add_argument("--hvfhv_input_path", required=True)
    parser.add_argument("--zone_lookup_path", required=True)
    parser.add_argument("--vehicle_master_path", required=True)
    parser.add_argument("--state_output_dir", required=True)
    parser.add_argument("--release_output_dir", required=True)
    parser.add_argument("--attribution_output_dir", required=True)
    parser.add_argument("--year_month", required=True)
    parser.add_argument(
        "--seed",
        type=_optional_config_int,
        default=None,
        help="비우거나 config면 generation.json의 global_seed",
    )
    parser.add_argument(
        "--bucket_size",
        type=_optional_config_int,
        default=None,
        help="비우거나 config면 generation.json의 allocation.bucket_size",
    )
    parser.add_argument("--spark_memory", default="4g")
    parser.add_argument("--test_row_limit", type=int, default=0)
    parser.add_argument("--storage", choices=("local", "s3"), default="local")
    parser.add_argument("--bucket", default=None, help="storage=s3일 때. 비우면 DATA_LAKE_S3_BUCKET")
    # `--storage` 와 역할이 다릅니다 — storage 는 입출력을 "어디에" 두는지, env 는
    # Spark 세션을 "어디서" 띄우는지입니다. local 은 컨테이너 안 local[3], prod 는
    # spark-submit(EMR Serverless) 이 준 세션을 그대로 씁니다. main job 과 같은 규칙.
    parser.add_argument(
        "--env",
        choices=("local", "prod"),
        default=os.getenv("SPARK_JOB_ENV", "local"),
        help="local=컨테이너 내 local[3], prod=spark-submit 세션(EMR Serverless)",
    )
    args = parser.parse_args(args_list)
    bucket = args.bucket or (os.environ["DATA_LAKE_S3_BUCKET"] if args.storage == "s3" else None)
    # EMR 워커는 Airflow 컨테이너의 로컬 디스크를 볼 수 없습니다. 조합을 허용하면
    # executor 가 FileNotFoundException 으로 죽는 데까지 수십 분이 걸립니다.
    if args.env == "prod" and args.storage != "s3":
        raise ValueError("--env prod 는 --storage s3 가 필요합니다 (EMR 워커는 로컬 디스크를 못 봅니다)")
    # 반대 방향 — 로컬 pyspark 는 hadoop-aws jar 이 없어 `s3://` 를 못 읽습니다(#712).
    # 조합이 아니라 실제로 들어온 경로로 판정합니다. `--storage s3` 로 출력만 S3 에
    # 쓰면서 입력은 로컬 파일로 직접 지정하는 실행을 막지 않기 위함입니다.
    if args.env == "local":
        s3_inputs = [
            f"{name}={value}"
            for name, value in (
                ("--hvfhv_input_path", args.hvfhv_input_path),
                ("--zone_lookup_path", args.zone_lookup_path),
                ("--vehicle_master_path", args.vehicle_master_path),
            )
            if is_s3_uri(value)
        ]
        if s3_inputs:
            raise ValueError(
                "--env local 은 s3:// 입력을 읽을 수 없습니다 (로컬 pyspark 에 hadoop-aws jar "
                f"없음, #712). --env prod 로 실행하거나 로컬 경로를 넘기세요: {s3_inputs}"
            )

    # lifecycle(join/exit/vehicle_change) 비율은 이제 `--change_rate` 가 아니라
    # config의 driver.{join,exit,vehicle_change}_rate 가 소유합니다 (#605/#628).
    config = _config_with_overrides(
        load_config(), seed=args.seed, bucket_size=args.bucket_size
    )
    run = RunContext.create(args.year_month, config)
    input_scope = "full" if args.test_row_limit == 0 else f"test_row_limit={args.test_row_limit}"

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
        # `Path` 로 올라가면 `s3://` 가 `s3:/` 로 뭉개집니다.
        hvfhv_input_dir=parent_uri(args.hvfhv_input_path, 2),
        output_dir=state_output_dir,
        snapshot_date=snapshot_date,
        config=config,
        vehicle_master_path=args.vehicle_master_path,
        storage=args.storage,
        bucket=bucket,
    )

    spark = get_or_create_spark_session(
        "synthetic_driver_trip_source",
        driver_memory=args.spark_memory,
        local_mode=args.env == "local",
    )
    spark.conf.set(
        "spark.sql.files.maxPartitionBytes",
        str(128 * 1024 * 1024 // config.allocation.bucket_size),
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
    current_driver_vehicle = read(str(state.current_driver_vehicle_path))
    vehicle_master = read(args.vehicle_master_path)
    candidates, candidate_rejects = build_trip_candidates(
        trips,
        preferences,
        current_driver_vehicle,
        seed=config.global_seed,
        bucket_size=config.allocation.bucket_size,
        score_weights=config.allocation.score_weights,
    )
    candidates = candidates.persist(StorageLevel.DISK_ONLY)
    assignments, allocation_rejects, assignment_count = allocate_trips(
        candidates, build_travel_times(trips)
    )
    candidates.unpersist(blocking=True)
    if args.storage == "s3":
        write_attribution_s3(assignments, bucket=bucket, year_month=args.year_month)
    else:
        write_attribution(
            assignments,
            output_dir=_test_scoped_root(args.attribution_output_dir, args.test_row_limit),
            year_month=args.year_month,
        )
    # 릴리스 계보가 아니라 진단용입니다 — manifest 에는 싣지 않습니다(#644).
    print(f"배정 탈락 사유: {dict(candidate_rejects, **allocation_rejects)}")
    quality_report = _quality_report(
        run=run,
        trips=trips,
        preferences=preferences,
        assignments=assignments,
        assignment_count=assignment_count,
        rejected=dict(candidate_rejects, **allocation_rejects),
        clip_rate=state.clip_rate,
    )
    trip_source = build_trip_source(
        raw_trips,
        trips,
        assignments,
    ).persist(StorageLevel.DISK_ONLY)
    if trip_source.count() != assignment_count:
        raise ValueError("배정 결과와 HVFHV 원천 행이 일대일로 연결되지 않습니다")
    snapshot_source = build_driver_vehicle_monthly_snapshot(
        current_driver_vehicle,
        vehicle_master,
        snapshot_date=snapshot_date,
        year_month=args.year_month,
        seed=config.global_seed,
    ).persist(StorageLevel.DISK_ONLY)
    inventory_source = build_lease_vehicle_inventory(
        current_driver_vehicle, vehicle_master
    ).persist(StorageLevel.DISK_ONLY)
    try:
        if args.storage == "s3":
            final = write_source_release_s3(
                trip_source,
                snapshot_source,
                inventory_source,
                bucket=bucket,
                run=run,
                input_scope=input_scope,
            )
            import boto3

            boto3.client("s3").put_object(
                Bucket=bucket,
                Key=f"source/published/_quality_reports/year_month={args.year_month}.json",
                Body=json.dumps(quality_report, ensure_ascii=False, indent=2).encode("utf-8"),
                ServerSideEncryption="AES256",
            )
        else:
            final = write_source_release(
                trip_source,
                snapshot_source,
                inventory_source,
                output_dir=release_output_dir,
                run=run,
                input_scope=input_scope,
            )
            (final / "quality_report.json").write_text(
                json.dumps(quality_report, ensure_ascii=False, indent=2), encoding="utf-8"
            )
        return final
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
