"""기사 데이터 수집과 Bronze·Silver 검증 함수."""

import logging
import os
import uuid
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from airflow.sdk import task

from common.lambda_runtime import lambda_handler_for
from common.project_paths import PROJECT_ROOT
from common.synthetic_release import validate_synthetic_bronze
from schema.silver.driver_vehicle_leases import REQUIRED_NON_NULL, SCHEMA


logger = logging.getLogger(__name__)
DEFAULT_API_BASE_URL = "http://host.docker.internal:8091"
DEFAULT_BRONZE_DIR = os.getenv(
    "BRONZE_DIR", str(PROJECT_ROOT / "data" / "bronze")
)
DEFAULT_SILVER_DIR = os.getenv(
    "DRIVER_MASTER_SILVER_DIR",
    str(PROJECT_ROOT / "data" / "silver" / "driver_vehicle_leases"),
)


def _clean_table(table: pa.Table) -> pa.Table:
    missing = set(SCHEMA.names) - set(table.column_names)
    if missing:
        raise ValueError(f"기사 데이터 필수 컬럼 누락: {sorted(missing)}")
    try:
        cleaned = table.select(SCHEMA.names).cast(SCHEMA)
    except (pa.ArrowInvalid, pa.ArrowNotImplementedError) as exc:
        raise ValueError("기사 데이터 타입을 Silver 스키마로 변환하지 못했습니다") from exc
    rows = cleaned.to_pylist()
    if not rows:
        raise ValueError("기사 데이터가 비어 있습니다")

    for row in rows:
        for column in REQUIRED_NON_NULL:
            value = row[column]
            if value is None or (isinstance(value, str) and not value.strip()):
                raise ValueError(f"기사 데이터 필수값이 비었습니다: {column}")
            if isinstance(value, str):
                row[column] = value.strip()
        row["make_key"] = row["make_key"].upper()
        row["model_key"] = row["model_key"].upper()
        if not 1900 <= row["model_year"] <= 2100:
            raise ValueError("model_year가 허용 범위를 벗어났습니다")
        ended = row["lease_ended_on"]
        if ended is not None and row["lease_started_on"] >= ended:
            raise ValueError("리스 종료일은 시작일보다 늦어야 합니다")

    lease_ids = [row["lease_id"] for row in rows]
    if len(lease_ids) != len(set(lease_ids)):
        raise ValueError("lease_id가 중복됩니다")
    _validate_no_overlap(rows, "taxi_id")
    _validate_no_overlap(rows, "driver_id")
    return pa.Table.from_pylist(rows, schema=SCHEMA)


def _validate_no_overlap(rows: list[dict], key: str) -> None:
    grouped: dict[str, list[tuple[date, date | None]]] = {}
    for row in rows:
        grouped.setdefault(row[key], []).append(
            (row["lease_started_on"], row["lease_ended_on"])
        )
    for value, periods in grouped.items():
        periods.sort(key=lambda period: period[0])
        for previous, current in zip(periods, periods[1:]):
            previous_end = previous[1]
            if previous_end is None or current[0] < previous_end:
                raise ValueError(f"{key}의 리스 기간이 겹칩니다: {value}")


def write_silver(table: pa.Table, output_dir: str | Path, year_month: str) -> Path:
    target = (
        Path(output_dir)
        / f"year_month={year_month}"
        / "driver_vehicle_leases.parquet"
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(f".{target.name}.{uuid.uuid4().hex}.tmp")
    try:
        pq.write_table(table, temporary, compression="snappy")
        temporary.replace(target)
    finally:
        temporary.unlink(missing_ok=True)
    return target


def clean_bronze_to_silver(
    bronze_path: str | Path,
    output_dir: str | Path,
    year_month: str,
) -> dict:
    table = pq.ParquetFile(bronze_path).read()
    cleaned = _clean_table(table)
    path = write_silver(cleaned, output_dir, year_month)
    return {
        "row_count": cleaned.num_rows,
        "locations": [str(path)],
        "year_month": year_month,
    }


def validate_silver_result(result: dict, expected_rows: int) -> None:
    locations = result.get("locations")
    if not isinstance(locations, list) or len(locations) != 1:
        raise ValueError("기사·택시 Silver 경로는 하나여야 합니다")
    path = Path(locations[0])
    if not path.is_file():
        raise ValueError(f"기사·택시 Silver 파일이 없습니다: {path}")
    table = pq.ParquetFile(path).read()
    if table.schema != SCHEMA or table.num_rows != expected_rows:
        raise ValueError("기사·택시 Silver 스키마 또는 행 수가 Bronze와 다릅니다")
    _clean_table(table)


@task(task_id="raw_to_bronze")
def raw_to_bronze_task(**context) -> dict:
    params = context.get("params", {})
    event = {
        "api_base_url": params.get("api_base_url") or DEFAULT_API_BASE_URL,
        "base_dir": params.get("base_dir") or DEFAULT_BRONZE_DIR,
        "year": params.get("year"),
        "month": params.get("month"),
    }
    logger.info("기사 데이터 Raw→Bronze 수집 시작: %s", event)
    return lambda_handler_for("driver_master_raw_to_bronze")(event=event)


@task(task_id="validate_bronze")
def validate_bronze_task(result: dict, **context) -> dict:
    base_dir = context.get("params", {}).get("base_dir") or DEFAULT_BRONZE_DIR
    path, _ = validate_synthetic_bronze(
        result,
        dataset="driver_vehicle_leases",
        dataset_dir="driver_vehicle_leases",
        base_dir=base_dir,
    )
    missing = set(SCHEMA.names) - set(pq.read_schema(path).names)
    if missing:
        raise ValueError(f"기사·택시 Bronze 필수 컬럼 누락: {sorted(missing)}")
    return result


@task(task_id="bronze_to_silver")
def bronze_to_silver_task(result: dict, **context) -> dict:
    return clean_bronze_to_silver(
        result["locations"][0],
        context["params"].get("silver_dir") or DEFAULT_SILVER_DIR,
        result["year_month"],
    )


@task(task_id="validate_silver")
def validate_silver_task(silver_result: dict, raw_result: dict) -> None:
    validate_silver_result(silver_result, raw_result["row_count"])
