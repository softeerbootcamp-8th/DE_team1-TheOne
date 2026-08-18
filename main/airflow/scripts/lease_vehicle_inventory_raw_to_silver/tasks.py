"""보유 차량 데이터 수집과 Bronze·Silver 검증 함수."""

import logging
import os
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from airflow.sdk import task

from shared.airflow.common.lambda_runtime import lambda_handler_for
from shared.airflow.common.project_paths import PROJECT_ROOT
from main.airflow.common.monthly_bronze import (
    DEFAULT_API_BASE_URL,
    DEFAULT_BRONZE_DIR,
    validate_synthetic_bronze,
)
from main.airflow.common.monthly_silver import write_month_partition
from schema.silver.lease_vehicle_inventory import REQUIRED_NON_NULL, SCHEMA


logger = logging.getLogger(__name__)
DATASET = "lease_vehicle_inventory"
DEFAULT_SILVER_DIR = os.getenv(
    "LEASE_VEHICLE_INVENTORY_SILVER_DIR",
    str(PROJECT_ROOT / "data" / "silver" / DATASET),
)
# 0 이하면 재고·연비·가격 어느 쪽이든 계산에 쓸 수 없는 값입니다. 그대로 두면
# Gold 의 대당 수익 계산이 0 으로 나누거나 음수 이익을 내놓습니다.
POSITIVE_COLUMNS = ("fuel_efficiency", "weekly_price_usd", "stock")


def _clean_table(table: pa.Table) -> pa.Table:
    missing = set(SCHEMA.names) - set(table.column_names)
    if missing:
        raise ValueError(f"보유 차량 데이터 필수 컬럼 누락: {sorted(missing)}")
    try:
        cleaned = table.select(SCHEMA.names).cast(SCHEMA)
    except (pa.ArrowInvalid, pa.ArrowNotImplementedError) as exc:
        raise ValueError("보유 차량 데이터 타입을 Silver 스키마로 변환하지 못했습니다") from exc
    rows = cleaned.to_pylist()
    if not rows:
        raise ValueError("보유 차량 데이터가 비어 있습니다")

    for row in rows:
        for column in REQUIRED_NON_NULL:
            value = row[column]
            if value is None or (isinstance(value, str) and not value.strip()):
                raise ValueError(f"보유 차량 데이터 필수값이 비었습니다: {column}")
            if isinstance(value, str):
                row[column] = value.strip()
        # 리스 계약(driver_vehicle_leases)의 make_key·model_key 와 붙일 조인 키라
        # 같은 대문자 규칙으로 맞춥니다.
        row["manufacturer"] = row["manufacturer"].upper()
        row["model_name"] = row["model_name"].upper()
        if not 1900 <= row["model_year"] <= 2100:
            raise ValueError("model_year가 허용 범위를 벗어났습니다")
        for column in POSITIVE_COLUMNS:
            if row[column] <= 0:
                raise ValueError(f"보유 차량 데이터 값이 0 이하입니다: {column}")

    model_ids = [row["vehicle_model_id"] for row in rows]
    if len(model_ids) != len(set(model_ids)):
        raise ValueError("vehicle_model_id가 중복됩니다")
    return pa.Table.from_pylist(rows, schema=SCHEMA)


def write_silver(table: pa.Table, output_dir: str | Path, year_month: str) -> Path:
    return write_month_partition(table, output_dir, year_month, f"{DATASET}.parquet")


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
        raise ValueError("보유 차량 Silver 경로는 하나여야 합니다")
    path = Path(locations[0])
    if not path.is_file():
        raise ValueError(f"보유 차량 Silver 파일이 없습니다: {path}")
    table = pq.ParquetFile(path).read()
    if table.schema != SCHEMA or table.num_rows != expected_rows:
        raise ValueError("보유 차량 Silver 스키마 또는 행 수가 Bronze와 다릅니다")
    _clean_table(table)


@task(task_id="inventory_raw_to_bronze")
def raw_to_bronze_task(**context) -> dict:
    params = context.get("params", {})
    return _collect_bronze(params)


def _collect_bronze(params: dict) -> dict:
    event = {
        "api_base_url": params.get("api_base_url") or DEFAULT_API_BASE_URL,
        "base_dir": params.get("base_dir") or DEFAULT_BRONZE_DIR,
        "year": params.get("year"),
        "month": params.get("month"),
    }
    logger.info("보유 차량 데이터 Raw→Bronze 수집 시작: %s", event)
    return lambda_handler_for("lease_vehicle_inventory_raw_to_bronze")(event=event)


@task(task_id="validate_inventory_bronze")
def validate_bronze_task(result: dict, **context) -> dict:
    params = context.get("params", {})
    base_dir = params.get("base_dir") or DEFAULT_BRONZE_DIR
    _, missing = _validate_bronze_result(result, base_dir)
    if missing:
        logger.warning("보유 차량 Bronze 필수 컬럼 누락(%s), 원천부터 한 번 다시 수집", missing)
        result = _collect_bronze(params)
        _, missing = _validate_bronze_result(result, base_dir)
    if missing:
        raise ValueError(f"보유 차량 Bronze 필수 컬럼 누락: {missing}")
    return result


def _validate_bronze_result(
    result: dict,
    base_dir: str | Path,
) -> tuple[Path, list[str]]:
    path, _ = validate_synthetic_bronze(
        result,
        dataset=DATASET,
        dataset_dir=DATASET,
        base_dir=base_dir,
    )
    missing = sorted(set(SCHEMA.names) - set(pq.read_schema(path).names))
    return path, missing


@task(task_id="inventory_bronze_to_silver")
def bronze_to_silver_task(result: dict, **context) -> dict:
    return clean_bronze_to_silver(
        result["locations"][0],
        context["params"].get("inventory_silver_dir") or DEFAULT_SILVER_DIR,
        result["year_month"],
    )


@task(task_id="validate_inventory_silver")
def validate_silver_task(silver_result: dict, raw_result: dict) -> None:
    validate_silver_result(silver_result, raw_result["row_count"])
