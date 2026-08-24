"""기사 차량 월별 스냅샷 수집·정제 Lambda 실행과 Bronze·Silver 검증 함수."""

import importlib
import logging
import os
from pathlib import Path

from airflow.sdk import task

from shared.airflow.common.lambda_runtime import lambda_handler_for
from shared.airflow.common.project_paths import PROJECT_ROOT
from shared.airflow.common.validation import (
    S3Location,
    parse_handler_result,
    parse_location,
    publish_success_marker,
    read_parquet,
)
from main.airflow.common.assets import resolve_service_area
from main.airflow.common.monthly_bronze import (
    silver_version_path,
    validate_monthly_parquet_bronze,
)
from schema.silver import CLEAN_DRIVER_VEHICLE_MONTHLY_SNAPSHOT_SCHEMA as SCHEMA


logger = logging.getLogger(__name__)
DATASET = "driver_vehicle_monthly_snapshot"
DEFAULT_API_BASE_URL = "http://10.0.10.81:8091"
DEFAULT_BRONZE_DIR = os.getenv(
    "BRONZE_DIR", str(PROJECT_ROOT / "data" / "bronze")
)
DEFAULT_SILVER_DIR = os.getenv(
    "DRIVER_VEHICLE_MONTHLY_SNAPSHOT_SILVER_DIR",
    str(PROJECT_ROOT / "data" / "silver" / DATASET),
)


def _silver_transformer():
    """정제 규칙은 Lambda 쪽 Transformer 가 원본입니다. DAG 파싱까지 그 모듈을
    끌어오지 않도록 검증할 때만 불러옵니다."""
    module = importlib.import_module(
        "main.aws_lambda.functions.driver_vehicle_monthly_snapshot_bronze_to_silver.transformer"
    )
    return module.DriverVehicleMonthlySnapshotSilverTransformer()


def validate_silver_result(result: dict, expected_rows: int) -> None:
    parsed = parse_handler_result(result, expected_locations=1)
    path = parsed.locations[0]
    try:
        table = read_parquet(path)
    except FileNotFoundError:
        raise ValueError(f"기사 차량 스냅샷 Silver 파일이 없습니다: {path}")
    if table.schema != SCHEMA or table.num_rows != expected_rows:
        raise ValueError("기사 차량 스냅샷 Silver 스키마 또는 행 수가 Bronze와 다릅니다")
    # 적재된 파일에 같은 정제 규칙을 다시 적용합니다. 변환이 통과했더라도 적재
    # 과정에서 다른 파일이 놓였다면 여기서 걸립니다.
    _silver_transformer().transform(table)


@task(task_id="raw_to_bronze")
def raw_to_bronze_task(**context) -> dict:
    params = context.get("params", {})
    return _collect_bronze(params)


def _collect_bronze(params: dict) -> dict:
    event = {
        "api_base_url": params.get("api_base_url") or DEFAULT_API_BASE_URL,
        "base_dir": params.get("base_dir") or DEFAULT_BRONZE_DIR,
        "year": params.get("year"),
        "month": params.get("month"),
        "service_area": resolve_service_area(params),
    }
    logger.info("기사 차량 스냅샷 Raw→Bronze 수집 시작: %s", event)
    return lambda_handler_for("driver_vehicle_monthly_snapshot_raw_to_bronze")(event=event)


@task(task_id="validate_bronze")
def validate_bronze_task(result: dict, **context) -> dict:
    params = context.get("params", {})
    base_dir = params.get("base_dir") or DEFAULT_BRONZE_DIR
    service_area = resolve_service_area(params)
    _, missing = _validate_bronze_result(result, base_dir, service_area)
    if missing:
        logger.warning("기사 차량 스냅샷 Bronze 필수 컬럼 누락(%s), 원천부터 한 번 다시 수집", missing)
        result = _collect_bronze(params)
        _, missing = _validate_bronze_result(result, base_dir, service_area)
    if missing:
        raise ValueError(f"기사 차량 스냅샷 Bronze 필수 컬럼 누락: {missing}")
    bronze_path = parse_handler_result(result, expected_locations=1).locations[0]
    publish_success_marker(bronze_path.parent)
    version_path = silver_version_path(
        params.get("silver_dir") or DEFAULT_SILVER_DIR,
        result,
        service_area,
    )
    return {
        **result,
        "silver_version_path": str(version_path),
    }


def _validate_bronze_result(
    result: dict,
    base_dir: str | Path,
    service_area: str,
) -> tuple[Path | S3Location, list[str]]:
    path, _ = validate_monthly_parquet_bronze(
        result,
        dataset_dir=DATASET,
        base_dir=base_dir,
        service_area=service_area,
    )
    missing = sorted(set(SCHEMA.names) - set(read_parquet(path).schema.names))
    return path, missing


@task(task_id="bronze_to_silver")
def bronze_to_silver_task(result: dict, **context) -> dict:
    bronze_location = parse_location(result["locations"][0])
    event = {
        "year_month": result["year_month"],
        "silver_output_path": result["silver_version_path"],
        "service_area": resolve_service_area(context.get("params", {})),
    }
    if isinstance(bronze_location, S3Location):
        event.update(storage="s3", bucket=bronze_location.bucket)
    logger.info("기사 차량 스냅샷 Bronze→Silver 정제 시작: %s", event)
    return lambda_handler_for("driver_vehicle_monthly_snapshot_bronze_to_silver")(event=event)


@task(task_id="validate_silver")
def validate_silver_task(silver_result: dict, raw_result: dict, **context) -> None:
    version_path = parse_location(raw_result["silver_version_path"])
    expected_part = (
        f"{version_path}/data.parquet"
        if isinstance(version_path, S3Location)
        else str(version_path / "data.parquet")
    )
    if silver_result["locations"] != [expected_part]:
        raise ValueError("기사 차량 스냅샷 Silver 경로가 Bronze와 다릅니다")
    # Silver 는 퇴사 기사를 제외하므로 Bronze 와 행 수가 다를 수 있습니다.
    validate_silver_result(silver_result, silver_result["row_count"])
    publish_success_marker(version_path)
