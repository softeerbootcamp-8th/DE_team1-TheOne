"""보유 차량 데이터 수집·정제 Lambda 실행과 Bronze·Silver 검증 함수."""

import importlib
import logging
import os
from pathlib import Path

from airflow.sdk import task

from shared.airflow.common.lambda_invoke import invoke_lambda
from shared.airflow.common.project_paths import PROJECT_ROOT
from shared.airflow.common.validation import (
    S3Location,
    parse_handler_result,
    parse_location,
    read_parquet,
    run_quality_gate,
    run_table_gx_validation,
)
from shared.aws_lambda.common.schema_validator import (
    SchemaValidationResult,
    validate_parquet_schema,
)
from main.airflow.common.monthly_bronze import (
    silver_version_path,
    validate_monthly_parquet_bronze,
)
from schema.bronze import LEASE_VEHICLE_INVENTORY_SCHEMA as BRONZE_SCHEMA
from schema.silver import (
    CLEAN_LEASE_VEHICLE_INVENTORY_REQUIRED_NON_NULL as SILVER_REQUIRED,
    CLEAN_LEASE_VEHICLE_INVENTORY_SCHEMA as SILVER_SCHEMA,
)
from schema.source import LEASE_VEHICLE_INVENTORY_REQUIRED_NON_NULL as BRONZE_REQUIRED


logger = logging.getLogger(__name__)
DATASET = "lease_vehicle_inventory"
DEFAULT_API_BASE_URL = "http://10.0.10.81:8091"
DEFAULT_BRONZE_DIR = os.getenv(
    "BRONZE_DIR", str(PROJECT_ROOT / "data" / "bronze")
)
DEFAULT_SILVER_DIR = os.getenv(
    "LEASE_VEHICLE_INVENTORY_SILVER_DIR",
    str(PROJECT_ROOT / "data" / "silver" / DATASET),
)


def _silver_transformer():
    """정제 규칙은 Lambda 쪽 Transformer 가 원본입니다. DAG 파싱까지 그 모듈을
    끌어오지 않도록 검증할 때만 불러옵니다 (다른 DAG 도 같은 방식)."""
    module = importlib.import_module(
        "main.aws_lambda.functions.lease_vehicle_inventory_bronze_to_silver.transformer"
    )
    return module.LeaseVehicleInventorySilverTransformer()


def validate_silver_result(
    result: dict, expected_rows: int, context: dict | None = None
) -> None:
    parsed = parse_handler_result(result, expected_locations=1)
    path = parsed.locations[0]
    try:
        table = read_parquet(path)
    except FileNotFoundError:
        raise ValueError(f"보유 차량 Silver 파일이 없습니다: {path}")
    if table.schema != SILVER_SCHEMA or table.num_rows != expected_rows:
        raise ValueError("보유 차량 Silver 스키마 또는 행 수가 Bronze와 다릅니다")
    # 적재된 파일에 같은 정제 규칙을 다시 적용합니다. 변환이 통과했더라도 적재
    # 과정에서 다른 파일이 놓였다면 여기서 걸립니다.
    _silver_transformer().transform(table)
    if isinstance(path, S3Location):
        run_table_gx_validation(
            table,
            SILVER_SCHEMA,
            SILVER_REQUIRED,
            dataset=DATASET,
            layer="silver",
            data_location=path,
            context=context or {},
            required_warning_ratio=None,
            required_error_ratio=0,
        )


@task(task_id="raw_to_bronze")
def raw_to_bronze_task(**context) -> dict:
    params = context.get("params", {})
    return _collect_bronze(params)


def _collect_bronze(params: dict) -> dict:
    event = {
        "api_base_url": params.get("api_base_url") or DEFAULT_API_BASE_URL,
        "year": params.get("year"),
        "month": params.get("month"),
    }
    if params.get("service_area") is not None:
        event["service_area"] = params["service_area"]
    logger.info("보유 차량 데이터 Raw→Bronze 수집 시작: %s", event)
    return invoke_lambda(
        "lease_vehicle_inventory_raw_to_bronze",
        package="main.aws_lambda.functions",
        event=event,
        local_event={
            "base_dir": params.get("base_dir") or DEFAULT_BRONZE_DIR,
        },
    )


@task(task_id="validate_bronze")
def validate_bronze_task(result: dict, **context) -> dict:
    state = {"result": result}
    params = context.get("params", {})
    return run_quality_gate(
        lambda: parse_location(state["result"]["locations"][0]).parent,
        lambda: _validate_bronze(state, params, context),
        layer="bronze",
        context=context,
    )


def _validate_bronze(
    state: dict, params: dict, context: dict | None = None
) -> dict:
    result = state["result"]
    base_dir = params.get("base_dir") or DEFAULT_BRONZE_DIR
    service_area = params.get("service_area")
    path, schema_result = _validate_bronze_result(result, base_dir, service_area)
    if schema_result.missing_columns:
        logger.warning(
            "보유 차량 Bronze 필수 컬럼 누락(%s), 원천부터 한 번 다시 수집",
            schema_result.missing_columns,
        )
        result = _collect_bronze(params)
        state["result"] = result
        path, schema_result = _validate_bronze_result(result, base_dir, service_area)
    for warning in schema_result.warnings:
        logger.warning("보유 차량 Bronze 스키마 확장: %s", warning)
    if schema_result.errors:
        raise ValueError(
            "보유 차량 Bronze 스키마 불일치: "
            + "; ".join(schema_result.errors)
        )
    if isinstance(path, S3Location):
        run_table_gx_validation(
            read_parquet(path),
            BRONZE_SCHEMA,
            BRONZE_REQUIRED,
            dataset=DATASET,
            layer="bronze",
            data_location=path,
            context=context or {},
            required_warning_ratio=None,
            required_error_ratio=0,
        )
    version_path = silver_version_path(
        params.get("silver_dir") or DEFAULT_SILVER_DIR,
        result,
        service_area=service_area,
    )
    return {
        **result,
        "silver_version_path": str(version_path),
    }


def _validate_bronze_result(
    result: dict,
    base_dir: str | Path,
    service_area: str,
) -> tuple[Path | S3Location, SchemaValidationResult]:
    path, _ = validate_monthly_parquet_bronze(
        result,
        dataset_dir=DATASET,
        base_dir=base_dir,
        service_area=service_area,
    )
    schema_result = validate_parquet_schema(read_parquet(path).schema, BRONZE_SCHEMA)
    return path, schema_result


@task(task_id="bronze_to_silver")
def bronze_to_silver_task(result: dict, **context) -> dict:
    params = context.get("params", {})
    bronze_location = parse_location(result["locations"][0])
    event = {
        "year_month": result["year_month"],
        "silver_output_path": result["silver_version_path"],
    }
    if params.get("service_area") is not None:
        event["service_area"] = params["service_area"]
    if isinstance(bronze_location, S3Location):
        event.update(storage="s3", bucket=bronze_location.bucket)
    logger.info("보유 차량 데이터 Bronze→Silver 정제 시작: %s", event)
    return invoke_lambda(
        "lease_vehicle_inventory_bronze_to_silver",
        package="main.aws_lambda.functions",
        event=event,
    )


@task(task_id="validate_silver")
def validate_silver_task(silver_result: dict, raw_result: dict, **context) -> None:
    version_path = parse_location(raw_result["silver_version_path"])
    run_quality_gate(
        version_path,
        lambda: _validate_silver_output(
            silver_result, raw_result["row_count"], version_path, context
        ),
        layer="silver",
        context=context,
    )


def _validate_silver_output(
    silver_result: dict,
    expected_rows: int,
    version_path: Path | S3Location,
    context: dict | None = None,
) -> None:
    expected_part = (
        f"{version_path}/data.parquet"
        if isinstance(version_path, S3Location)
        else str(version_path / "data.parquet")
    )
    if silver_result["locations"] != [expected_part]:
        raise ValueError("보유 차량 Silver 경로가 Bronze와 다릅니다")
    validate_silver_result(silver_result, expected_rows, context)
