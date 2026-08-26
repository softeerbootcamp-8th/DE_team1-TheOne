"""기사 차량 월별 스냅샷 수집·정제 Lambda 실행과 Bronze·Silver 검증 함수."""

import importlib
import json
import logging
import os
from pathlib import Path

import pyarrow as pa
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
from main.airflow.common.assets import resolve_service_area
from main.airflow.common.monthly_bronze import (
    silver_version_path,
    validate_monthly_parquet_bronze,
)
from schema.silver import (
    CLEAN_DRIVER_VEHICLE_MONTHLY_SNAPSHOT_SCHEMA as SILVER_SCHEMA,
    CLEAN_DRIVER_VEHICLE_MONTHLY_SNAPSHOT_SCHEMA_REQUIRED_NON_NULL as SILVER_REQUIRED,
)


logger = logging.getLogger(__name__)
DATASET = "driver_vehicle_monthly_snapshot"
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


def validate_silver_result(
    result: dict, expected_rows: int, context: dict | None = None
) -> None:
    parsed = parse_handler_result(result, expected_locations=1)
    path = parsed.locations[0]
    try:
        table = read_parquet(path)
    except FileNotFoundError:
        raise ValueError(f"기사 차량 스냅샷 Silver 파일이 없습니다: {path}")
    if table.schema != SILVER_SCHEMA or table.num_rows != expected_rows:
        raise ValueError("기사 차량 스냅샷 Silver 스키마 또는 행 수가 Bronze와 다릅니다")
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
        "api_base_url": params["api_base_url"],
        "year": params.get("year"),
        "month": params.get("month"),
        "service_area": resolve_service_area(params),
    }
    logger.info("기사 차량 스냅샷 Raw→Bronze 수집 시작: %s", event)
    return invoke_lambda(
        "driver_vehicle_monthly_snapshot_raw_to_bronze",
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


def _bronze_recon_counts(table: pa.Table) -> dict:
    """Silver 검증 시 Bronze 에서 기대 Silver 행 수를 계산합니다.

    Silver 변환이 행을 줄이는 경로는 **퇴사 기사 제외 하나뿐**입니다. 나머지 규칙
    (필수값·리스료·driver_id 중복)은 걸러내지 않고 예외를 던지므로 보존식에
    들어가지 않습니다.

        Bronze = Silver + 퇴사 기사

    핸들러가 보고한 행 수를 그 자신의 기대값으로 넘기면 변환과 적재가 같이 틀릴 때
    통과합니다. 다만 이 계산은 원본 공개 여부를 판정하는 Bronze 책임이 아니라,
    정제 결과를 판정하는 Silver 책임으로 둡니다.
    """
    if "exit_date" not in table.column_names:
        raise ValueError("Bronze 에 exit_date 가 없어 보존식을 세울 수 없습니다")
    # 퇴사자 = exit_date 가 있는 행. NULL 이 아직 재직 중입니다.
    exited = table.num_rows - table["exit_date"].null_count
    return {"bronze_row_count": table.num_rows, "exited_driver_rows": exited}


def _validate_bronze(
    state: dict, params: dict, context: dict | None = None
) -> dict:
    result = state["result"]
    base_dir = params.get("base_dir") or DEFAULT_BRONZE_DIR
    service_area = resolve_service_area(params)
    validate_monthly_parquet_bronze(
        result,
        dataset_dir=DATASET,
        base_dir=base_dir,
        service_area=service_area,
    )
    version_path = silver_version_path(
        params.get("silver_dir") or DEFAULT_SILVER_DIR,
        result,
        service_area,
    )
    return {
        **result,
        "silver_version_path": str(version_path),
    }


def _read_bronze_table(path: Path | S3Location) -> pa.Table:
    """Silver 보존식 계산을 위해 타임스탬프 단위를 맞춰 Bronze 를 읽습니다.

    상류 Spark 가 타임스탬프를 **INT96** 으로 씁니다. PyArrow 는 그걸 `ns` 로 읽고
    Bronze 는 원본 바이트를 보존하므로 적재 쪽에서 고칠 수 없습니다. INT96 저장은
    `shared/spark/common/session.py` 가 타임존 처리를 그 위에 세워 둔 값이라
    바꾸면 Gold 조인이 밀립니다.

    Bronze 공개 단계는 레코드를 해석하지 않습니다. 이 정규화는 Silver 보존식 계산과
    정제 규칙 검증에서만 사용합니다.
    """
    table = read_parquet(path)
    fields = []
    changed = False
    for field in table.schema:
        expected_index = SILVER_SCHEMA.get_field_index(field.name)
        if expected_index < 0:
            fields.append(field)
            continue
        expected = SILVER_SCHEMA.field(expected_index).type
        if (
            pa.types.is_timestamp(field.type)
            and pa.types.is_timestamp(expected)
            and field.type != expected
        ):
            fields.append(field.with_type(expected))
            changed = True
        else:
            fields.append(field)
    if not changed:
        return table
    return table.cast(pa.schema(fields))


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
    return invoke_lambda(
        "driver_vehicle_monthly_snapshot_bronze_to_silver",
        package="main.aws_lambda.functions",
        event=event,
    )


@task(task_id="validate_silver")
def validate_silver_task(silver_result: dict, raw_result: dict, **context) -> None:
    version_path = parse_location(raw_result["silver_version_path"])
    run_quality_gate(
        version_path,
        lambda: _validate_silver_output(
            silver_result, raw_result, version_path, context
        ),
        layer="silver",
        context=context,
    )


def _validate_silver_output(
    silver_result: dict,
    raw_result: dict,
    version_path: Path | S3Location,
    context: dict | None = None,
) -> None:
    expected_part = (
        f"{version_path}/data.parquet"
        if isinstance(version_path, S3Location)
        else str(version_path / "data.parquet")
    )
    if silver_result["locations"] != [expected_part]:
        raise ValueError("기사 차량 스냅샷 Silver 경로가 Bronze와 다릅니다")
    validate_silver_result(silver_result, _expected_silver_rows(raw_result), context)


def _expected_silver_rows(raw_result: dict) -> int:
    """`Bronze = Silver + 퇴사 기사` 로 Silver 행 수를 Bronze 에서 되짚습니다.

    Silver 는 퇴사 기사를 제외하니 Bronze 와 행 수가 다릅니다. 그렇다고 비교를
    포기하면(예전에는 핸들러가 보고한 값을 그 자신의 기대값으로 넘겼습니다) 변환과
    적재가 같이 틀렸을 때 통과합니다. 빠진 만큼이 설명되는지를 봅니다.
    """
    parsed = parse_handler_result(raw_result, expected_locations=1)
    table = _read_bronze_table(parsed.locations[0])
    if table.num_rows != parsed.row_count:
        raise ValueError(
            "기사 차량 스냅샷 Bronze 파일 행 수가 수집 결과와 다릅니다"
        )
    counts = _bronze_recon_counts(table)
    bronze_rows = counts["bronze_row_count"]
    exited = counts["exited_driver_rows"]
    expected = bronze_rows - exited
    if expected < 0:
        raise ValueError(
            f"퇴사 기사 수가 Bronze 행 수를 넘습니다: "
            f"bronze={bronze_rows} exited={exited}"
        )
    logger.info(
        "reconciliation %s",
        json.dumps(
            {
                "dataset": DATASET,
                "input_rows": bronze_rows,
                "output_rows": expected,
                "excluded_rows": exited,
                "rule": "input = output + excluded",
            },
            ensure_ascii=False,
        ),
    )
    return expected
