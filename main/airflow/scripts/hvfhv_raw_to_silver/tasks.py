"""HVFHV Raw-to-Silver DAG의 실행·검증 함수."""

import importlib
import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from airflow.sdk import task

from shared.airflow.common.lambda_runtime import lambda_handler_for
from shared.airflow.common.project_paths import PROJECT_ROOT
from shared.airflow.common.slack_failure_callback import slack_failure_callback
from main.airflow.common.monthly_bronze import validate_synthetic_bronze
from shared.airflow.common.validation import (
    parse_handler_result,
    parse_year_month,
    run_gx_validation,
)
from schema.bronze import MONTHLY_TAXI_TRIP_SCHEMA as SCHEMA


logger = logging.getLogger(__name__)

for path in (
    PROJECT_ROOT / "main" / "lambda",
    PROJECT_ROOT / "main" / "spark",
):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

DEFAULT_BRONZE_DIR = os.getenv(
    "BRONZE_DIR", str(PROJECT_ROOT / "data" / "bronze")
)
DEFAULT_SILVER_DIR = os.getenv(
    "SILVER_DIR", str(PROJECT_ROOT / "data" / "silver" / "hvfhv")
)
DEFAULT_ZONE_LOOKUP_PATH = os.getenv(
    "ZONE_LOOKUP_PATH",
    str(PROJECT_ROOT / "data" / "bronze" / "taxi_zone_lookup.csv"),
)
# Bronze 한 달에서 버려도 되는 행의 비율. 넘으면 원천이 바뀐 것으로 보고 멈춥니다.
# 0.2 는 초기 관측치를 넉넉히 감싸려고 둔 값이라, 원천 스키마가 통째로 어긋나도
# 통과할 만큼 느슨했습니다. 실측 불합격률이 1% 미만이라 5% 로 조입니다 (#508).
HVFHV_ERROR_THRESHOLD = 0.05
DEFAULT_API_BASE_URL = "http://host.docker.internal:8091"


def _schema_signature(schema: pa.Schema, *, logical_timestamp: bool = False) -> str:
    """GX가 한 행짜리 품질 요약에서 비교할 Parquet 스키마 문자열을 만듭니다."""
    fields = []
    for field in schema:
        if logical_timestamp and pa.types.is_timestamp(field.type):
            field_type = (
                "timestamp"
                if field.type.tz is None
                else f"timestamp[tz={field.type.tz}]"
            )
        else:
            field_type = str(field.type)
        fields.append(f"{field.name}:{field_type}")
    return "|".join(fields)


def _bronze_quality_summary(parquet_file, required_columns):
    """Spark와 같은 유효성 조건을 Parquet 배치별로 계산합니다.

    물리 스키마 전체 일치는 확인하지 않습니다 — 원천이 MONTHLY_TAXI_TRIP_SCHEMA 보다
    많은 컬럼을 보내도(#529 진행 중) required_columns 만 있으면 검증을 계속합니다.
    """
    import pandas as pd

    schema = parquet_file.schema_arrow
    row_count = parquet_file.metadata.num_rows
    missing_columns = [name for name in required_columns if name not in schema.names]
    invalid_rows = 0

    if row_count and not missing_columns:
        for batch in parquet_file.iter_batches(columns=required_columns):
            frame = batch.to_pandas()
            trip_miles = pd.to_numeric(frame["trip_miles"], errors="coerce")
            trip_time = pd.to_numeric(frame["trip_time"], errors="coerce")
            driver_pay = pd.to_numeric(frame["driver_pay"], errors="coerce")
            valid = (
                pd.to_datetime(frame["pickup_datetime"], errors="coerce").notna()
                & pd.to_datetime(
                    frame["dropoff_datetime"], errors="coerce"
                ).notna()
                & trip_miles.gt(0)
                & trip_miles.le(1000)
                & trip_time.gt(0)
                & trip_time.le(86400)
                & driver_pay.ge(0)
                & driver_pay.le(5000)
                & frame["taxi_id"].notna()
                & frame["taxi_id"].ne("")
            )
            invalid_rows += int((~valid).sum())

    return pd.DataFrame(
        [
            {
                "row_count": row_count,
                "schema_signature": _schema_signature(schema),
                "missing_required_columns": ",".join(missing_columns),
                "invalid_required_row_ratio": (
                    invalid_rows / row_count
                    if row_count and not missing_columns
                    else None
                ),
            }
        ]
    )


def _spark_schema_to_arrow(spark_schema) -> pa.Schema:
    type_map = {
        "string": pa.string(),
        "timestamp": pa.timestamp("us"),
        "int": pa.int32(),
        "bigint": pa.int64(),
        "double": pa.float64(),
    }
    return pa.schema(
        pa.field(field.name, type_map[field.dataType.simpleString()])
        for field in spark_schema.fields
        if field.name != "year_month"
    )


def _silver_quality_summary(parquet_files, required_columns):
    """Silver 월 전체 행 수·논리 스키마·필수값 NULL 건수를 요약합니다."""
    import pandas as pd

    row_count = 0
    schema_signatures = set()
    null_counts = {column: 0 for column in required_columns}
    for parquet_file in parquet_files:
        schema = parquet_file.schema_arrow
        row_count += parquet_file.metadata.num_rows
        schema_signatures.add(_schema_signature(schema, logical_timestamp=True))
        for column in required_columns:
            if column not in schema.names:
                null_counts[column] = None
            elif null_counts[column] is not None:
                null_counts[column] += sum(
                    batch.column(column).null_count
                    for batch in parquet_file.iter_batches(columns=[column])
                )
    return pd.DataFrame(
        [
            {
                "row_count": row_count,
                "schema_signature": ",".join(sorted(schema_signatures)),
                **{
                    f"{column}_null_count": value
                    for column, value in null_counts.items()
                },
            }
        ]
    )


@task(task_id="raw_to_bronze")
def raw_to_bronze_task(**context) -> dict:
    """HVFHV+taxi_id 데이터를 Bronze에 저장합니다."""
    params = context.get("params", {})
    return _collect_bronze(params)


def _collect_bronze(params: dict) -> dict:
    event = {
        "api_base_url": params.get("api_base_url") or DEFAULT_API_BASE_URL,
        "base_dir": params.get("base_dir") or DEFAULT_BRONZE_DIR,
        "year": params.get("year"),
        "month": params.get("month"),
    }
    logger.info("raw_to_bronze 작업 시작: event=%s", event)
    result = lambda_handler_for("hvfhv_raw_to_bronze")(event=event)
    logger.info("raw_to_bronze 작업 완료: result=%s", result)
    return result


@task(
    task_id="validate_bronze",
    retries=1,
    retry_delay=timedelta(minutes=10),
    on_failure_callback=slack_failure_callback,
)
def validate_bronze_task(result: dict, **context) -> dict:
    """파일 경계를 확인한 뒤 Bronze 데이터 품질을 GX로 검증합니다."""
    params = context.get("params", {})
    summary = _bronze_quality_result(result, params, list(SCHEMA.names))
    missing = summary.at[0, "missing_required_columns"]
    if missing:
        logger.warning("Bronze 필수 컬럼 누락(%s), 원천부터 한 번 다시 수집", missing)
        result = _collect_bronze(params)
        summary = _bronze_quality_result(
            result, params, list(SCHEMA.names)
        )

    import great_expectations as gx

    expectations = [
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="row_count", min_value=1
        ),
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="missing_required_columns", value_set=[""]
        ),
    ]
    if summary["invalid_required_row_ratio"].notna().all():
        expectations.append(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="invalid_required_row_ratio",
                min_value=0,
                max_value=HVFHV_ERROR_THRESHOLD,
                strict_max=True,
            )
        )
    run_gx_validation(
        summary,
        expectations,
        suite_name="hvfhv_bronze_suite",
        layer="bronze",
    )
    return result


def _bronze_quality_result(
    result: dict,
    params: dict,
    required_columns: list[str],
):
    base_dir = params.get("base_dir") or DEFAULT_BRONZE_DIR
    path, _ = validate_synthetic_bronze(
        result,
        dataset="hvfhv_taxi_trips",
        dataset_dir="hvfhv",
        base_dir=base_dir,
    )
    try:
        parquet_file = pq.ParquetFile(path)
    except (OSError, pa.ArrowInvalid) as exc:
        raise ValueError(
            f"Parquet 을 읽지 못했습니다 (다운로드가 잘렸을 수 있음): {path}"
        ) from exc

    summary = _bronze_quality_summary(parquet_file, required_columns)
    return summary


@task(
    task_id="validate_silver",
    retries=1,
    retry_delay=timedelta(minutes=10),
    on_failure_callback=slack_failure_callback,
)
def validate_silver_task(raw_result: dict) -> None:
    """BashOperator 라 handler 결과 dict 가 없어, Silver 파티션을 직접 열어서 확인합니다."""
    parsed = parse_handler_result(raw_result, expected_locations=1)
    year_month = parse_year_month(
        raw_result.get("year_month"), field="year_month"
    )
    bronze_rows = pq.ParquetFile(parsed.locations[0]).metadata.num_rows

    silver_partition = Path(DEFAULT_SILVER_DIR) / f"year_month={year_month}"
    silver_files = sorted(silver_partition.glob("*.parquet"))
    if not silver_files:
        raise ValueError(
            f"Silver 파티션에 Parquet 파일이 없습니다: {silver_partition}"
        )

    transformer = importlib.import_module(
        "jobs.bronze_to_silver.hvfhv.transformer"
    )
    expected_schema = _spark_schema_to_arrow(transformer.FINAL_SCHEMA)
    required_columns = [
        field.name
        for field in transformer.FINAL_SCHEMA.fields
        if not field.nullable and field.name != "year_month"
    ]
    parquet_files = [pq.ParquetFile(path) for path in silver_files]
    summary = _silver_quality_summary(parquet_files, required_columns)
    import great_expectations as gx

    expectations = [
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="row_count", min_value=1
        ),
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="schema_signature",
            value_set=[
                _schema_signature(expected_schema, logical_timestamp=True)
            ],
        ),
        *(
            gx.expectations.ExpectColumnValuesToBeInSet(
                column=f"{column}_null_count", value_set=[0]
            )
            for column in required_columns
        ),
    ]
    run_gx_validation(
        summary,
        expectations,
        suite_name="hvfhv_silver_suite",
        layer="silver",
    )

    silver_rows = int(summary["row_count"].sum())
    if silver_rows > bronze_rows:
        raise ValueError(
            f"Silver 행 수가 Bronze 보다 많습니다: {silver_rows} > {bronze_rows}"
        )

    other_partitions = [
        p
        for p in Path(DEFAULT_SILVER_DIR).glob("year_month=*")
        if p.name != silver_partition.name
    ]
    if other_partitions:
        prev_first_day = datetime.strptime(year_month, "%Y-%m").replace(
            day=1
        ) - timedelta(days=1)
        prev_partition = (
            Path(DEFAULT_SILVER_DIR)
            / f"year_month={prev_first_day:%Y-%m}"
        )
        if not any(prev_partition.glob("*.parquet")):
            raise ValueError(
                "직전 달 파티션이 사라졌습니다 (#165 재발): "
                f"{prev_partition}"
            )
