"""HVFHV Raw -> Bronze -> Silver 데이터 파이프라인 DAG.

매월 10일 실행되며, 실행일 기준 직전 달(Previous Month)의 NYC HVFHV 트립 데이터를 수집하여
Bronze 레이어(Parquet)에 적재하고 Spark 정제 작업을 통해 Silver 레이어로 변환합니다.
"""

import importlib
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from airflow.sdk.exceptions import AirflowSkipException

try:
    from airflow.sdk import Param, dag, task
except ImportError:
    from airflow.decorators import dag, task
    from airflow.models.param import Param

try:
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:
    from airflow.operators.bash import BashOperator

from common.validation import (
    parse_handler_result,
    parse_year_month,
    run_gx_validation,
)

# 프로젝트 루트 디렉토리를 sys.path에 추가 (컨테이너 /opt/airflow/project-root 및 로컬 호환)
CURRENT_DIR = Path(__file__).resolve().parent
AIRFLOW_DIR = CURRENT_DIR.parent
CONTAINER_ROOT = Path("/opt/airflow/project-root")
PROJECT_ROOT = CONTAINER_ROOT if CONTAINER_ROOT.exists() else AIRFLOW_DIR.parent

# libs/pipeline_core 는 Airflow 이미지에 설치돼 있지 않아 경로로 참조(이후 변경 필요)
for path_str in [
    str(PROJECT_ROOT),
    str(PROJECT_ROOT / "lambda"),
    str(PROJECT_ROOT / "spark"),
    str(PROJECT_ROOT / "libs" / "pipeline_core"),
]:
    if path_str not in sys.path:
        sys.path.insert(0, path_str)


def lambda_handler_for(function_name: str):
    """`lambda`가 파이썬 예약어라 정적 import가 안 돼 동적으로 불러옵니다."""
    return importlib.import_module(
        f"lambda.functions.{function_name}.handler"
    ).lambda_handler

logger = logging.getLogger(__name__)

# 슬랙 에러 콜백 임포트 (안전한 Fallback 처리)
try:
    from common.slack_failure_callback import slack_failure_callback
except Exception as e:
    logger.warning("slack_failure_callback 임포트 실패 (기본 로깅으로 대체): %s", e)

    def slack_failure_callback(context):
        task_id = context.get("task_instance").task_id if context.get("task_instance") else "unknown"
        logger.error("Task [%s] failed without slack callback.", task_id)

# 기본 설정값 (PROJECT_ROOT 기준 절대경로)
DEFAULT_BRONZE_DIR = os.getenv("BRONZE_DIR", str(PROJECT_ROOT / "data" / "bronze"))
DEFAULT_SILVER_DIR = os.getenv("SILVER_DIR", str(PROJECT_ROOT / "data" / "silver" / "hvfhv"))
DEFAULT_ZONE_LOOKUP_PATH = os.getenv("ZONE_LOOKUP_PATH", str(PROJECT_ROOT / "data" / "bronze" / "taxi_zone_lookup.csv"))
HVFHV_ERROR_THRESHOLD = 0.2


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


def _bronze_quality_summary(parquet_file, expected_schema, required_columns):
    """Spark와 같은 유효성 조건을 Parquet 배치별로 계산합니다."""
    import pandas as pd

    schema = parquet_file.schema_arrow
    row_count = parquet_file.metadata.num_rows
    missing_columns = [name for name in required_columns if name not in schema.names]
    invalid_rows = 0

    if row_count and not missing_columns and schema == expected_schema:
        for batch in parquet_file.iter_batches(columns=required_columns):
            frame = batch.to_pandas()
            trip_miles = pd.to_numeric(frame["trip_miles"], errors="coerce")
            trip_time = pd.to_numeric(frame["trip_time"], errors="coerce")
            fare = pd.to_numeric(frame["base_passenger_fare"], errors="coerce")
            driver_pay = pd.to_numeric(frame["driver_pay"], errors="coerce")
            valid = (
                pd.to_datetime(frame["pickup_datetime"], errors="coerce").notna()
                & pd.to_datetime(frame["dropoff_datetime"], errors="coerce").notna()
                & frame["PULocationID"].notna()
                & frame["DOLocationID"].notna()
                & trip_miles.gt(0)
                & trip_miles.le(1000)
                & trip_time.gt(0)
                & trip_time.le(86400)
                & fare.ge(0)
                & fare.le(5000)
                & driver_pay.ge(0)
                & driver_pay.le(5000)
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
                    if row_count and not missing_columns and schema == expected_schema
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


def resolve_target_year_month(logical_date: datetime, params: dict) -> tuple[str, str]:
    """실행 시점 또는 수동 입력 파라미터를 기반으로 수집/정제 대상 (year, month)를 반환합니다.

    - 수동 트리거 시 params['year'], params['month'] 가 지정되어 있으면 해당 값 우선 사용
    - 기본 스케줄 실행 시: logical_date 기준 직전 달(Previous Month) 계산
      (예: 오늘이 4월 10일이면 3월 데이터 처리)
    """
    param_year = params.get("year")
    param_month = params.get("month")

    if param_year and param_month:
        year_str = str(param_year).strip()
        month_str = str(param_month).strip().zfill(2)
        logger.info("수동 파라미터 적용: year=%s, month=%s", year_str, month_str)
        return year_str, month_str

    # logical_date 기준 직전 달 계산
    if logical_date.tzinfo is None:
        logical_date = logical_date.replace(tzinfo=timezone.utc)

    first_day_of_current_month = logical_date.replace(day=1)
    prev_month_date = first_day_of_current_month - timedelta(days=1)

    year_str = prev_month_date.strftime("%Y")
    month_str = prev_month_date.strftime("%m")
    logger.info("자동 계산 대상 연월 (직전 달): year=%s, month=%s", year_str, month_str)
    return year_str, month_str


# 직전 달부터 몇 달까지 거슬러 보며 찾을지. TLC 지연은 두 달 안팎이지만 공백이
# 길어질 수 있어 여유를 둡니다. 상한이 없으면 원본이 통째로 사라졌을 때 수십 번
# HEAD 를 던지게 됩니다.
MAX_MONTH_LOOKBACK = 6


def already_collected(base_dir: str, year_month: str) -> bool:
    """그 달 Bronze 파티션에 파일이 이미 있는지."""
    partition = Path(base_dir) / "hvfhv" / f"year_month={year_month}"
    return partition.is_dir() and any(partition.glob("*.parquet"))


def resolve_collectable_year_month(
    logical_date: datetime, params: dict, base_dir: str, is_available=None
) -> tuple[str, str] | None:
    """**아직 안 받았고 TLC 에 올라와 있는** 가장 최신 달을 고릅니다.

    직전 달을 그대로 쓰면 안 됩니다 — TLC 는 두 달쯤 늦게 공개해서 그 파일은
    존재한 적이 없습니다(#345). "2개월 전" 같은 상수도 지연 폭이 일정하지 않아
    다시 틀립니다. 그래서 있는지 물어보고 정합니다.

    이미 받은 달을 건너뛰는 이유는 따로 있습니다. "있는 것 중 최신" 만 보면 새
    달이 공개될 때까지 매달 같은 수백 MB 를 다시 받아 파티션에 파일만 쌓입니다.

    새로 받을 달이 없으면 `None` 입니다. 실패가 아닙니다 — 아직 공개되지 않은
    것은 오류가 아니고, 다음 달에 다시 보면 됩니다.
    """
    if params.get("year") and params.get("month"):
        # 수동 지정은 존중합니다. 백필은 이미 받은 달을 다시 받는 것이 목적입니다.
        return resolve_target_year_month(logical_date, params)

    if is_available is None:
        is_available = importlib.import_module(
            "lambda.functions.hvfhv_raw_to_bronze.extractor"
        ).is_available

    if logical_date.tzinfo is None:
        logical_date = logical_date.replace(tzinfo=timezone.utc)

    cursor = logical_date.replace(day=1)
    for _ in range(MAX_MONTH_LOOKBACK):
        cursor = (cursor - timedelta(days=1)).replace(day=1)
        year_str, month_str = cursor.strftime("%Y"), cursor.strftime("%m")
        year_month = f"{year_str}-{month_str}"

        if already_collected(base_dir, year_month):
            logger.info("이미 수집한 달입니다: %s", year_month)
            continue
        if is_available(year_str, month_str):
            logger.info("수집 대상 연월: %s", year_month)
            return year_str, month_str

    logger.info(
        "새로 받을 달이 없습니다 (최근 %d개월 확인). 다음 실행에서 다시 봅니다.",
        MAX_MONTH_LOOKBACK,
    )
    return None


default_args = {
    "owner": "DE_team1",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=30),
    "on_failure_callback": slack_failure_callback,
}


@dag(
    dag_id="hvfhv_raw_to_silver_pipeline",
    default_args=default_args,
    description="HVFHV 트립 데이터 Raw -> Bronze -> Silver 수집 및 클렌징 파이프라인",
    schedule="0 0 10 * *",  # 매월 10일 00:00 UTC 실행
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["hvfhv", "bronze", "silver", "spark", "lambda"],
    params={
        "year": Param(
            None,
            type=["string", "null"],
            description="수동 수집 연도 (예: '2024'). 비워두면 실행일 기준 직전 달 자동 계산",
        ),
        "month": Param(
            None,
            type=["string", "null"],
            description="수동 수집 월 (예: '03' 또는 '3'). 비워두면 실행일 기준 직전 달 자동 계산",
        ),
        "base_dir": Param(
            DEFAULT_BRONZE_DIR,
            type="string",
            description="Bronze 데이터 저장 기본 경로",
        ),
    },
)
def hvfhv_raw_to_silver_pipeline():
    @task(task_id="raw_to_bronze")
    def raw_to_bronze_task(**context) -> dict:
        """Lambda 함수(lambda/functions/hvfhv)를 호출하여 HVFHV 데이터를 Bronze 레이어에 저장합니다."""
        logical_date = context.get("logical_date") or context.get("data_interval_start") or datetime.now(timezone.utc)
        params = context.get("params", {})

        base_dir = params.get("base_dir") or DEFAULT_BRONZE_DIR
        target = resolve_collectable_year_month(logical_date, params, base_dir)
        if target is None:
            raise AirflowSkipException(
                "TLC 에 새로 공개된 달이 없습니다. 다음 실행에서 다시 확인합니다."
            )
        year_str, month_str = target

        event = {
            "year": year_str,
            "month": month_str,
            "base_dir": base_dir,
        }

        logger.info("raw_to_bronze 작업 시작: event=%s", event)
        # `lambda/functions/` 아래의 **디렉터리 이름**입니다. 데이터셋 이름("hvfhv")을
        # 넘기면 import 가 실패합니다 (#322).
        result = lambda_handler_for("hvfhv_raw_to_bronze")(event=event)
        logger.info("raw_to_bronze 작업 완료: result=%s", result)
        return result

    @task(
        task_id="validate_bronze",
        retries=1,
        retry_delay=timedelta(minutes=10),
        on_failure_callback=slack_failure_callback,
    )
    def validate_bronze_task(result: dict, **context) -> None:
        """파일 경계를 확인한 뒤 Bronze 데이터 품질을 GX로 검증합니다."""
        parsed = parse_handler_result(
            result, expected_locations=1, expected_rows=1
        )
        year_month = parse_year_month(result.get("year_month"), field="year_month")
        path = parsed.locations[0]
        if not path.is_file() or path.stat().st_size != result["file_size_bytes"]:
            raise ValueError(f"Bronze 파일이 없거나 크기가 다릅니다: {path}")

        expected_partition = f"year_month={year_month}"
        if path.parent.name != expected_partition:
            raise ValueError(
                f"파티션이 year_month와 다릅니다: {path.parent.name} != {expected_partition}"
            )

        loader = importlib.import_module("lambda.functions.hvfhv_raw_to_bronze.loader")
        transformer = importlib.import_module("jobs.bronze_to_silver.hvfhv.transformer")
        base_dir = context.get("params", {}).get("base_dir") or DEFAULT_BRONZE_DIR
        try:
            collected_at = datetime.strptime(path.stem, "%Y%m%dT%H%M%SZ").replace(
                tzinfo=timezone.utc
            )
        except ValueError as exc:
            raise ValueError(f"Bronze 파일명이 수집시각 형식과 다릅니다: {path.name}") from exc
        expected_path = (
            loader.HvfhvBronzeLoader(base_dir, year_month, collected_at)
            .partition_path()
            / path.name
        )
        if path.resolve() != expected_path.resolve():
            raise ValueError(
                f"Bronze 경로가 layout 규칙과 다릅니다: {path} != {expected_path}"
            )
        try:
            parquet_file = pq.ParquetFile(path)
        except (OSError, pa.ArrowInvalid) as exc:
            raise ValueError(f"Parquet 을 읽지 못했습니다 (다운로드가 잘렸을 수 있음): {path}") from exc

        summary = _bronze_quality_summary(
            parquet_file,
            loader.SCHEMA,
            transformer.REQUIRED_COLUMNS,
        )
        import great_expectations as gx

        expectations = [
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="row_count", min_value=1
            ),
            gx.expectations.ExpectColumnValuesToBeInSet(
                column="schema_signature",
                # 2024-12 이전 원본에는 `cbd_congestion_fee` 가 없습니다. 두 벌을
                # 다 받아 그 달들도 백필할 수 있게 합니다 (#324).
                value_set=[
                    _schema_signature(loader.SCHEMA),
                    _schema_signature(loader.LEGACY_SCHEMA),
                ],
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

    # Spark 클렌징 실행 태스크 (spark/jobs/bronze_to_silver/hvfhv/job.py)
    # BashOperator를 사용하여 spark python 스크립트 실행
    # job.py는 year/month 대신 year_month range만 받아서, 한 달만 처리할 땐 start=end로 넘긴다.
    TARGET_YEAR_MONTH = (
        "{{ task_instance.xcom_pull(task_ids='raw_to_bronze')['year'] }}"
        "-{{ task_instance.xcom_pull(task_ids='raw_to_bronze')['month'] }}"
    )
    bronze_to_silver_task = BashOperator(
        task_id="bronze_to_silver",
        bash_command=(
            f"python {PROJECT_ROOT}/spark/jobs/bronze_to_silver/hvfhv/job.py "
            f"--input_path {DEFAULT_BRONZE_DIR}/hvfhv "
            f"--output_path {DEFAULT_SILVER_DIR} "
            f"--zone_lookup_path {DEFAULT_ZONE_LOOKUP_PATH} "
            f"--error_threshold {HVFHV_ERROR_THRESHOLD} "
            f"--start_year_month \"{TARGET_YEAR_MONTH}\" "
            f"--end_year_month \"{TARGET_YEAR_MONTH}\""
        ),
        env={
            **os.environ,
            "PYTHONPATH": (
                f"{PROJECT_ROOT}:{PROJECT_ROOT}/spark"
                f":{PROJECT_ROOT}/libs/pipeline_core:{os.getenv('PYTHONPATH', '')}"
            ),
        },
    )

    @task(
        task_id="validate_silver",
        retries=1,
        retry_delay=timedelta(minutes=10),
        on_failure_callback=slack_failure_callback,
    )
    def validate_silver_task(raw_result: dict) -> None:
        """BashOperator 라 handler 결과 dict 가 없어, Silver 파티션을 직접 열어서 확인합니다."""
        parsed = parse_handler_result(
            raw_result, expected_locations=1, expected_rows=1
        )
        year_month = parse_year_month(raw_result.get("year_month"), field="year_month")
        # job.py 는 파티션 내 최신 파일 1개만 골라 Spark 에 넘긴다 (재시도로 남은 옛 파일은 안 씀).
        bronze_rows = pq.ParquetFile(parsed.locations[0]).metadata.num_rows

        silver_partition = Path(DEFAULT_SILVER_DIR) / f"year_month={year_month}"
        silver_files = sorted(silver_partition.glob("*.parquet"))
        if not silver_files:
            raise ValueError(f"Silver 파티션에 Parquet 파일이 없습니다: {silver_partition}")

        transformer = importlib.import_module("jobs.bronze_to_silver.hvfhv.transformer")
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
            raise ValueError(f"Silver 행 수가 Bronze 보다 많습니다: {silver_rows} > {bronze_rows}")

        # 다른 달 파티션이 이미 있는데 직전 달만 없으면 #165(정적 overwrite로 다른 달을 지움) 재발입니다.
        other_partitions = [
            p for p in Path(DEFAULT_SILVER_DIR).glob("year_month=*")
            if p.name != silver_partition.name
        ]
        if other_partitions:
            prev_first_day = datetime.strptime(year_month, "%Y-%m").replace(day=1) - timedelta(days=1)
            prev_partition = Path(DEFAULT_SILVER_DIR) / f"year_month={prev_first_day:%Y-%m}"
            if not any(prev_partition.glob("*.parquet")):
                raise ValueError(f"직전 달 파티션이 사라졌습니다 (#165 재발): {prev_partition}")

    # 태스크 의존성 연결: Bronze 검증을 통과해야 Spark 가 돌고, Spark 결과도 검증한다
    raw_result = raw_to_bronze_task()
    bronze_checked = validate_bronze_task(raw_result)
    bronze_checked >> bronze_to_silver_task

    silver_checked = validate_silver_task(raw_result)
    bronze_to_silver_task >> silver_checked


# DAG 인스턴스 생성
hvfhv_dag = hvfhv_raw_to_silver_pipeline()
