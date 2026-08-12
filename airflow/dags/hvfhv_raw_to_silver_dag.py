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

import pyarrow.parquet as pq

try:
    from airflow.sdk import Param, dag, task
except ImportError:
    from airflow.decorators import dag, task
    from airflow.models.param import Param

try:
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:
    from airflow.operators.bash import BashOperator

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

        year_str, month_str = resolve_target_year_month(logical_date, params)
        base_dir = params.get("base_dir") or DEFAULT_BRONZE_DIR

        event = {
            "year": year_str,
            "month": month_str,
            "base_dir": base_dir,
        }

        logger.info("raw_to_bronze 작업 시작: event=%s", event)
        result = lambda_handler_for("hvfhv")(event=event)
        logger.info("raw_to_bronze 작업 완료: result=%s", result)
        return result

    @task(
        task_id="validate_bronze",
        retries=1,
        retry_delay=timedelta(minutes=10),
        on_failure_callback=slack_failure_callback,
    )
    def validate_bronze_task(result: dict) -> None:
        """다운로드가 잘려도 파일 자체는 생기므로, 크기/스키마/행 수를 직접 열어서 봅니다."""
        path = Path(result["locations"][0])
        if not path.is_file() or path.stat().st_size != result["file_size_bytes"]:
            raise ValueError(f"Bronze 파일이 없거나 크기가 다릅니다: {path}")

        expected_partition = f"year_month={result['year_month']}"
        if path.parent.name != expected_partition:
            raise ValueError(
                f"파티션이 year_month와 다릅니다: {path.parent.name} != {expected_partition}"
            )

        loader = importlib.import_module("lambda.functions.hvfhv_raw_to_bronze.loader")
        try:
            table = pq.ParquetFile(path).read()
        except Exception as exc:
            raise ValueError(f"Parquet 을 읽지 못했습니다 (다운로드가 잘렸을 수 있음): {path}") from exc
        if table.schema != loader.SCHEMA:
            raise ValueError(f"Bronze 스키마가 loader.SCHEMA 와 다릅니다: {path}")
        if table.num_rows == 0:
            raise ValueError(f"Bronze 행 수가 0입니다: {path}")

    # Spark 클렌징 실행 태스크 (spark/jobs/bronze_to_silver/hvfhv/job.py)
    # BashOperator를 사용하여 spark python 스크립트 실행
    bronze_to_silver_task = BashOperator(
        task_id="bronze_to_silver",
        bash_command=(
            f"python {PROJECT_ROOT}/spark/jobs/bronze_to_silver/hvfhv/job.py "
            f"--input_path {DEFAULT_BRONZE_DIR}/hvfhv "
            f"--output_path {DEFAULT_SILVER_DIR} "
            f"--zone_lookup_path {DEFAULT_ZONE_LOOKUP_PATH} "
            f"--error_threshold 0.2 "
            f"--year \"{{{{ task_instance.xcom_pull(task_ids='raw_to_bronze')['year'] }}}}\" "
            f"--month \"{{{{ task_instance.xcom_pull(task_ids='raw_to_bronze')['month'] }}}}\""
        ),
        env={
            **os.environ,
            "PYTHONPATH": f"{PROJECT_ROOT}:{PROJECT_ROOT}/spark:{os.getenv('PYTHONPATH', '')}",
        },
    )

    # 태스크 의존성 연결: Bronze 검증을 통과해야 Spark 가 돈다
    bronze_checked = validate_bronze_task(raw_to_bronze_task())
    bronze_checked >> bronze_to_silver_task


# DAG 인스턴스 생성
hvfhv_dag = hvfhv_raw_to_silver_pipeline()
