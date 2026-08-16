"""fueleconomy.gov 차종별 제원 Raw → Bronze → Silver 월간 DAG."""

from datetime import datetime, timedelta, timezone

from airflow.sdk import Param, dag

from common.slack_failure_callback import (
    slack_failure_callback,
    slack_retry_alert_callback,
)
from scripts.fueleconomy_vehicle_specs_raw_to_silver.tasks import (
    DEFAULT_BRONZE_DIR,
    DEFAULT_SILVER_DIR,
    bronze_to_silver_task,
    raw_to_bronze_task,
    validate_bronze_task,
    validate_silver_task,
)


default_args = {
    "owner": "DE_team1",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=15),
    "execution_timeout": timedelta(minutes=15),
    "on_retry_callback": slack_retry_alert_callback,
    "on_failure_callback": slack_failure_callback,
}


@dag(
    dag_id="fueleconomy_vehicle_specs_raw_to_silver_pipeline",
    default_args=default_args,
    description="fueleconomy.gov 차종별 제원 Raw -> Bronze -> Silver 월 1회 파이프라인",
    schedule="0 4 1 * *",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["vehicle_specs", "raw", "bronze", "silver", "lambda"],
    params={
        "collected_date": Param(
            None,
            type=["null", "string"],
            pattern=r"^\d{4}-\d{2}-\d{2}$",
            description=(
                "이미 적재된 Bronze 를 다시 변환할 때만 지정 (예: '2026-01-01'). "
                "비워두면 이번 실행이 적재한 수집일을 그대로 씁니다."
            ),
        ),
        "bronze_dir": Param(
            DEFAULT_BRONZE_DIR,
            type="string",
            description="Bronze 데이터 저장 기본 경로",
        ),
        "silver_dir": Param(
            DEFAULT_SILVER_DIR,
            type="string",
            description="Silver 데이터 저장 기본 경로",
        ),
    },
)
def fueleconomy_vehicle_specs_raw_to_silver_pipeline():
    raw_result = raw_to_bronze_task.override(
        retries=2,
        retry_delay=timedelta(minutes=5),
        retry_exponential_backoff=True,
    )()
    bronze_checked = validate_bronze_task.override(retries=0)(raw_result)
    silver_result = bronze_to_silver_task(raw_result)
    bronze_checked >> silver_result
    validate_silver_task.override(retries=0)(silver_result)


fueleconomy_vehicle_specs_dag = fueleconomy_vehicle_specs_raw_to_silver_pipeline()
