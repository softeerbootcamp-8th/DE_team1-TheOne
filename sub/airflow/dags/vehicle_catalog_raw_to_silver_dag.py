"""리스 업체 차량 대장 Raw → Bronze → Silver 주간 DAG."""

from datetime import datetime, timedelta

from airflow.sdk import Param, dag

from shared.airflow.common.slack_failure_callback import (
    slack_failure_callback,
    slack_retry_alert_callback,
)
from sub.airflow.scripts.vehicle_catalog_raw_to_silver.tasks import (
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
    "retry_delay": timedelta(minutes=30),
    "on_retry_callback": slack_retry_alert_callback,
    "on_failure_callback": slack_failure_callback,
}


@dag(
    dag_id="vehicle_catalog_raw_to_silver_pipeline",
    default_args=default_args,
    description="리스 업체 보유 차량 대장 Raw -> Bronze -> Silver 수집 및 정제 파이프라인",
    schedule="0 3 * * 1",
    start_date=datetime(2026, 8, 1),
    catchup=False,
    max_active_runs=1,
    tags=["vehicle_catalog", "raw", "bronze", "silver", "lambda"],
    params={
        "collected_date": Param(
            None,
            type=["null", "string"],
            pattern=r"^\d{4}-\d{2}-\d{2}$",
            description=(
                "이미 적재된 Bronze 를 다시 변환할 때만 지정 (예: '2026-08-09'). "
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
def vehicle_catalog_raw_to_silver_pipeline():
    raw_result = raw_to_bronze_task.override(
        retries=2,
        retry_delay=timedelta(minutes=5),
        retry_exponential_backoff=True,
    )()
    bronze_checked = validate_bronze_task.override(retries=0)(raw_result)
    silver_result = bronze_to_silver_task(raw_result)
    bronze_checked >> silver_result
    validate_silver_task.override(retries=0)(silver_result)


vehicle_catalog_dag = vehicle_catalog_raw_to_silver_pipeline()
