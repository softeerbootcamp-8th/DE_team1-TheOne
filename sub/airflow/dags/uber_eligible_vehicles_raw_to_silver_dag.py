"""Uber 배차 가능 차량 Raw → Bronze → Silver 주간 DAG."""

from datetime import datetime, timedelta, timezone

from airflow.sdk import Param, dag

from shared.airflow.common.slack_failure_callback import (
    slack_failure_callback,
    slack_retry_alert_callback,
)
from sub.airflow.scripts.uber_eligible_vehicles_raw_to_silver.tasks import (
    DEFAULT_CITY_SLUG,
    DEFAULT_CURATED_DIR,
    DEFAULT_RAW_DIR,
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
    dag_id="uber_eligible_vehicles_raw_to_silver_pipeline",
    default_args=default_args,
    description="Uber 배차 가능 차량 목록 Raw -> Bronze -> Silver 수집 및 정제 파이프라인",
    schedule="0 5 * * 1",
    start_date=datetime(2026, 8, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["uber_eligible_vehicles", "raw", "bronze", "silver", "lambda"],
    params={
        "collected_date": Param(
            None,
            type=["null", "string"],
            pattern=r"^\d{4}-\d{2}-\d{2}$",
            description=(
                "수집·변환에 쓸 일자 (예: '2026-08-11'). 지정하면 크롤링은 지금 하되 "
                "파티션과 행의 collected_at 이 그 일자로 적재됩니다. "
                "비워두면 실행 시각을 씁니다."
            ),
        ),
        "city_slug": Param(
            DEFAULT_CITY_SLUG,
            type="string",
            description="Uber 자격 페이지의 도시 슬러그 (예: 'new-york')",
        ),
        "bronze_dir": Param(
            DEFAULT_RAW_DIR,
            type="string",
            description="Raw 데이터 저장 기본 경로",
        ),
        "silver_dir": Param(
            DEFAULT_CURATED_DIR,
            type="string",
            description="Curated 데이터 저장 기본 경로",
        ),
    },
)
def uber_eligible_vehicles_raw_to_silver_pipeline():
    raw_result = raw_to_bronze_task.override(
        retries=2,
        retry_delay=timedelta(minutes=5),
        retry_exponential_backoff=True,
    )()
    bronze_checked = validate_bronze_task.override(retries=0)(raw_result)
    silver_result = bronze_to_silver_task(raw_result)
    bronze_checked >> silver_result
    validate_silver_task.override(retries=0)(silver_result)


uber_eligible_vehicles_dag = uber_eligible_vehicles_raw_to_silver_pipeline()
