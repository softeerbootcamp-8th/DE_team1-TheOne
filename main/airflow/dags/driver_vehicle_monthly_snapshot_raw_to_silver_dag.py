"""기사 차량 월별 스냅샷 Raw → Bronze → Silver 파이프라인을 선언합니다."""

import os
from datetime import datetime, timedelta

from airflow.sdk import Param, dag

from shared.airflow.common.slack_failure_callback import (
    slack_failure_callback,
    slack_retry_alert_callback,
)
from main.airflow.common.assets import (
    DEFAULT_SERVICE_AREA,
    MAX_ACTIVE_SERVICE_AREA_RUNS,
)
from main.airflow.scripts.driver_vehicle_monthly_snapshot_raw_to_silver.tasks import (
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
    "on_retry_callback": slack_retry_alert_callback,
    "on_failure_callback": slack_failure_callback,
}


@dag(
    dag_id="driver_vehicle_monthly_snapshot_raw_to_silver_pipeline",
    default_args=default_args,
    description="기사 차량 월별 스냅샷 Raw→Bronze→Silver 파이프라인",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=MAX_ACTIVE_SERVICE_AREA_RUNS,
    tags=["main", "driver", "taxi", "snapshot", "bronze", "silver"],
    params={
        "year": Param(None, type=["string", "null"], pattern=r"^\d{4}$"),
        "month": Param(None, type=["string", "null"], pattern=r"^(0?[1-9]|1[0-2])$"),
        "api_base_url": Param(
            os.environ["SOURCE_API_URL"],
            type="string",
            minLength=1,
        ),
        "service_area": Param(
            DEFAULT_SERVICE_AREA,
            type="string",
            pattern=r"^[A-Z][A-Z0-9_]*$",
            description="대상 지역 코드 (예: NYC). AWS 리전과 무관합니다",
        ),
    },
)
def driver_vehicle_monthly_snapshot_raw_to_silver_pipeline():
    raw = raw_to_bronze_task.override(
        retries=2,
        retry_delay=timedelta(minutes=5),
        retry_exponential_backoff=True,
    )()
    checked = validate_bronze_task.override(retries=0)(raw)
    silver = bronze_to_silver_task(checked)
    validate_silver_task.override(retries=0)(silver, checked)


driver_vehicle_monthly_snapshot_raw_to_silver_dag = driver_vehicle_monthly_snapshot_raw_to_silver_pipeline()
