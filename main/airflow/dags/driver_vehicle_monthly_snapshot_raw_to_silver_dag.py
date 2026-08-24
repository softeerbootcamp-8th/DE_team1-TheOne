"""기사 차량 월별 스냅샷 Raw → Bronze → Silver 파이프라인을 선언합니다."""

import os
from datetime import datetime, timedelta

from airflow.sdk import Param, dag

from shared.airflow.common.lambda_remote import (
    JsonLambdaInvokeFunctionOperator,
    templated_json_payload,
)
from shared.airflow.common.slack_failure_callback import (
    slack_failure_callback,
    slack_retry_alert_callback,
)
from main.airflow.common.assets import (
    DEFAULT_SERVICE_AREA,
    MAX_ACTIVE_SERVICE_AREA_RUNS,
)
from main.airflow.scripts.driver_vehicle_monthly_snapshot_raw_to_silver.tasks import (
    DEFAULT_API_BASE_URL,
    DEFAULT_BRONZE_DIR,
    DEFAULT_SILVER_DIR,
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
            os.getenv("SOURCE_API_URL", DEFAULT_API_BASE_URL),
            type="string",
        ),
        "base_dir": Param(DEFAULT_BRONZE_DIR, type="string"),
        "silver_dir": Param(DEFAULT_SILVER_DIR, type="string"),
        "service_area": Param(
            DEFAULT_SERVICE_AREA,
            type="string",
            pattern=r"^[A-Z][A-Z0-9_]*$",
            description="대상 지역 코드 (예: NYC). AWS 리전과 무관합니다",
        ),
    },
)
def driver_vehicle_monthly_snapshot_raw_to_silver_pipeline():
    raw = JsonLambdaInvokeFunctionOperator(
        task_id="raw_to_bronze",
        function_name="driver_vehicle_monthly_snapshot_raw_to_bronze",
        aws_conn_id="aws_default",
        invocation_type="RequestResponse",
        payload=templated_json_payload(
            api_base_url="params.api_base_url",
            base_dir="params.base_dir",
            year="params.year",
            month="params.month",
            service_area="params.service_area",
        ),
        retries=2,
        retry_delay=timedelta(minutes=5),
        retry_exponential_backoff=True,
    ).output
    checked = validate_bronze_task.override(retries=0)(raw)
    silver = JsonLambdaInvokeFunctionOperator(
        task_id="bronze_to_silver",
        function_name="driver_vehicle_monthly_snapshot_bronze_to_silver",
        aws_conn_id="aws_default",
        invocation_type="RequestResponse",
        payload=templated_json_payload(
            year_month="task_instance.xcom_pull(task_ids='validate_bronze')['year_month']",
            silver_output_path="task_instance.xcom_pull(task_ids='validate_bronze')['silver_version_path']",
            service_area="params.service_area",
        ),
    ).output
    checked >> silver
    validate_silver_task.override(retries=0)(silver, checked)


driver_vehicle_monthly_snapshot_raw_to_silver_dag = driver_vehicle_monthly_snapshot_raw_to_silver_pipeline()
