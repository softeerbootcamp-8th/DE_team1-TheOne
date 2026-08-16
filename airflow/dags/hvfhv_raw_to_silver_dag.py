"""HVFHV Raw → Bronze → Silver 파이프라인을 선언합니다."""

import os
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Param, dag

from common.slack_failure_callback import (
    slack_failure_callback,
    slack_retry_alert_callback,
)
from scripts.hvfhv_raw_to_silver.tasks import (
    DEFAULT_BRONZE_DIR,
    DEFAULT_SILVER_DIR,
    DEFAULT_ZONE_LOOKUP_PATH,
    HVFHV_ERROR_THRESHOLD,
    PROJECT_ROOT,
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
    dag_id="hvfhv_raw_to_silver_pipeline",
    default_args=default_args,
    description="HVFHV 트립 데이터 Raw -> Bronze -> Silver 수집 및 클렌징 파이프라인",
    schedule="0 0 10 * *",
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
    target_year_month = (
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
            f"--start_year_month \"{target_year_month}\" "
            f"--end_year_month \"{target_year_month}\""
        ),
        env={
            **os.environ,
            "PYTHONPATH": (
                f"{PROJECT_ROOT}:{PROJECT_ROOT}/spark"
                f":{PROJECT_ROOT}/libs/pipeline_core:"
                f"{os.getenv('PYTHONPATH', '')}"
            ),
        },
    )

    raw_result = raw_to_bronze_task()
    bronze_checked = validate_bronze_task(raw_result)
    bronze_checked >> bronze_to_silver_task

    silver_checked = validate_silver_task(raw_result)
    bronze_to_silver_task >> silver_checked


hvfhv_dag = hvfhv_raw_to_silver_pipeline()
