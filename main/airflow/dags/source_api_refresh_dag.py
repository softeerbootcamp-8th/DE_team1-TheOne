"""원천 API 3종의 latest 변경을 독립적으로 감시하고 적재 DAG를 실행합니다."""

import os
from datetime import datetime, timedelta

from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import Param, dag

from main.airflow.scripts.source_api_refresh.tasks import (
    check_and_should_refresh_task,
    mark_processed_task,
    publish_api_refresh_ready_task,
)
from shared.airflow.common.slack_failure_callback import (
    slack_failure_callback,
    slack_retry_alert_callback,
)


# 원천 API 서버(EC2) 주소. 로컬 개발은 SOURCE_API_URL 로 덮어씁니다.
DEFAULT_API_BASE_URL = "http://10.0.10.81:8091"
SOURCES = (
    ("monthly_taxi_trip", "hvfhv_raw_to_silver_pipeline"),
    (
        "driver_vehicle_monthly_snapshot",
        "driver_vehicle_monthly_snapshot_raw_to_silver_pipeline",
    ),
    ("lease_vehicle_inventory", "lease_vehicle_inventory_raw_to_silver_pipeline"),
)

default_args = {
    "owner": "DE_team1",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "on_retry_callback": slack_retry_alert_callback,
    "on_failure_callback": slack_failure_callback,
}


@dag(
    dag_id="source_api_refresh_pipeline",
    default_args=default_args,
    description="원천 API 3종 독립 HEAD 감시 및 Raw→Silver 조정",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    render_template_as_native_obj=True,
    tags=["main", "source-api", "silver", "monitor"],
    params={
        "year": Param(None, type=["string", "null"], pattern=r"^\d{4}$"),
        "month": Param(
            None,
            type=["string", "null"],
            pattern=r"^(0?[1-9]|1[0-2])$",
        ),
        "api_base_url": Param(
            os.getenv("SOURCE_API_URL", DEFAULT_API_BASE_URL),
            type="string",
        ),
        "request_timeout": Param(30, type="integer", minimum=1),
        "dry_run": Param(
            False,
            type="boolean",
            description="실제 적재와 상태/Asset 발행 없이 실행 흐름을 검증",
        ),
    },
)
def source_api_refresh_pipeline():
    check_task_ids = []
    completed = []
    for dataset, dag_id in SOURCES:
        gate_task_id = f"check_and_should_refresh_{dataset}"
        check_task_ids.append(gate_task_id)
        refresh = check_and_should_refresh_task.override(task_id=gate_task_id)(
            dataset
        )
        trigger = TriggerDagRunOperator(
            task_id=f"trigger_{dataset}",
            trigger_dag_id=dag_id,
            trigger_run_id=(
                f"source_refresh__{dataset}__"
                f"{{{{ ti.xcom_pull(task_ids='{gate_task_id}')['year_month'] }}}}__"
                f"{{{{ ti.xcom_pull(task_ids='{gate_task_id}')['version'] }}}}"
                "{% if params.dry_run %}__dry_run__{{ run_id }}{% endif %}"
            ),
            conf={
                "year": (
                    f"{{{{ ti.xcom_pull(task_ids='{gate_task_id}')['year'] "
                    "| tojson }}"
                ),
                "month": (
                    f"{{{{ ti.xcom_pull(task_ids='{gate_task_id}')['month'] "
                    "| tojson }}"
                ),
                "api_base_url": (
                    f"{{{{ ti.xcom_pull(task_ids='{gate_task_id}')['api_base_url'] "
                    "| tojson }}"
                ),
                "dry_run": "{{ params.dry_run }}",
            },
            reset_dag_run=True,
            wait_for_completion=True,
            deferrable=True,
            poke_interval=30,
        )
        processed = mark_processed_task.override(
            task_id=f"mark_processed_{dataset}"
        )(refresh)
        refresh >> trigger >> processed
        completed.append(processed)

    ready = publish_api_refresh_ready_task(check_task_ids)
    for processed in completed:
        processed >> ready


source_api_refresh_dag = source_api_refresh_pipeline()
