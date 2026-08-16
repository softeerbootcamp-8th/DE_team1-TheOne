"""월별 HVFHV 기사 배정 Silver 파이프라인을 선언합니다."""

import os
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Param, dag

from common.slack_failure_callback import (
    slack_failure_callback,
    slack_retry_alert_callback,
)
from scripts.hvfhv_driver_trip_silver.tasks import (
    DEFAULT_PATHS,
    ROOT,
    validate_inputs_task,
    validate_silver_task,
)


default_args = {
    "owner": "DE_team1",
    "retries": 1,
    "retry_delay": timedelta(minutes=30),
    "on_retry_callback": slack_retry_alert_callback,
    "on_failure_callback": slack_failure_callback,
}


@dag(
    dag_id="hvfhv_driver_trip_silver_pipeline",
    default_args=default_args,
    schedule="0 1 12 * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["hvfhv", "driver", "silver", "spark"],
    params={
        "year": Param(None, type=["string", "null"]),
        "month": Param(None, type=["string", "null"]),
        "snapshot_date": Param(None, type=["string", "null"]),
        "seed": Param(42, type="integer"),
        **{name: Param(path, type="string") for name, path in DEFAULT_PATHS.items()},
    },
)
def driver_trip_pipeline():
    build = BashOperator(
        task_id="build_driver_trip_silver",
        bash_command=(
            f"python {ROOT}/spark/jobs/driver_assignment/silver_job.py "
            + "--trips_path {{ params.trips_path }}/year_month="
            + "{{ task_instance.xcom_pull(task_ids='validate_inputs')['year_month'] }} "
            + "--preferences_path {{ params.preferences_path }} "
            + "--customers_path {{ params.company_path }}/snapshot_date="
            + "{{ task_instance.xcom_pull(task_ids='validate_inputs')['snapshot_date'] }}/customer.parquet "
            + "--leases_path {{ params.company_path }}/snapshot_date="
            + "{{ task_instance.xcom_pull(task_ids='validate_inputs')['snapshot_date'] }}/lease_contract.parquet "
            + "--taxis_path {{ params.company_path }}/snapshot_date="
            + "{{ task_instance.xcom_pull(task_ids='validate_inputs')['snapshot_date'] }}/taxi.parquet "
            + "--travel_times_path {{ params.travel_times_path }} "
            + "--output_path {{ params.output_path }}"
            + " --year_month {{ task_instance.xcom_pull(task_ids='validate_inputs')['year_month'] }}"
            + " --snapshot_date {{ task_instance.xcom_pull(task_ids='validate_inputs')['snapshot_date'] }}"
            + " --seed {{ params.seed }}"
        ),
        env={
            **os.environ,
            "PYTHONPATH": (
                f"{ROOT}:{ROOT}/spark:{ROOT}/libs/pipeline_core"
                f":{os.getenv('PYTHONPATH', '')}"
            ),
        },
    )

    result = validate_inputs_task()
    result >> build >> validate_silver_task()


hvfhv_driver_trip_silver_dag = driver_trip_pipeline()
