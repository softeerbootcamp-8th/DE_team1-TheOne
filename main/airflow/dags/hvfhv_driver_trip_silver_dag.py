"""월별 기사 운행 이력 Silver 파이프라인을 선언합니다.

HVFHV Clean Silver 와 기사 리스 Clean Silver 를 `taxi_id` + 운행 시점으로 조인합니다.
후보 생성·배정은 여기 없습니다 — 가짜 데이터 API 가 운행마다 `taxi_id` 를 붙여
내보내므로(#450), 이 DAG 은 결정적인 조인만 합니다.
"""

import os
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Param, dag

from shared.airflow.common.slack_failure_callback import (
    slack_failure_callback,
    slack_retry_alert_callback,
)
from main.airflow.scripts.hvfhv_driver_trip_silver.tasks import (
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
        "year": Param(None, type=["string", "null"], pattern=r"^\d{4}$"),
        "month": Param(None, type=["string", "null"], pattern=r"^(0?[1-9]|1[0-2])$"),
        **{name: Param(path, type="string") for name, path in DEFAULT_PATHS.items()},
    },
)
def driver_trip_pipeline():
    build = BashOperator(
        task_id="build_driver_trip_silver",
        bash_command=(
            f"python {ROOT}/main/spark/jobs/driver_trip/job.py "
            + "--trips_path {{ params.trips_path }}/year_month="
            + "{{ task_instance.xcom_pull(task_ids='validate_inputs')['year_month'] }} "
            + "--leases_path {{ params.leases_path }}/year_month="
            + "{{ task_instance.xcom_pull(task_ids='validate_inputs')['year_month'] }} "
            + "--output_path {{ params.output_path }}"
            + " --year_month {{ task_instance.xcom_pull(task_ids='validate_inputs')['year_month'] }}"
        ),
        env={
            **os.environ,
            "PYTHONPATH": (
                f"{ROOT}:{ROOT}/main/spark:{ROOT}/libs/pipeline_core"
                f":{os.getenv('PYTHONPATH', '')}"
            ),
        },
    )

    result = validate_inputs_task.override(retries=0)()
    result >> build >> validate_silver_task.override(retries=0)()


hvfhv_driver_trip_silver_dag = driver_trip_pipeline()
