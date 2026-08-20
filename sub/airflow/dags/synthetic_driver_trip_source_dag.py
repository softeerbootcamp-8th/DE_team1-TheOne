"""검증된 월별 HVFHV로 운행·리스·보유 차량 데이터를 생성합니다."""

import os
from datetime import datetime, timedelta, timezone

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Param, dag

from shared.airflow.common.slack_failure_callback import (
    slack_failure_callback,
    slack_retry_alert_callback,
)
from sub.airflow.scripts.synthetic_driver_trip_source.tasks import (
    DEFAULT_PATHS,
    ROOT,
    collect_source_input_task,
    validate_inputs_task,
    validate_release_task,
)

default_args = {
    "owner": "DE_team1",
    "retries": 1,
    "retry_delay": timedelta(minutes=30),
    "on_retry_callback": slack_retry_alert_callback,
    "on_failure_callback": slack_failure_callback,
}


@dag(
    dag_id="synthetic_driver_trip_source_pipeline",
    default_args=default_args,
    description="월별 HVFHV에 기사·차량을 배정해 제공 데이터 3종 생성",
    schedule="0 0 10 * *",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["hvfhv", "driver", "synthetic", "source", "spark"],
    params={
        "year": Param(None, type=["string", "null"], pattern=r"^\d{4}$"),
        "month": Param(None, type=["string", "null"], pattern=r"^(0?[1-9]|1[0-2])$"),
        "seed": Param(42, type="integer"),
        # TEMPORARY(#452): 로컬 DAG smoke test용. 0이면 전체 월을 처리합니다.
        #
        # 0 이 아니면 생성 결과가 `<release_output_dir>/_temporary/test_row_limit=N/`
        # 아래로 갑니다. 가짜 데이터 API 는 그 위 디렉터리만 보므로, 하류 DAG 까지
        # 이어서 돌리려면 API 를 그 경로로 띄워야 합니다 — 안 그러면 404 입니다.
        #
        #   python sub/source_api/server.py --port 8091 \
        #     --root "data/source/synthetic_driver_trip_api/_temporary/test_row_limit=1000"
        "test_row_limit": Param(
            0,
            type="integer",
            minimum=0,
            description=(
                "임시 테스트 입력 행 수(0=전체). 0이 아니면 생성 결과가 "
                "_temporary/test_row_limit=N/ 아래로 가므로, API 를 그 경로를 "
                "--root 로 지정해 띄워야 하류 DAG 가 받을 수 있습니다"
            ),
        ),
        **{name: Param(path, type="string") for name, path in DEFAULT_PATHS.items()},
    },
)
def synthetic_driver_trip_source_pipeline():
    build = BashOperator(
        task_id="build_source_release",
        bash_command=(
            f"python {ROOT}/sub/spark/jobs/driver_assignment/source_job.py "
            + "--hvfhv_input_path "
            + "{{ task_instance.xcom_pull(task_ids='validate_inputs')['hvfhv_input_path'] }} "
            + "--zone_lookup_path "
            + "{{ task_instance.xcom_pull(task_ids='validate_inputs')['zone_lookup_path'] }} "
            + "--previous_snapshot_dir "
            + "{{ task_instance.xcom_pull(task_ids='validate_inputs')['previous_snapshot_dir'] }} "
            + "--previous_preferences_path "
            + "{{ task_instance.xcom_pull(task_ids='validate_inputs')['previous_preferences_path'] }} "
            + "--vehicle_master_path "
            + "{{ task_instance.xcom_pull(task_ids='validate_inputs')['vehicle_master_path'] }} "
            + "--state_output_dir {{ params.state_output_dir }} "
            + "--release_output_dir {{ params.release_output_dir }} "
            + "--year_month "
            + "{{ task_instance.xcom_pull(task_ids='validate_inputs')['year_month'] }} "
            + "--seed {{ params.seed }} "
            + "--test_row_limit {{ params.test_row_limit }}"
        ),
        env={
            **os.environ,
            "PYTHONPATH": (
                f"{ROOT}:{ROOT}/main/spark:{ROOT}/libs/pipeline_core"
                f":{os.getenv('PYTHONPATH', '')}"
            ),
        },
    )

    source = collect_source_input_task.override(
        retries=2,
        retry_delay=timedelta(minutes=5),
        retry_exponential_backoff=True,
    )()
    inputs = validate_inputs_task.override(retries=0)(source)
    inputs >> build >> validate_release_task.override(retries=0)()


synthetic_driver_trip_source_dag = synthetic_driver_trip_source_pipeline()
