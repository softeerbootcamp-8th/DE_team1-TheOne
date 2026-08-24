"""원천 API 3종의 latest 변경을 묶어 Gold 준비 Asset을 한 번만 냅니다."""

import os
from datetime import datetime, timedelta

from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import Param, dag

from main.airflow.common.assets import (
    DEFAULT_SERVICE_AREA,
    MAX_ACTIVE_SERVICE_AREA_RUNS,
)
from main.airflow.scripts.source_api_refresh.tasks import (
    check_and_should_refresh_task,
    mark_processed_task,
    publish_api_refresh_ready_task,
)
from shared.airflow.common.slack_failure_callback import (
    slack_failure_callback,
    slack_retry_alert_callback,
)


DEFAULT_API_BASE_URL = "http://10.0.10.81:8091"
SOURCES = (
    ("monthly_taxi_trip", "monthly_taxi_trip_raw_to_silver_pipeline"),
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
    max_active_runs=MAX_ACTIVE_SERVICE_AREA_RUNS,
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
        # 대상 지역. Airflow asset 파티션 키가 "{service_area}:{year_month}" 복합
        # 문자열이라 이 값이 키의 앞부분이 됩니다(#674). 지금은 NYC 하나뿐이라
        # 기본값으로 두고, 지역이 늘면 트리거 시 지정합니다.
        #
        # 새 파라미터를 추가하면 test_main_dag_params.py의 기대 집합도 함께
        # 고쳐야 합니다 — 그 테스트가 파라미터 집합 완전일치를 요구합니다.
        "service_area": Param(
            DEFAULT_SERVICE_AREA,
            type="string",
            pattern=r"^[A-Z][A-Z0-9_]*$",
            description="대상 지역 코드 (예: NYC). AWS 리전과 무관합니다",
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
            # 지역을 run_id 에 넣습니다. version 은 (year_month, etag, last_modified)
            # 해시라, 두 지역이 같은 원천에서 같은 응답을 받으면 run_id 가 겹치고
            # reset_dag_run=True 때문에 **한 지역이 다른 지역의 DagRun 을 리셋**합니다
            # (#674). 값은 params 가 아니라 xcom 에서 꺼내 실제로 상태 조회에 쓴
            # 지역과 어긋나지 않게 합니다.
            trigger_run_id=(
                f"source_refresh__{dataset}__"
                f"{{{{ ti.xcom_pull(task_ids='{gate_task_id}')['service_area'] }}}}__"
                f"{{{{ ti.xcom_pull(task_ids='{gate_task_id}')['year_month'] }}}}__"
                f"{{{{ ti.xcom_pull(task_ids='{gate_task_id}')['version'] }}}}"
            ),
            conf={
                "year": (
                    f"{{{{ ti.xcom_pull(task_ids='{gate_task_id}')['year'] }}}}"
                ),
                "month": (
                    f"{{{{ ti.xcom_pull(task_ids='{gate_task_id}')['month'] }}}}"
                ),
                "api_base_url": (
                    f"{{{{ ti.xcom_pull(task_ids='{gate_task_id}')['api_base_url'] }}}}"
                ),
                "service_area": (
                    f"{{{{ ti.xcom_pull(task_ids='{gate_task_id}')['service_area'] }}}}"
                ),
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
