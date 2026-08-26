"""휘발유·전력 CLEAN Silver 두 개를 대상 월의 통합 연료비 Silver 로 붙입니다.

산출물은 `gas_ev_price/year_month=YYYY-MM/input_version=<상류조합>/fuel.parquet` — Gold 가 읽는
자리입니다. 출처는 `price_source` 로 남깁니다.

Bronze 원본을 직접 읽지 않습니다 — 정제(주간·월간 원본을 일별로 펼치는 일)는 각 원천의
`*_bronze_to_silver` DAG 가 이미 끝냈습니다(#512, #517). 이 DAG 는 날짜로 붙이는
일만 합니다 (#518).

스케줄
-----
지정이 없으면 전력 공개 지연(약 3개월)만큼 물러선 달을 채웁니다 — 두 CLEAN 중 전력이
늦게 나오므로 그쪽에 맞춥니다. 원천 정제 DAG 두 개(01:00, 02:00 UTC)의 완료를
ExternalTaskSensor 로 기다린 뒤 03:00 에 통합합니다 — 스케줄 오프셋만으로는 상류가
재시도로 늦어질 때 빈 Silver 로 실패합니다.
"""

from datetime import datetime, timedelta

from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.sdk import Param, dag

from main.airflow.common.assets import (
    DEFAULT_SERVICE_AREA,
    MAX_ACTIVE_SERVICE_AREA_RUNS,
)
from main.airflow.scripts.eia_fuel_price_silver.tasks import (
    check_clean_silver_task,
    combine_silver_task,
    validate_silver_task,
)
from shared.airflow.common.slack_failure_callback import (
    slack_failure_callback,
    slack_retry_alert_callback,
)


default_args = {
    "owner": "DE_team1",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "on_retry_callback": slack_retry_alert_callback,
    "on_failure_callback": slack_failure_callback,
}


def _upstream_logical_date(hour: int):
    """같은 달 실행의 상류 논리 날짜로 바꿉니다.

    세 DAG 모두 매월 1일에 실행되지만 시각이 달라(01·02·03시 UTC) 논리 실행일이
    정확히 일치하지 않습니다. 상류 실행 시각으로 시간을 맞춰 같은 달 실행을
    기다립니다.
    """

    def _map(logical_date):
        return logical_date.replace(hour=hour, minute=0, second=0, microsecond=0)

    return _map


def _wait_for(external_dag_id: str, hour: int, task_id: str) -> ExternalTaskSensor:
    return ExternalTaskSensor(
        task_id=task_id,
        external_dag_id=external_dag_id,
        external_task_ids=["validate_silver"],
        allowed_states=["success"],
        execution_date_fn=_upstream_logical_date(hour),
        check_existence=True,
        # 월간 DAG 라 worker 를 점유하지 않도록 reschedule 모드로 폴링합니다.
        mode="reschedule",
        poll_interval=300,
        timeout=60 * 60 * 6,
    )


@dag(
    dag_id="eia_fuel_price_silver_pipeline",
    default_args=default_args,
    # 원천 정제(01:00, 02:00 UTC)가 끝난 뒤 통합합니다.
    schedule="0 3 1 * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=MAX_ACTIVE_SERVICE_AREA_RUNS,
    tags=["main", "fuel", "eia", "silver"],
    params={
        # 비우면 전력 공개 지연(약 3개월)만큼 물러선 달을 채웁니다.
        "year_month": Param(None, type=["string", "null"]),
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
def eia_fuel_price_silver_pipeline():
    # 통합만 재시도합니다. 확인·검증은 파일을 다시 봐도 결과가 같아서 재시도가
    # 실패를 늦추기만 합니다.
    (
        [
            _wait_for(
                "eia_gas_price_raw_to_silver_pipeline", 1, "wait_gas_silver"
            ),
            _wait_for(
                "eia_electricity_price_raw_to_silver_pipeline",
                2,
                "wait_electricity_silver",
            ),
        ]
        >> check_clean_silver_task.override(retries=0)()
        >> combine_silver_task.override(retries=1, retry_delay=timedelta(minutes=10))()
        >> validate_silver_task.override(retries=0)()
    )


eia_fuel_price_silver_dag = eia_fuel_price_silver_pipeline()
