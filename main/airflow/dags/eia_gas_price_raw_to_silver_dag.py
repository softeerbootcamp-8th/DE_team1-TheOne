"""EIA 주간 휘발유 원본을 수집하고 대상 월의 일별 단가 Silver로 변환합니다."""

from datetime import datetime, timedelta

from airflow.sdk import Param, dag

from main.airflow.common.assets import (
    DEFAULT_SERVICE_AREA,
    MAX_ACTIVE_SERVICE_AREA_RUNS,
)
from main.airflow.scripts.eia_gas_price_bronze_to_silver.tasks import (
    bronze_to_silver_task,
    validate_silver_task,
)
from main.airflow.scripts.eia_gas_price_raw_to_bronze.tasks import (
    raw_to_bronze_task,
    validate_bronze_task,
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


@dag(
    dag_id="eia_gas_price_raw_to_silver_pipeline",
    default_args=default_args,
    schedule="0 1 1 * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=MAX_ACTIVE_SERVICE_AREA_RUNS,
    tags=["main", "fuel", "eia", "gas", "silver"],
    params={
        "year": Param(None, type=["string", "null"], pattern=r"^\d{4}$"),
        "month": Param(
            None,
            type=["string", "null"],
            pattern=r"^(0?[1-9]|1[0-2])$",
        ),
        # 대상 지역. Bronze/Silver S3 경로를 지역별로 나누는 데 씁니다(#843).
        # 지금은 NYC 하나뿐이라 기본값으로 두고, 지역이 늘면 트리거 시 지정합니다.
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
def eia_gas_price_raw_to_silver_pipeline():
    raw_result = raw_to_bronze_task.override(
        retries=2,
        retry_delay=timedelta(minutes=5),
        retry_exponential_backoff=True,
    )()
    bronze_validated = validate_bronze_task.override(retries=0)(raw_result)
    silver_result = bronze_to_silver_task.override(
        retries=1,
        retry_delay=timedelta(minutes=10),
    )()
    silver_validated = validate_silver_task.override(retries=0)()

    bronze_validated >> silver_result >> silver_validated


eia_gas_price_raw_to_silver_dag = eia_gas_price_raw_to_silver_pipeline()
