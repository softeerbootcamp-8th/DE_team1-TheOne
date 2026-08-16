"""Gas·EV 일별 Bronze를 월별 개별·통합 Silver로 변환합니다.

정기 실행은 매월 1일에 직전 완료 월을 처리합니다. 과거 월을 다시 처리하려면
DAG를 수동 실행하면서 ``collected_month``에 ``YYYY-MM``을 입력하세요.
"""

from datetime import datetime, timedelta, timezone

from airflow.sdk import Param, dag
from common.slack_failure_callback import (
    slack_failure_callback,
    slack_retry_alert_callback,
)
from scripts.gas_ev_price_bronze_to_silver.tasks import (
    check_month_completeness_task,
    ev_bronze_to_silver_task,
    gas_bronze_to_silver_task,
    integrate_silver_task,
    validate_ev_silver_task,
    validate_gas_silver_task,
    validate_integrated_silver_task,
)


default_args = {
    "owner": "DE_team1",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=15),
    "on_retry_callback": slack_retry_alert_callback,
    "on_failure_callback": slack_failure_callback,
}


@dag(
    dag_id="gas_ev_price_bronze_to_silver_pipeline",
    default_args=default_args,
    description="Gas·EV 월별 Bronze -> 개별·통합 Silver 파이프라인",
    schedule="0 10 1 * *",
    start_date=datetime(2026, 8, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["gas_price", "ev_charging", "bronze", "silver", "lambda"],
    params={
        "collected_month": Param(
            None,
            type=["null", "string"],
            pattern=r"^\d{4}-(0[1-9]|1[0-2])$",
            description="처리할 Bronze 수집월(YYYY-MM). 비우면 직전 완료 월입니다.",
        ),
        "require_complete_month": Param(
            1,
            type="integer",
            enum=[0, 1],
            description="1이면 일별 Bronze가 모두 있어야 하며, 0이면 부분 월을 허용합니다.",
        ),
    },
)
def gas_ev_price_bronze_to_silver_pipeline():
    completeness_checked = check_month_completeness_task()
    gas_result = gas_bronze_to_silver_task()
    gas_validated = validate_gas_silver_task(gas_result)
    ev_result = ev_bronze_to_silver_task()
    ev_validated = validate_ev_silver_task(ev_result)
    completeness_checked >> [gas_result, ev_result]
    integrated_result = integrate_silver_task(gas_result, ev_result)
    [gas_validated, ev_validated] >> integrated_result
    validate_integrated_silver_task(integrated_result)


gas_ev_price_bronze_to_silver_dag = gas_ev_price_bronze_to_silver_pipeline()
