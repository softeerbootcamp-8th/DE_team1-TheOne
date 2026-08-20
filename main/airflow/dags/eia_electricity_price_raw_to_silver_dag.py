"""EIA 월간 전력요금 원본을 수집하고 대상 월의 일별 충전 단가 Silver로 변환합니다."""

from datetime import datetime, timedelta

from airflow.sdk import Param, dag

from main.airflow.scripts.eia_electricity_price_bronze_to_silver.tasks import (
    SILVER_DIR,
    bronze_to_silver_task,
    validate_silver_task,
)
from main.airflow.scripts.eia_electricity_price_raw_to_bronze.tasks import (
    BRONZE_DIR,
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
    dag_id="eia_electricity_price_raw_to_silver_pipeline",
    default_args=default_args,
    schedule="0 6 1 * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["fuel", "eia", "ev", "silver"],
    params={
        "year": Param(None, type=["string", "null"], pattern=r"^\d{4}$"),
        "month": Param(
            None,
            type=["string", "null"],
            pattern=r"^(0?[1-9]|1[0-2])$",
        ),
        "markup": Param(2.0, type="number"),
        "bronze_dir": Param(BRONZE_DIR, type="string"),
        "silver_dir": Param(SILVER_DIR, type="string"),
    },
)
def eia_electricity_price_raw_to_silver_pipeline():
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


eia_electricity_price_raw_to_silver_dag = (
    eia_electricity_price_raw_to_silver_pipeline()
)
