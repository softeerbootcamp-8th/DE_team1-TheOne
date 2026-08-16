"""뉴욕주 정규 휘발유 가격을 매일 수집해 Bronze JSON으로 적재합니다."""

from datetime import datetime, timedelta, timezone

from airflow.sdk import dag
from common.slack_failure_callback import (
    slack_failure_callback,
    slack_retry_alert_callback,
)
from scripts.gas_price_raw_to_bronze.tasks import (
    raw_to_bronze_task,
    validate_bronze_task,
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
    dag_id="gas_price_raw_to_bronze_pipeline",
    default_args=default_args,
    description="뉴욕주 정규 휘발유 가격 일별 Raw -> Bronze 파이프라인",
    schedule="0 9 * * *",
    start_date=datetime(2026, 8, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["gas_price", "raw", "bronze", "lambda"],
)
def gas_price_raw_to_bronze_pipeline():
    validate_bronze_task(raw_to_bronze_task())


gas_price_raw_to_bronze_dag = gas_price_raw_to_bronze_pipeline()
