"""뉴욕주 전기차 충전소 원문을 매일 Bronze JSON으로 적재합니다.

NLR API 키 설정:
1. https://developer.nlr.gov/signup/ 에서 API 키를 발급받습니다.
2. Airflow UI의 Admin > Variables에서 다음 Variable을 등록합니다.
   - Key: NLR_API_KEY
   - Value: 발급받은 API 키
"""

from datetime import datetime, timedelta, timezone

from airflow.sdk import dag
from common.slack_failure_callback import (
    slack_failure_callback,
    slack_retry_alert_callback,
)
from scripts.ev_charging_price_raw_to_bronze.tasks import (
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
    dag_id="ev_charging_price_raw_to_bronze_pipeline",
    default_args=default_args,
    description="뉴욕주 전기차 충전소 일별 Raw -> Bronze 파이프라인",
    schedule="0 9 * * *",
    start_date=datetime(2026, 8, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["ev_charging", "raw", "bronze", "lambda"],
)
def ev_charging_price_raw_to_bronze_pipeline():
    validate_bronze_task(raw_to_bronze_task())


ev_charging_price_raw_to_bronze_dag = ev_charging_price_raw_to_bronze_pipeline()
