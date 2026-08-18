"""EIA 주간 휘발유 원본을 Bronze 에 적재합니다.

파일 하나에 이력이 통째로 들어 있어 매일 받을 이유가 없습니다. 월 1회 갱신하는 것은
EIA 가 **과거 값을 개정**하기 때문입니다 — 최신 개정분을 주기적으로 확보합니다.
"""

from datetime import datetime, timedelta

from airflow.sdk import Param, dag

from shared.airflow.common.slack_failure_callback import (
    slack_failure_callback,
    slack_retry_alert_callback,
)
from main.airflow.scripts.eia_gas_price_raw_to_bronze.tasks import (
    BRONZE_DIR,
    raw_to_bronze_task,
    validate_bronze_task,
)


default_args = {
    "owner": "DE_team1",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "on_retry_callback": slack_retry_alert_callback,
    "on_failure_callback": slack_failure_callback,
}


@dag(
    dag_id="eia_gas_price_raw_to_bronze_pipeline",
    default_args=default_args,
    schedule="0 5 1 * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["fuel", "eia", "gas"],
    params={"bronze_dir": Param(BRONZE_DIR, type="string")},
)
def eia_gas_price_raw_to_bronze_pipeline():
    # 수집은 남의 사이트가 잠깐 죽는 것에 대비해 넉넉히 재시도하고, 검증은 재시도하지
    # 않습니다 — 같은 파일을 다시 봐도 결과가 같습니다.
    raw_result = raw_to_bronze_task.override(
        retries=2,
        retry_delay=timedelta(minutes=5),
        retry_exponential_backoff=True,
    )()
    validate_bronze_task.override(retries=0)(raw_result)


eia_gas_price_raw_to_bronze_dag = eia_gas_price_raw_to_bronze_pipeline()
