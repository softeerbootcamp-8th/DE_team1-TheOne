"""휘발유·전력 CLEAN Silver 두 개를 대상 월의 통합 연료비 Silver 로 붙입니다.

산출물은 `gas_ev_price/year_month=YYYY-MM/gas_ev_price.parquet` — Gold 가 읽는
자리입니다. 출처는 `price_source` 로 남깁니다.

Bronze 원본을 직접 읽지 않습니다 — 정제(주간·월간 원본을 일별로 펼치는 일)는 각 원천의
`*_bronze_to_silver` DAG 가 이미 끝냈습니다(#512, #517). 이 DAG 는 날짜로 붙이는
일만 합니다 (#518).

스케줄
-----
지정이 없으면 전력 공개 지연(약 3개월)만큼 물러선 달을 채웁니다 — 두 CLEAN 중 전력이
늦게 나오므로 그쪽에 맞춥니다. 정제 DAG 두 개가 매월 1일 07시에 돌고 나면 그 결과로
한 달을 만드는 흐름이라 그 뒤에 둡니다.
"""

from datetime import datetime, timedelta

from airflow.sdk import Param, dag

from main.airflow.scripts.eia_fuel_price_silver.tasks import (
    SILVER_DIR,
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


@dag(
    dag_id="eia_fuel_price_silver_pipeline",
    default_args=default_args,
    # 정제 두 개(07시)가 끝난 뒤에 돕니다.
    schedule="0 8 1 * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["main", "fuel", "eia", "silver"],
    params={
        # 비우면 전력 공개 지연(약 3개월)만큼 물러선 달을 채웁니다.
        "year_month": Param(None, type=["string", "null"]),
        "silver_dir": Param(SILVER_DIR, type="string"),
    },
)
def eia_fuel_price_silver_pipeline():
    # 통합만 재시도합니다. 확인·검증은 파일을 다시 봐도 결과가 같아서 재시도가
    # 실패를 늦추기만 합니다.
    (
        check_clean_silver_task.override(retries=0)()
        >> combine_silver_task.override(retries=1, retry_delay=timedelta(minutes=10))()
        >> validate_silver_task.override(retries=0)()
    )


eia_fuel_price_silver_dag = eia_fuel_price_silver_pipeline()
