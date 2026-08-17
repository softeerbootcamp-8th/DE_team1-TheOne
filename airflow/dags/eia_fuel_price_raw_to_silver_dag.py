"""EIA 공개 통계로 대상 월의 연료비 Silver 를 만듭니다.

크롤링(`gas_ev_price_bronze_to_silver`)이 오늘 값만 모아 앞으로를 쌓는다면, 이 DAG 는
이력 파일에서 **지정한 달**을 꺼내 채웁니다. 두 경로가 같은 Silver 자리에 쓰고,
`price_source` 로 어느 쪽이 만든 달인지 구분합니다.

스케줄이 월 1회인 이유
--------------------
EIA 파일 하나에 이력이 통째로 들어 있어 매일 받을 이유가 없습니다. 또 전력 통계가
약 3개월 늦게 공개되므로, 지정 없이 돌 때는 그만큼 물러선 달을 채웁니다.
과거 달을 채우려면 `year_month` 로 직접 지정하세요.
"""

from datetime import datetime, timedelta

from airflow.sdk import Param, dag

from common.slack_failure_callback import (
    slack_failure_callback,
    slack_retry_alert_callback,
)
from scripts.eia_fuel_price_raw_to_silver.tasks import (
    BRONZE_DIR,
    SILVER_DIR,
    bronze_to_silver_task,
    collect_bronze_task,
    validate_silver_task,
)


default_args = {
    "owner": "DE_team1",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "on_retry_callback": slack_retry_alert_callback,
    "on_failure_callback": slack_failure_callback,
}


@dag(
    dag_id="eia_fuel_price_raw_to_silver_pipeline",
    default_args=default_args,
    # 크롤링 통합(매월 1일 10:00)과 같은 날 뒤에 둡니다. 겹치는 달이 생겨도 나중에
    # 실행한 쪽이 남으므로, 순서를 고정해 두어야 어느 쪽이 남는지 예측 가능합니다.
    schedule="0 11 1 * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["fuel", "eia", "silver"],
    params={
        # 비우면 전력 공개 지연(약 3개월)만큼 물러선 달을 채웁니다.
        "year_month": Param(None, type=["string", "null"]),
        # 공공 충전 마진 배수. EIA 는 전력 소매요금이라 충전 단가로 쓰려면 보정이
        # 필요합니다 — 근거는 lambda 쪽 transformer 주석 참고.
        "markup": Param(2.0, type="number"),
        "bronze_dir": Param(BRONZE_DIR, type="string"),
        "silver_dir": Param(SILVER_DIR, type="string"),
    },
)
def eia_fuel_price_raw_to_silver_pipeline():
    collect_bronze_task() >> bronze_to_silver_task() >> validate_silver_task()


eia_fuel_price_raw_to_silver_dag = eia_fuel_price_raw_to_silver_pipeline()
