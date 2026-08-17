"""EIA 원본 두 개를 대상 월의 통합 연료비 Silver 로 변환합니다.

크롤링 쪽 통합(`gas_ev_price_bronze_to_silver`)과 **같은 자리**에 씁니다 —
`gas_ev_price/collected_month=YYYY-MM/`. 구분은 `price_source` 로 합니다.

스케줄이 없는 이유
----------------
EIA 파일에는 이력이 통째로 들어 있어 **어느 달이든** 만들 수 있습니다. 그래서 자동
주기보다 "필요한 달을 지정해 돌리는" 편이 맞습니다.

자동 스케줄을 두면 문제가 생기기도 합니다 — 지연 오프셋 때문에 시간이 지나면 EIA 가
크롤링이 **일별 실측으로 만든 달을 따라잡아 덮어씁니다**(주간·월간을 편 값으로).
크롤링을 유지할지 EIA 로 일원화할지 정해지기 전까지는 자동으로 덮어쓰지 않게 둡니다.
"""

from datetime import datetime, timedelta

from airflow.sdk import Param, dag

from common.slack_failure_callback import (
    slack_failure_callback,
    slack_retry_alert_callback,
)
from scripts.eia_fuel_price_bronze_to_silver.tasks import (
    BRONZE_DIR,
    SILVER_DIR,
    bronze_to_silver_task,
    check_bronze_task,
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
    dag_id="eia_fuel_price_bronze_to_silver_pipeline",
    default_args=default_args,
    schedule=None,
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
def eia_fuel_price_bronze_to_silver_pipeline():
    check_bronze_task() >> bronze_to_silver_task() >> validate_silver_task()


eia_fuel_price_bronze_to_silver_dag = eia_fuel_price_bronze_to_silver_pipeline()
