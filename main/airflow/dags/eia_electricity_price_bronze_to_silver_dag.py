"""EIA 전력 Bronze 를 대상 월의 일별 충전 단가 Silver 로 변환합니다.

산출물은 `eia_electricity_price/year_month=YYYY-MM/eia_electricity_price.parquet`
입니다. 휘발유 CLEAN Silver 와 함께 연료비 통합 단계의 입력이 됩니다.

스케줄
-----
지정이 없으면 전력 공개 지연(약 3개월)만큼 물러선 달을 채웁니다. 수집 DAG 가 매월
1일 06시에 돌고 나면 그 결과로 한 달을 만드는 흐름이라 그 뒤에 둡니다.

EIA 파일에는 이력이 통째로 들어 있어 **어느 달이든** 만들 수 있습니다. 과거를 채울
때는 `year_month` 를 지정해 수동으로 돌리면 됩니다 — 자동 주기가 붙어 있어도 지정
실행이 막히지 않습니다.
"""

from datetime import datetime, timedelta

from airflow.sdk import Param, dag

from main.airflow.scripts.eia_electricity_price_bronze_to_silver.tasks import (
    BRONZE_DIR,
    SILVER_DIR,
    bronze_to_silver_task,
    check_bronze_task,
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
    dag_id="eia_electricity_price_bronze_to_silver_pipeline",
    default_args=default_args,
    # 전력 수집(06시)이 끝난 뒤에 돕니다.
    schedule="0 7 1 * *",
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
def eia_electricity_price_bronze_to_silver_pipeline():
    # 변환만 재시도합니다. 확인·검증은 파일을 다시 봐도 결과가 같아서 재시도가
    # 실패를 늦추기만 합니다.
    (
        check_bronze_task.override(retries=0)()
        >> bronze_to_silver_task.override(retries=1, retry_delay=timedelta(minutes=10))()
        >> validate_silver_task.override(retries=0)()
    )


eia_electricity_price_bronze_to_silver_dag = (
    eia_electricity_price_bronze_to_silver_pipeline()
)
