"""차량 대장·제원·Uber/Lyft 자격 Curated 를 차량 마스터 Curated 로 조립합니다."""

from datetime import datetime, timedelta, timezone

from airflow.sdk import Param, dag

from sub.airflow.common import assets
from shared.airflow.common.slack_failure_callback import (
    slack_failure_callback,
    slack_retry_alert_callback,
)
from sub.airflow.scripts.vehicle_master_curated_to_curated.tasks import (
    build_vehicle_master_task,
    validate_curated_task,
)


default_args = {
    "owner": "DE_team1",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=15),
    "execution_timeout": timedelta(minutes=15),
    "on_retry_callback": slack_retry_alert_callback,
    "on_failure_callback": slack_failure_callback,
}


@dag(
    dag_id="vehicle_master_curated_to_curated_pipeline",
    default_args=default_args,
    description="차량 대장·제원·배차 자격 Curated 를 합쳐 차량 마스터 Curated 생성",
    # 원천 4개가 모두 이번 달 것으로 갱신됐을 때만 조립합니다.
    #
    # 예전에는 제원만 OR 로 빼뒀습니다. 나머지 3종이 주간이라 제원을 AND 에 넣으면
    # 전체가 월 1회로 묶여 배차 자격이 최대 3주 묵었기 때문입니다. 이제 4개가 모두
    # 매월 1일이라 그 손해가 없어졌고, OR 로 두면 한 원천만 늦어도 나머지 3개의
    # 지난달 값으로 마스터가 만들어집니다.
    schedule=(
        assets.VEHICLE_CATALOG_CURATED
        & assets.UBER_ELIGIBLE_VEHICLES_CURATED
        & assets.LYFT_ELIGIBLE_VEHICLES_CURATED
        & assets.FUELECONOMY_VEHICLE_SPECS_CURATED
    ),
    start_date=datetime(2026, 8, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["sub", "vehicle_master", "curated", "lambda"],
    params={
        "collected_date": Param(
            None,
            type=["null", "string"],
            pattern=r"^\d{4}-\d{2}-\d{2}$",
            description=(
                "원천을 읽을 **상한** 날짜 (예: '2026-08-13'). 비우면 실행 시각의 "
                "UTC 날짜. 원천은 이 날짜 이하의 최신 파티션에서 각각 읽습니다. "
                "적재 파티션은 이 값이 아니라 읽은 원천의 최신 수집일입니다 — "
                "재시도로 다음 날 조립돼도 같은 파티션에 들어갑니다."
            ),
        ),
    },
)
def vehicle_master_curated_to_curated_pipeline():
    curated_result = build_vehicle_master_task()
    validate_curated_task.override(retries=0)(curated_result)


vehicle_master_dag = vehicle_master_curated_to_curated_pipeline()
