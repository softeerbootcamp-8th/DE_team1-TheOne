"""차량 대장·제원·Uber/Lyft 자격 Silver를 차량 마스터 Silver로 조립합니다."""

from datetime import datetime, timedelta, timezone

from airflow.sdk import Param, dag

from sub.airflow.common import assets
from shared.airflow.common.slack_failure_callback import (
    slack_failure_callback,
    slack_retry_alert_callback,
)
from sub.airflow.scripts.vehicle_master_silver.tasks import (
    DEFAULT_CURATED_DIR,
    build_vehicle_master_task,
    validate_silver_task,
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
    dag_id="vehicle_master_silver_pipeline",
    default_args=default_args,
    description="차량 대장·제원·배차 자격 Silver 를 합쳐 차량 마스터 Silver 생성",
    schedule=(
        assets.VEHICLE_CATALOG_SILVER
        & assets.UBER_ELIGIBLE_VEHICLES_SILVER
        & assets.LYFT_ELIGIBLE_VEHICLES_SILVER
    )
    | assets.FUELECONOMY_VEHICLE_SPECS_SILVER,
    start_date=datetime(2026, 8, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["sub", "vehicle_master", "silver", "lambda"],
    params={
        "collected_date": Param(
            None,
            type=["null", "string"],
            pattern=r"^\d{4}-\d{2}-\d{2}$",
            description=(
                "이 테이블을 만드는 날짜 (예: '2026-08-13'). 비우면 실행 시각의 "
                "UTC 날짜. 원천은 이 날짜 이하의 최신 파티션에서 각각 읽습니다."
            ),
        ),
        "silver_dir": Param(
            DEFAULT_CURATED_DIR,
            type="string",
            description="Curated 데이터 기본 경로 (읽기·쓰기 모두 여기)",
        ),
    },
)
def vehicle_master_silver_pipeline():
    silver_result = build_vehicle_master_task()
    validate_silver_task.override(retries=0)(silver_result)


vehicle_master_dag = vehicle_master_silver_pipeline()
