"""Lyft 배차 가능 차량 Source → Raw → Curated 주간 DAG."""

from datetime import datetime, timedelta, timezone

from airflow.sdk import Param, dag

from shared.airflow.common.slack_failure_callback import (
    slack_failure_callback,
    slack_retry_alert_callback,
)
from sub.airflow.scripts.lyft_eligible_vehicles_raw_to_curated.tasks import (
    DEFAULT_CITY_SLUG,
    DEFAULT_CURATED_DIR,
    DEFAULT_RAW_DIR,
    raw_to_curated_task,
    source_to_raw_task,
    validate_raw_task,
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
    dag_id="lyft_eligible_vehicles_raw_to_curated_pipeline",
    default_args=default_args,
    description="Lyft 배차 가능 차량 목록 Source -> Raw -> Curated 수집 및 정제 파이프라인",
    schedule="0 4 * * 1",
    start_date=datetime(2026, 8, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["sub", "lyft_eligible_vehicles", "raw", "curated", "lambda"],
    params={
        "collected_date": Param(
            None,
            type=["null", "string"],
            pattern=r"^\d{4}-\d{2}-\d{2}$",
            description=(
                "수집·변환에 쓸 일자 (예: '2026-08-11'). 지정하면 크롤링은 지금 하되 "
                "파티션과 행의 collected_at 이 그 일자로 적재됩니다. "
                "비워두면 실행 시각을 씁니다."
            ),
        ),
        "city_slug": Param(
            DEFAULT_CITY_SLUG,
            type="string",
            description="Lyft 자격 페이지의 도시 슬러그 (예: 'new-york')",
        ),
        "raw_dir": Param(
            DEFAULT_RAW_DIR,
            type="string",
            description="Raw 데이터 저장 기본 경로",
        ),
        "curated_dir": Param(
            DEFAULT_CURATED_DIR,
            type="string",
            description="Curated 데이터 저장 기본 경로",
        ),
    },
)
def lyft_eligible_vehicles_raw_to_curated_pipeline():
    raw_result = source_to_raw_task.override(
        retries=2,
        retry_delay=timedelta(minutes=5),
        retry_exponential_backoff=True,
    )()
    raw_checked = validate_raw_task.override(retries=0)(raw_result)
    curated_result = raw_to_curated_task(raw_result)
    raw_checked >> curated_result
    validate_curated_task.override(retries=0)(curated_result)


lyft_eligible_vehicles_dag = lyft_eligible_vehicles_raw_to_curated_pipeline()
