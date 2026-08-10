"""fueleconomy.gov 차종별 제원 Raw -> Bronze 연 1회 파이프라인.

제원 자체는 바뀌지 않고 신규 차종이 추가될 뿐이라 1년에 한 번만 돕니다.
매 실행은 전량 스냅샷을 새 파티션에 씁니다. Silver 단계는 아직 없습니다.

주의: `catchup=False` + 연 1회 스케줄이라 배포 직후에는 실행되지 않습니다.
다음 스케줄이 1년 뒤이므로, 처음 한 번은 Airflow UI 에서 수동 트리거하세요.
"""

import importlib
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.sdk import dag, task

logger = logging.getLogger(__name__)

try:
    from common.slack_failure_callback import slack_failure_callback
except Exception as exc:
    logger.warning("Slack 실패 콜백을 불러오지 못했습니다: %s", exc)

    def slack_failure_callback(context):
        task_instance = context.get("task_instance")
        logger.error(
            "Task 실패: %s",
            task_instance.task_id if task_instance else "unknown",
        )

CURRENT_DIR = Path(__file__).resolve().parent
AIRFLOW_DIR = CURRENT_DIR.parent
CONTAINER_ROOT = Path("/opt/airflow/project-root")
PROJECT_ROOT = CONTAINER_ROOT if CONTAINER_ROOT.exists() else AIRFLOW_DIR.parent

# Airflow 이미지에는 pipeline-core가 설치돼 있지 않아 경로로 참조(이후 변경 필요)
for path in (PROJECT_ROOT, PROJECT_ROOT / "libs" / "pipeline_core"):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

BRONZE_DIR = str(PROJECT_ROOT / "data" / "bronze")


def lambda_handler_for(function_name: str):
    """`lambda`가 파이썬 예약어라 정적 import가 안 돼 동적으로 불러옵니다."""
    module = importlib.import_module(f"lambda.functions.{function_name}.handler")
    return module.lambda_handler


default_args = {
    "owner": "DE_team1",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=15),
    "execution_timeout": timedelta(minutes=15),
    "on_failure_callback": slack_failure_callback,
}


@dag(
    dag_id="fueleconomy_vehicle_specs_raw_to_bronze_pipeline",
    default_args=default_args,
    description="fueleconomy.gov 차종별 제원 Raw -> Bronze 연 1회 파이프라인",
    schedule="0 4 1 1 *",  # 매년 1월 1일 04:00 UTC
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["vehicle_specs", "raw", "bronze", "lambda"],
)
def fueleconomy_vehicle_specs_raw_to_bronze_pipeline():
    @task(task_id="raw_to_bronze")
    def raw_to_bronze_task() -> dict:
        result = lambda_handler_for("fueleconomy_vehicle_specs")(
            event={"base_dir": BRONZE_DIR}
        )
        logger.info("Raw -> Bronze 완료: %s", result)
        return result

    raw_to_bronze_task()


fueleconomy_vehicle_specs_dag = fueleconomy_vehicle_specs_raw_to_bronze_pipeline()
