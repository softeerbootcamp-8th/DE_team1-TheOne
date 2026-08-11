"""뉴욕주 전기차 충전소 원문을 매일 Bronze JSON으로 적재합니다.

NLR API 키 설정:
1. https://developer.nlr.gov/signup/ 에서 API 키를 발급받습니다.
2. Airflow UI의 Admin > Variables에서 다음 Variable을 등록합니다.
   - Key: NLR_API_KEY
   - Value: 발급받은 API 키
"""

import importlib
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.sdk import Variable, dag, task

logger = logging.getLogger(__name__)

try:
    from common.slack_failure_callback import slack_failure_callback
except Exception as exc:
    logger.warning("Slack 실패 콜백을 불러오지 못했습니다: %s", exc)

    def slack_failure_callback(context):
        task = context.get("task_instance")
        logger.error("Task 실패: %s", task.task_id if task else "unknown")


CURRENT_DIR = Path(__file__).resolve().parent
AIRFLOW_DIR = CURRENT_DIR.parent
CONTAINER_ROOT = Path("/opt/airflow/project-root")
PROJECT_ROOT = CONTAINER_ROOT if CONTAINER_ROOT.exists() else AIRFLOW_DIR.parent

for path in (PROJECT_ROOT, PROJECT_ROOT / "libs" / "pipeline_core"):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

BRONZE_DIR = str(PROJECT_ROOT / "data" / "bronze")


def lambda_handler_for(function_name: str):
    module = importlib.import_module(f"lambda.functions.{function_name}.handler")
    return module.lambda_handler


default_args = {
    "owner": "DE_team1",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=15),
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
    @task(task_id="raw_to_bronze")
    def raw_to_bronze_task() -> dict:
        api_key = os.getenv("NLR_API_KEY") or Variable.get(
            "NLR_API_KEY", default=None
        )
        if not api_key:
            raise ValueError("Airflow Variable 또는 환경변수 NLR_API_KEY가 필요합니다.")
        os.environ["NLR_API_KEY"] = api_key

        result = lambda_handler_for("ev_charging_stations_raw_to_bronze")(
            event={"base_dir": BRONZE_DIR}
        )
        logger.info("Raw -> Bronze 완료: %s", result)
        return result

    raw_to_bronze_task()


ev_charging_price_raw_to_bronze_dag = ev_charging_price_raw_to_bronze_pipeline()
