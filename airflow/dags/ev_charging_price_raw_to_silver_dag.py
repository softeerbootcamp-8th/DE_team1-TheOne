"""NYC 전기차 충전 요금 Raw -> Bronze -> Silver 일일 파이프라인.

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
from datetime import date, datetime, timedelta, timezone
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

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

BRONZE_DIR = str(PROJECT_ROOT / "data" / "bronze")
SILVER_DIR = str(PROJECT_ROOT / "data" / "silver")
PARTITION_PREFIX = "collected_date="


def collected_date_from_path(path: str) -> str:
    """Bronze 파일의 Hive 파티션에서 수집일을 반환합니다."""
    partition = Path(path).parent.name
    if not partition.startswith(PARTITION_PREFIX):
        raise ValueError(f"Bronze 경로에 수집일 파티션이 없습니다: {path}")

    collected_date = partition.removeprefix(PARTITION_PREFIX)
    try:
        date.fromisoformat(collected_date)
    except ValueError as exc:
        raise ValueError(f"유효하지 않은 수집일 파티션입니다: {partition}") from exc
    return collected_date


default_args = {
    "owner": "DE_team1",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=15),
    "on_failure_callback": slack_failure_callback,
}


@dag(
    dag_id="ev_charging_price_raw_to_silver_pipeline",
    default_args=default_args,
    description="NYC 전기차 충전 요금 Raw -> Bronze -> Silver 일일 파이프라인",
    schedule="0 9 * * *",
    start_date=datetime(2026, 8, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["ev_charging", "raw", "bronze", "silver", "lambda"],
)
def ev_charging_price_raw_to_silver_pipeline():
    @task(task_id="raw_to_bronze")
    def raw_to_bronze_task() -> dict:
        api_key = os.getenv("NLR_API_KEY") or Variable.get("NLR_API_KEY", default=None)
        if not api_key:
            raise ValueError("Airflow Variable 또는 환경변수 NLR_API_KEY가 필요합니다.")
        os.environ["NLR_API_KEY"] = api_key

        handler = importlib.import_module(
            "lambda.functions.ev_charging_stations.handler"
        ).lambda_handler
        result = handler(event={"base_dir": BRONZE_DIR})
        logger.info("Raw -> Bronze 완료: %s", result)
        return result

    @task(task_id="bronze_to_silver")
    def bronze_to_silver_task(raw_result: dict) -> dict:
        collected_date = collected_date_from_path(raw_result["path"])
        handler = importlib.import_module(
            "lambda.functions.ev_charging_stations_bronze_to_silver.handler"
        ).lambda_handler
        result = handler(
            event={
                "collected_date": collected_date,
                "bronze_dir": BRONZE_DIR,
                "silver_dir": SILVER_DIR,
            }
        )
        logger.info("Bronze -> Silver 완료: %s", result)
        return result

    bronze_to_silver_task(raw_to_bronze_task())


ev_charging_price_dag = ev_charging_price_raw_to_silver_pipeline()
