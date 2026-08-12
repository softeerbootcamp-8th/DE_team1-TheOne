"""EV Charging 일별 Bronze JSON을 매월 Silver Parquet으로 변환합니다.

정기 실행은 매월 1일에 직전 완료 월을 처리합니다. 과거 월을 다시 처리하려면
DAG를 수동 실행하면서 ``collected_month``에 ``YYYY-MM``을 입력하세요.
"""

import importlib
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.sdk import Param, dag, task
from common.validation import parse_handler_result, parse_year_month, read_parquet

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
SILVER_DIR = str(PROJECT_ROOT / "data" / "silver")


def lambda_handler_for(function_name: str):
    module = importlib.import_module(f"lambda.functions.{function_name}.handler")
    return module.lambda_handler


def previous_month(data_interval_end: datetime) -> str:
    return (data_interval_end.replace(day=1) - timedelta(days=1)).strftime("%Y-%m")


default_args = {
    "owner": "DE_team1",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=15),
    "on_failure_callback": slack_failure_callback,
}


@dag(
    dag_id="ev_charging_price_bronze_to_silver_pipeline",
    default_args=default_args,
    description="뉴욕시 평균 전기 요금 월별 Bronze -> Silver 파이프라인",
    schedule="0 10 1 * *",
    start_date=datetime(2026, 8, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["ev_charging", "bronze", "silver", "lambda"],
    params={
        "collected_month": Param(
            None,
            type=["null", "string"],
            pattern=r"^\d{4}-(0[1-9]|1[0-2])$",
            description="처리할 Bronze 수집월(YYYY-MM). 비우면 직전 완료 월입니다.",
        )
    },
)
def ev_charging_price_bronze_to_silver_pipeline():
    @task(task_id="bronze_to_silver")
    def bronze_to_silver_task(**context) -> dict:
        target_month = context.get("params", {}).get("collected_month")
        if not target_month:
            interval_end = context.get("data_interval_end") or datetime.now(
                timezone.utc
            )
            target_month = previous_month(interval_end)

        result = lambda_handler_for("ev_charging_stations_bronze_to_silver")(
            event={
                "collected_month": target_month,
                "bronze_dir": BRONZE_DIR,
                "silver_dir": SILVER_DIR,
            }
        )
        logger.info("Bronze -> Silver 완료: %s", result)
        return result

    @task(
        task_id="validate_silver",
        retries=1,
        retry_delay=timedelta(minutes=10),
        on_failure_callback=slack_failure_callback,
    )
    def validate_silver_task(result: dict) -> None:
        parsed = parse_handler_result(result, expected_locations=1)
        collected_month = parse_year_month(result.get("collected_month"))

        layout = importlib.import_module(
            "lambda.functions.common.ev_charging_layout"
        )
        path = parsed.locations[0]
        expected = layout.silver_file(SILVER_DIR, collected_month)
        if path.resolve() != expected.resolve():
            raise ValueError(f"적재 경로가 예상과 다릅니다: {path}")
        table = read_parquet(path)
        if table.num_rows != parsed.row_count:
            raise ValueError("Silver 파일 행 수와 Handler row_count가 다릅니다.")
        loader = importlib.import_module(
            "lambda.functions.ev_charging_stations_bronze_to_silver.loader"
        )
        if table.schema != loader.SCHEMA:
            raise ValueError("Silver 스키마가 올바르지 않습니다.")

    validate_silver_task(bronze_to_silver_task())


ev_charging_price_bronze_to_silver_dag = (
    ev_charging_price_bronze_to_silver_pipeline()
)
