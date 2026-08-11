"""뉴욕주 정규 휘발유 가격을 매일 수집해 Bronze JSON으로 적재합니다."""

import importlib
import json
import logging
import math
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
    dag_id="gas_price_raw_to_bronze_pipeline",
    default_args=default_args,
    description="뉴욕주 정규 휘발유 가격 일별 Raw -> Bronze 파이프라인",
    schedule="0 9 * * *",
    start_date=datetime(2026, 8, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["gas_price", "raw", "bronze", "lambda"],
)
def gas_price_raw_to_bronze_pipeline():
    @task(task_id="raw_to_bronze")
    def raw_to_bronze_task() -> dict:
        result = lambda_handler_for("gas_price_raw_to_bronze")(
            event={"base_dir": BRONZE_DIR}
        )
        logger.info("Raw -> Bronze 완료: %s", result)
        return result

    @task(
        task_id="validate_bronze",
        retries=1,
        retry_delay=timedelta(minutes=10),
        on_failure_callback=slack_failure_callback,
    )
    def validate_bronze_task(result: dict) -> None:
        if not isinstance(result, dict):
            raise TypeError("Handler 결과가 dict가 아닙니다.")

        row_count = result.get("row_count")
        locations = result.get("locations")
        collected_date = result.get("collected_date")
        if row_count != 1:
            raise ValueError("Bronze row_count는 1이어야 합니다.")
        if not isinstance(locations, list) or len(locations) != 1:
            raise ValueError("locations에는 파일 경로가 하나 있어야 합니다.")
        if not isinstance(collected_date, str):
            raise ValueError("collected_date가 문자열이 아닙니다.")
        try:
            target_date = datetime.strptime(collected_date, "%Y-%m-%d").date()
        except ValueError as exc:
            raise ValueError("collected_date는 YYYY-MM-DD 형식이어야 합니다.") from exc

        layout = importlib.import_module("lambda.functions.common.gas_price_layout")
        path = Path(locations[0])
        expected = layout.bronze_file(BRONZE_DIR, collected_date)
        if path.resolve() != expected.resolve():
            raise ValueError(f"적재 경로가 예상과 다릅니다: {path}")
        if not path.is_file():
            raise FileNotFoundError(f"적재 파일이 없습니다: {path}")

        try:
            record = json.loads(path.read_text(encoding="utf-8"))
            price = float(str(record["price_raw"]).replace("$", "").strip())
            datetime.strptime(str(record["price_date_raw"]), "%m/%d/%y")
            collected_at = datetime.fromisoformat(
                str(record["collected_at"]).replace("Z", "+00:00")
            )
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise ValueError("Bronze JSON 값이 올바르지 않습니다.") from exc
        if record.get("state") != "NY" or record.get("fuel_type") != "regular":
            raise ValueError("Bronze state 또는 fuel_type이 올바르지 않습니다.")
        if not math.isfinite(price) or price <= 0:
            raise ValueError("Bronze 가격은 0보다 커야 합니다.")
        if collected_at.tzinfo is None:
            raise ValueError("Bronze collected_at에 시간대가 없습니다.")
        if collected_at.astimezone(timezone.utc).date() != target_date:
            raise ValueError("Bronze collected_at과 collected_date가 다릅니다.")
        if not str(record.get("source_url") or "").strip():
            raise ValueError("Bronze source_url이 비어 있습니다.")

    validate_bronze_task(raw_to_bronze_task())


gas_price_raw_to_bronze_dag = gas_price_raw_to_bronze_pipeline()
