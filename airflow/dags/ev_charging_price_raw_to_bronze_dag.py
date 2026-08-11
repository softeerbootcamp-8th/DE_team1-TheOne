"""뉴욕주 전기차 충전소 원문을 매일 Bronze JSON으로 적재합니다.

NLR API 키 설정:
1. https://developer.nlr.gov/signup/ 에서 API 키를 발급받습니다.
2. Airflow UI의 Admin > Variables에서 다음 Variable을 등록합니다.
   - Key: NLR_API_KEY
   - Value: 발급받은 API 키
"""

import importlib
import json
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
        if isinstance(row_count, bool) or row_count != 1:
            raise ValueError("Bronze row_count는 1이어야 합니다.")
        if (
            not isinstance(locations, list)
            or len(locations) != 1
            or not isinstance(locations[0], str)
            or not locations[0]
        ):
            raise ValueError("locations에는 파일 경로가 하나 있어야 합니다.")
        if not isinstance(collected_date, str):
            raise ValueError("collected_date가 문자열이 아닙니다.")
        try:
            target_date = datetime.strptime(collected_date, "%Y-%m-%d").date()
        except ValueError as exc:
            raise ValueError("collected_date는 YYYY-MM-DD 형식이어야 합니다.") from exc
        if target_date.isoformat() != collected_date:
            raise ValueError("collected_date는 YYYY-MM-DD 형식이어야 합니다.")
        if result.get("state") != "NY" or result.get("fuel_type_code") != "ELEC":
            raise ValueError("Handler의 state 또는 fuel_type_code가 올바르지 않습니다.")

        path = Path(locations[0])
        if not path.is_file():
            raise FileNotFoundError(f"적재 파일이 없습니다: {path}")
        try:
            collected_at = datetime.strptime(path.stem, "%Y%m%dT%H%M%SZ").replace(
                tzinfo=timezone.utc
            )
        except ValueError as exc:
            raise ValueError("Bronze 파일명의 수집시각 형식이 올바르지 않습니다.") from exc

        layout = importlib.import_module(
            "lambda.functions.common.ev_charging_layout"
        )
        expected = layout.bronze_file(BRONZE_DIR, collected_at)
        if path.resolve() != expected.resolve():
            raise ValueError(f"적재 경로가 예상과 다릅니다: {path}")
        if collected_at.date() != target_date:
            raise ValueError("Bronze 파일명과 collected_date가 다릅니다.")

        try:
            payload = json.loads(path.read_bytes())
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError(f"Bronze JSON을 읽지 못했습니다: {path}") from exc
        if not isinstance(payload, dict):
            raise ValueError("Bronze JSON이 객체 형식이 아닙니다.")

        stations = payload.get("fuel_stations")
        total_results = payload.get("total_results")
        if not isinstance(stations, list) or not stations:
            raise ValueError("Bronze fuel_stations가 비어 있습니다.")
        if (
            isinstance(total_results, bool)
            or not isinstance(total_results, int)
            or total_results != len(stations)
        ):
            raise ValueError("Bronze total_results와 충전소 수가 다릅니다.")
        if any(not isinstance(station, dict) for station in stations):
            raise ValueError("Bronze 충전소 데이터가 객체 형식이 아닙니다.")
        if any(
            station.get("state") != "NY"
            or station.get("fuel_type_code") != "ELEC"
            for station in stations
        ):
            raise ValueError("Bronze에 NY 전기차 충전소가 아닌 데이터가 있습니다.")

    validate_bronze_task(raw_to_bronze_task())


ev_charging_price_raw_to_bronze_dag = ev_charging_price_raw_to_bronze_pipeline()
