"""뉴욕주 정규 휘발유 가격 Raw -> Bronze -> Silver 일일 파이프라인.

정기 실행은 그날 수집분(collected_date) 하나만 정제합니다. 월 전체를 다시 읽으면
과거 파티션의 깨진 파일 하나가 그 달 내내 배치를 막기 때문입니다.

과거 데이터를 고친 뒤 다시 정제하려면 이 DAG 를 수동 트리거하면서
`backfill_collected_month` 파라미터에 대상 월(예: "2026-08")을 넣으세요.
그 달의 수집 파티션 전체를 다시 정제합니다.
"""

import importlib
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.sdk import Param, dag, task

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
SILVER_DIR = str(PROJECT_ROOT / "data" / "silver")


def lambda_handler_for(function_name: str):
    """`lambda`가 파이썬 예약어라 정적 import가 안 돼 동적으로 불러옵니다."""
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
    dag_id="gas_price_raw_to_silver_pipeline",
    default_args=default_args,
    description="뉴욕주 정규 휘발유 가격 Raw -> Bronze -> Silver 일일 파이프라인",
    schedule="0 9 * * *",
    start_date=datetime(2026, 8, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["gas_price", "raw", "bronze", "silver", "lambda"],
    params={
        # 수동 트리거로 과거를 다시 정제할 때만 씁니다 (예: "2026-08").
        # 비워두면 정기 실행 = 그날 수집분만 처리합니다.
        "backfill_collected_month": Param(
            None,
            type=["null", "string"],
            pattern=r"^\d{4}-(0[1-9]|1[0-2])$",
            description="백필 대상 수집월(YYYY-MM). 비우면 당일 수집분만 처리합니다.",
        ),
    },
)
def gas_price_raw_to_silver_pipeline():
    @task(task_id="raw_to_bronze")
    def raw_to_bronze_task() -> dict:
        result = lambda_handler_for("gas_price_raw_to_bronze")(
            event={"base_dir": BRONZE_DIR}
        )
        logger.info("Raw -> Bronze 완료: %s", result)
        return result

    @task(task_id="bronze_to_silver")
    def bronze_to_silver_task(raw_result: dict, **context) -> dict:
        backfill_month = context.get("params", {}).get("backfill_collected_month")
        event = {"bronze_dir": BRONZE_DIR, "silver_dir": SILVER_DIR}
        if backfill_month:
            # 백필은 그 달 전체가 대상이라 특정 날짜 반영 여부를 검증하지 않습니다.
            logger.info("백필 모드: %s 수집분 전체를 다시 정제합니다.", backfill_month)
            event["collected_month"] = backfill_month
        else:
            event["collected_date"] = raw_result["collected_date"]
            event["expect_price_date"] = raw_result["price_date"]

        result = lambda_handler_for("gas_price_bronze_to_silver")(event=event)
        logger.info("Bronze -> Silver 완료: %s", result)
        return result

    bronze_to_silver_task(raw_to_bronze_task())


gas_price_dag = gas_price_raw_to_silver_pipeline()
