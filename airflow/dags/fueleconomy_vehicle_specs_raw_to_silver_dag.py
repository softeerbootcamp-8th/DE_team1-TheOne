"""fueleconomy.gov 차종별 제원 Raw -> Bronze -> Silver 연 1회 파이프라인.

EPA/DOE 벌크 CSV 전량을 Bronze 에 원본 그대로 적재하고, 조인 키와 연비/전비를
정제해 Silver 로 변환합니다. 두 단계 모두 lambda/functions 의 핸들러를 그대로
호출합니다.

제원 자체는 바뀌지 않고 신규 차종이 추가될 뿐이라 1년에 한 번만 돕니다.
매 실행은 전량 스냅샷을 새 파티션에 씁니다.

주의: `catchup=False` + 연 1회 스케줄이라 배포 직후에는 실행되지 않습니다.
다음 스케줄이 1년 뒤이므로, 처음 한 번은 Airflow UI 에서 수동 트리거하세요.

이미 적재된 Bronze 를 다시 변환하려면 수동 트리거하면서 `collected_date`
파라미터에 대상 수집일(예: "2026-01-01")을 넣으세요.
"""

import importlib
import logging
import os
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

DEFAULT_BRONZE_DIR = os.getenv("BRONZE_DIR", str(PROJECT_ROOT / "data" / "bronze"))
DEFAULT_SILVER_DIR = os.getenv("SILVER_DIR", str(PROJECT_ROOT / "data" / "silver"))


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
    dag_id="fueleconomy_vehicle_specs_raw_to_silver_pipeline",
    default_args=default_args,
    description="fueleconomy.gov 차종별 제원 Raw -> Bronze -> Silver 연 1회 파이프라인",
    schedule="0 4 1 1 *",  # 매년 1월 1일 04:00 UTC
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["vehicle_specs", "raw", "bronze", "silver", "lambda"],
    params={
        "collected_date": Param(
            None,
            type=["null", "string"],
            pattern=r"^\d{4}-\d{2}-\d{2}$",
            description=(
                "이미 적재된 Bronze 를 다시 변환할 때만 지정 (예: '2026-01-01'). "
                "비워두면 이번 실행이 적재한 수집일을 그대로 씁니다."
            ),
        ),
        "bronze_dir": Param(
            DEFAULT_BRONZE_DIR,
            type="string",
            description="Bronze 데이터 저장 기본 경로",
        ),
        "silver_dir": Param(
            DEFAULT_SILVER_DIR,
            type="string",
            description="Silver 데이터 저장 기본 경로",
        ),
    },
)
def fueleconomy_vehicle_specs_raw_to_silver_pipeline():
    @task(task_id="raw_to_bronze")
    def raw_to_bronze_task(**context) -> dict:
        """벌크 CSV 를 받아 원본 컬럼 그대로 Bronze 에 적재합니다."""
        params = context.get("params", {})
        result = lambda_handler_for("fueleconomy_vehicle_specs_raw_to_bronze")(
            event={"base_dir": params.get("bronze_dir") or DEFAULT_BRONZE_DIR}
        )
        logger.info("Raw -> Bronze 완료: %s", result)
        return result

    @task(task_id="bronze_to_silver")
    def bronze_to_silver_task(raw_result: dict, **context) -> dict:
        """Bronze 제원의 조인 키와 연비/전비를 정제해 Silver 로 적재합니다."""
        params = context.get("params", {})
        # Bronze 핸들러는 실행 시각으로 파티션을 정하므로 DAG 가 수집일을 따로
        # 계산하면 자정 근처에서 어긋납니다. Bronze 가 알려준 값을 그대로 씁니다.
        collected_date = (params.get("collected_date") or "").strip() or raw_result[
            "collected_date"
        ]

        result = lambda_handler_for("fueleconomy_vehicle_specs_bronze_to_silver")(
            event={
                "collected_date": collected_date,
                "bronze_dir": params.get("bronze_dir") or DEFAULT_BRONZE_DIR,
                "silver_dir": params.get("silver_dir") or DEFAULT_SILVER_DIR,
            }
        )
        logger.info("Bronze -> Silver 완료: %s", result)
        return result

    bronze_to_silver_task(raw_to_bronze_task())


fueleconomy_vehicle_specs_dag = fueleconomy_vehicle_specs_raw_to_silver_pipeline()
