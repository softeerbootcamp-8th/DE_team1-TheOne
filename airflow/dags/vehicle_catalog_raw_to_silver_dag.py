"""리스 업체 보유 차량 대장 Raw -> Bronze -> Silver 파이프라인 DAG.

렌탈 업체 사이트에서 차량 대장(차종/주간 렌트료)을 수집해 Bronze 에 적재하고,
조인 키를 정규화해 Silver 로 변환합니다. 두 단계 모두 lambda/functions 의
핸들러를 그대로 호출합니다.

수집 대상이 12대뿐이고 업체가 카드 이미지를 새로 올릴 때만 값이 바뀌므로
주 1회로 잡았습니다. 실제 변경 빈도가 관측되면 조정하세요.
"""

import logging
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

try:
    from airflow.sdk import Param, dag, task
except ImportError:
    from airflow.decorators import dag, task
    from airflow.models.param import Param

# 프로젝트 루트 디렉토리를 sys.path에 추가 (컨테이너 /opt/airflow/project-root 및 로컬 호환)
CURRENT_DIR = Path(__file__).resolve().parent
AIRFLOW_DIR = CURRENT_DIR.parent
CONTAINER_ROOT = Path("/opt/airflow/project-root")
PROJECT_ROOT = CONTAINER_ROOT if CONTAINER_ROOT.exists() else AIRFLOW_DIR.parent

for path_str in [str(PROJECT_ROOT), str(PROJECT_ROOT / "lambda")]:
    if path_str not in sys.path:
        sys.path.insert(0, path_str)

logger = logging.getLogger(__name__)

# 슬랙 에러 콜백 임포트 (안전한 Fallback 처리)
try:
    from common.slack_failure_callback import slack_failure_callback
except Exception as e:  # noqa: BLE001
    logger.warning("slack_failure_callback 임포트 실패 (기본 로깅으로 대체): %s", e)

    def slack_failure_callback(context):
        task_instance = context.get("task_instance")
        task_id = task_instance.task_id if task_instance else "unknown"
        logger.error("Task [%s] failed without slack callback.", task_id)


DEFAULT_BRONZE_DIR = os.getenv("BRONZE_DIR", str(PROJECT_ROOT / "data" / "bronze"))
DEFAULT_SILVER_DIR = os.getenv("SILVER_DIR", str(PROJECT_ROOT / "data" / "silver"))

BRONZE_HANDLER = "lambda.functions.fasttrack_vehicle_pricing.handler"
SILVER_HANDLER = "lambda.functions.fasttrack_vehicle_pricing_bronze_to_silver.handler"

# Bronze 적재 경로에서 수집일을 되읽습니다.
COLLECTED_DATE_RE = re.compile(r"collected_date=(\d{4}-\d{2}-\d{2})")


def resolve_collected_date(bronze_result: dict, params: dict) -> str:
    """Silver 변환 대상 수집일을 정합니다.

    Bronze 핸들러는 실행 시각으로 파티션을 정하므로, 수집일을 DAG 가 따로
    계산하면 자정 근처에서 Bronze 가 쓴 파티션과 어긋날 수 있습니다.
    그래서 Bronze 가 실제로 적재한 경로에서 되읽는 것을 기본으로 합니다.
    """
    manual_date = (params.get("collected_date") or "").strip()
    if manual_date:
        logger.info("수동 파라미터 적용: collected_date=%s", manual_date)
        return manual_date

    path = str(bronze_result.get("path") or "")
    matched = COLLECTED_DATE_RE.search(path)
    if not matched:
        raise ValueError(f"Bronze 적재 경로에서 collected_date를 찾지 못했습니다: {path}")

    collected_date = matched.group(1)
    logger.info("Bronze 적재 경로에서 수집일 확인: collected_date=%s", collected_date)
    return collected_date


default_args = {
    "owner": "DE_team1",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=30),
    "on_failure_callback": slack_failure_callback,
}


@dag(
    dag_id="vehicle_catalog_raw_to_silver_pipeline",
    default_args=default_args,
    description="리스 업체 보유 차량 대장 Raw -> Bronze -> Silver 수집 및 정제 파이프라인",
    schedule="0 3 * * 1",  # 매주 월요일 03:00 UTC
    start_date=datetime(2026, 8, 1),
    catchup=False,
    tags=["vehicle-catalog", "bronze", "silver", "lambda"],
    params={
        "collected_date": Param(
            None,
            type=["string", "null"],
            description=(
                "이미 적재된 Bronze 를 다시 변환할 때만 지정 (예: '2026-08-09'). "
                "비워두면 이번 실행이 적재한 Bronze 경로에서 자동으로 읽습니다."
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
def vehicle_catalog_raw_to_silver_pipeline():
    @task(task_id="raw_to_bronze")
    def raw_to_bronze_task(**context) -> dict:
        """렌탈 업체 사이트를 수집해 Bronze 에 적재합니다."""
        import importlib

        lambda_handler = importlib.import_module(BRONZE_HANDLER).lambda_handler
        params = context.get("params", {})
        event = {"base_dir": params.get("bronze_dir") or DEFAULT_BRONZE_DIR}

        logger.info("raw_to_bronze 작업 시작: event=%s", event)
        result = lambda_handler(event=event)
        logger.info("raw_to_bronze 작업 완료: result=%s", result)
        return result

    @task(task_id="bronze_to_silver")
    def bronze_to_silver_task(bronze_result: dict, **context) -> dict:
        """Bronze 차량 대장의 조인 키를 정규화해 Silver 로 적재합니다."""
        import importlib

        lambda_handler = importlib.import_module(SILVER_HANDLER).lambda_handler
        params = context.get("params", {})
        event = {
            "collected_date": resolve_collected_date(bronze_result, params),
            "bronze_dir": params.get("bronze_dir") or DEFAULT_BRONZE_DIR,
            "silver_dir": params.get("silver_dir") or DEFAULT_SILVER_DIR,
        }

        logger.info("bronze_to_silver 작업 시작: event=%s", event)
        result = lambda_handler(event=event)
        logger.info("bronze_to_silver 작업 완료: result=%s", result)
        return result

    bronze_to_silver_task(raw_to_bronze_task())


# DAG 인스턴스 생성
vehicle_catalog_dag = vehicle_catalog_raw_to_silver_pipeline()
