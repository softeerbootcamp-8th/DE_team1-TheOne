"""Uber 배차 가능 차량 목록 Raw -> Bronze -> Silver 파이프라인 DAG.

Uber 자격 안내 페이지를 수집해 Bronze 에 원문 그대로 적재하고, (차종, 상품) 단위로
펼쳐 조인 키를 정규화한 뒤 Silver 로 변환합니다. 두 단계 모두 lambda/functions 의
핸들러를 그대로 호출합니다.

Lyft DAG(`lyft_eligible_vehicles_raw_to_silver_dag`) 와 같은 모양이지만 **별도로
둡니다.** 두 플랫폼은 서로 다른 사이트를 긁고 데이터 의존이 없습니다. 한쪽 사이트가
개편돼 크롤링이 깨져도 다른 쪽은 끝까지 가야 합니다.

    Bronze  ZDX  "2010 (UberX, Comfort) / 2018 (Comfort Electric)"
    Silver  ZDX  UberX             min_year=2010
            ZDX  Comfort           min_year=2010
            ZDX  Comfort Electric  min_year=2018

Gold 조인 관점:
    - Silver 컬럼이 `lyft_eligible_vehicles` 와 동일합니다
      (make_key, model_key, product, min_year). 두 플랫폼을 union 해서 쓸 수 있습니다.
    - 다만 상품명이 겹칩니다. `Black` / `Black SUV` 는 Uber 에도 Lyft 에도 있습니다.
      union 할 때 어느 플랫폼인지 구분할 값을 읽는 쪽에서 채워야 합니다
      (데이터셋 경로가 다르므로 상수로 채울 수 있습니다).
    - 조인 키는 `lambda/functions/common/join_keys.py` 규칙을 따릅니다. 차량 대장과
      반드시 같은 규칙이어야 합니다 — 한쪽만 바뀌면 실패하지 않고 조인이 0건이 됩니다.

자격 기준은 Uber 가 정책을 바꿀 때만 움직여서 주 1회로 잡았습니다. 차량 대장
DAG(03:00) · Lyft DAG(04:00) 와 같은 요일에 두되 뒤로 밀어, 같은 날 파티션에
떨어지면서도 세 크롤링이 겹치지 않게 했습니다.

이미 적재된 Bronze 를 다시 변환하려면 수동 트리거하면서 `collected_date`
파라미터에 대상 수집일(예: "2026-08-11")을 넣으세요.
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
# 자격 페이지가 도시마다 다릅니다. 지금 대상은 뉴욕 하나뿐이지만 값을 박아두지 않고
# 파라미터로 빼서, 다른 도시를 볼 때 코드를 고치지 않고 수동 트리거할 수 있게 둡니다.
DEFAULT_CITY_SLUG = os.getenv("UBER_CITY_SLUG", "new-york")


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
    dag_id="uber_eligible_vehicles_raw_to_silver_pipeline",
    default_args=default_args,
    description="Uber 배차 가능 차량 목록 Raw -> Bronze -> Silver 수집 및 정제 파이프라인",
    schedule="0 5 * * 1",  # 매주 월요일 05:00 UTC (차량 대장 03:00 / Lyft 04:00 다음)
    start_date=datetime(2026, 8, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["uber_eligible_vehicles", "raw", "bronze", "silver", "lambda"],
    params={
        "collected_date": Param(
            None,
            type=["null", "string"],
            pattern=r"^\d{4}-\d{2}-\d{2}$",
            description=(
                "이미 적재된 Bronze 를 다시 변환할 때만 지정 (예: '2026-08-11'). "
                "비워두면 이번 실행이 적재한 수집일을 그대로 씁니다."
            ),
        ),
        "city_slug": Param(
            DEFAULT_CITY_SLUG,
            type="string",
            description="Uber 자격 페이지의 도시 슬러그 (예: 'new-york')",
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
def uber_eligible_vehicles_raw_to_silver_pipeline():
    @task(task_id="raw_to_bronze")
    def raw_to_bronze_task(**context) -> dict:
        """Uber 자격 페이지를 수집해 Bronze 에 적재합니다."""
        params = context.get("params", {})
        # 이 핸들러는 Bronze 경로를 `base_dir` 로 받습니다. Param 이름은 `bronze_dir`
        # 이지만 event 키는 raw_to_bronze 공통 규칙을 따릅니다.
        result = lambda_handler_for("uber_eligible_vehicles_raw_to_bronze")(
            event={
                "base_dir": params.get("bronze_dir") or DEFAULT_BRONZE_DIR,
                "city_slug": params.get("city_slug") or DEFAULT_CITY_SLUG,
            }
        )
        logger.info("Raw -> Bronze 완료: %s", result)
        return result

    @task(task_id="bronze_to_silver")
    def bronze_to_silver_task(raw_result: dict, **context) -> dict:
        """Bronze 를 (차종, 상품) 단위로 펼치고 조인 키를 정규화해 Silver 로 적재합니다."""
        params = context.get("params", {})
        # Bronze 핸들러는 실행 시각으로 파티션을 정하므로 DAG 가 수집일을 따로
        # 계산하면 자정 근처에서 어긋납니다. Bronze 가 알려준 값을 그대로 씁니다.
        collected_date = (params.get("collected_date") or "").strip() or raw_result[
            "collected_date"
        ]

        # Silver 핸들러는 city_slug 를 받지 않습니다. 수집일 파티션 아래의 도시
        # 디렉터리를 전부 훑어서 한 번에 변환합니다.
        result = lambda_handler_for("uber_eligible_vehicles_bronze_to_silver")(
            event={
                "collected_date": collected_date,
                "bronze_dir": params.get("bronze_dir") or DEFAULT_BRONZE_DIR,
                "silver_dir": params.get("silver_dir") or DEFAULT_SILVER_DIR,
            }
        )
        logger.info("Bronze -> Silver 완료: %s", result)
        return result

    bronze_to_silver_task(raw_to_bronze_task())


uber_eligible_vehicles_dag = uber_eligible_vehicles_raw_to_silver_pipeline()
