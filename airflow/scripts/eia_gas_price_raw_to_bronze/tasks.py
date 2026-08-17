"""EIA 주간 휘발유 원본 수집 DAG의 실행·검증 함수.

크롤링(AAA·NLR)이 **오늘 값만** 주는 것과 달리, 이 파일 하나에 이력이 통째로 들어
있습니다. 그래서 매일 받을 이유가 없고 월 1회로 갱신합니다 — EIA 가 과거 값을
개정하므로 주기적으로 다시 받아 최신 개정분을 확보하는 것이 목적입니다.
"""

import importlib
import logging
from datetime import datetime, timezone

from airflow.sdk import task

from common.lambda_runtime import lambda_handler_for
from common.project_paths import PROJECT_ROOT
from common.validation import parse_handler_result, require_file

logger = logging.getLogger(__name__)

BRONZE_DIR = str(PROJECT_ROOT / "data" / "bronze")
HANDLER_NAME = "eia_gas_price_raw_to_bronze"


def _layout():
    return importlib.import_module("lambda.functions.common.eia_fuel_price_layout")


@task(task_id="raw_to_bronze")
def raw_to_bronze_task(**context) -> dict:
    result = lambda_handler_for(HANDLER_NAME)(
        event={"base_dir": context["params"]["bronze_dir"]}
    )
    logger.info("Raw -> Bronze 완료: %s", result)
    return result


@task(task_id="validate_bronze")
def validate_bronze_task(result: dict, **context) -> None:
    """적재 경로가 layout 규칙과 같은지, 파일이 비어 있지 않은지 확인합니다."""
    parsed = parse_handler_result(result, expected_locations=1, expected_rows=1)
    path = require_file(parsed.locations[0])

    layout = _layout()
    collected_date = datetime.strptime(result["collected_date"], "%Y-%m-%d").date()
    expected = layout.gas_bronze_file(context["params"]["bronze_dir"], collected_date)
    if path.resolve() != expected.resolve():
        raise ValueError(f"적재 경로가 예상과 다릅니다: {path}")

    # 하한은 수집(lambda)과 같은 값을 씁니다 — 두 곳이 갈라지면 한쪽만 통과합니다.
    size = path.stat().st_size
    if size < layout.GAS_MIN_BYTES:
        raise ValueError(f"EIA 원본이 너무 작습니다: {size} bytes ({path})")
    logger.info("bronze 검증 통과: %s (%d bytes)", path, size)
