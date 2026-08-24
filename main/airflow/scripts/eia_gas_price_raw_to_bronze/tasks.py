"""EIA 주간 휘발유 원본 수집 DAG의 실행·검증 함수.

이 파일 하나에 2000년부터의 이력이 통째로 들어 있어 매일 받을 이유가 없습니다.
월 1회로 갱신하는 것은 EIA 가 과거 값을 개정하므로 최신 개정분을 확보하기
위해서입니다.
"""

import importlib
import logging
from datetime import timezone

from airflow.sdk import task

from shared.airflow.common.lambda_invoke import invoke_lambda
from shared.airflow.common.project_paths import PROJECT_ROOT
from shared.airflow.common.validation import (
    layout_tail,
    location_size,
    parse_handler_result,
    publish_success_marker,
    require_file,
)

logger = logging.getLogger(__name__)

BRONZE_DIR = str(PROJECT_ROOT / "data" / "bronze")


def _layout():
    return importlib.import_module("main.aws_lambda.common.eia_fuel_price_layout")


@task(task_id="raw_to_bronze")
def raw_to_bronze_task(**context) -> dict:
    params = context["params"]
    collected_at = (
        context["dag_run"].start_date.astimezone(timezone.utc)
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )
    event = {
        "service_area": params["service_area"],
        "collected_at": collected_at,
    }
    result = invoke_lambda(
        "eia_gas_price_raw_to_bronze",
        package="main.aws_lambda.functions",
        event=event,
        local_event={"base_dir": params["bronze_dir"]},
    )
    logger.info("Raw -> Bronze 완료: %s", result)
    return result


@task(task_id="validate_bronze")
def validate_bronze_task(result: dict, **context) -> None:
    """적재 경로가 layout 규칙과 같은지, 파일이 비어 있지 않은지 확인합니다."""
    parsed = parse_handler_result(result, expected_locations=1, expected_rows=1)
    layout = _layout()
    service_area = context["params"]["service_area"]
    expected = layout.gas_bronze_file(
        context["params"]["bronze_dir"], result.get("collected_at"), service_area
    )
    path = require_file(parsed.locations[0])
    if layout_tail(path, segments=4, service_area=service_area) != layout_tail(
        expected, segments=4, service_area=service_area
    ):
        raise ValueError(f"적재 경로가 예상과 다릅니다: {path}")

    # 하한은 수집(lambda)과 같은 값을 씁니다 — 두 곳이 갈라지면 한쪽만 통과합니다.
    size = location_size(path)
    if size < layout.GAS_MIN_BYTES:
        raise ValueError(f"EIA 원본이 너무 작습니다: {size} bytes ({path})")
    publish_success_marker(path.parent)
    logger.info("bronze 검증 통과: %s (%d bytes)", path, size)
