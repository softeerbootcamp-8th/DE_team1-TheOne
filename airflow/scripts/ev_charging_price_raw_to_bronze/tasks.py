"""EV 충전소 Raw → Bronze 실행과 검증 함수."""

import importlib
import json
import logging
import os
from datetime import datetime, timedelta, timezone

from airflow.sdk import Variable, task
from common.lambda_runtime import lambda_handler_for
from common.project_paths import PROJECT_ROOT
from common.slack_failure_callback import slack_failure_callback
from common.validation import (
    parse_handler_result,
    parse_iso_date,
    require_file,
    run_gx_validation,
)

logger = logging.getLogger(__name__)

BRONZE_DIR = str(PROJECT_ROOT / "data" / "bronze")


def run_gx_bronze_validation(stations: list[dict], total_results: int) -> None:
    """EV 충전소 원문의 데이터 품질 규칙을 GX로 검증합니다."""
    import great_expectations as gx
    import pandas as pd

    dataframe = pd.DataFrame(stations)
    expectations = [
        gx.expectations.ExpectTableRowCountToBeBetween(min_value=1),
        gx.expectations.ExpectTableRowCountToEqual(value=total_results),
        *(
            gx.expectations.ExpectColumnToExist(column=column)
            for column in ("state", "fuel_type_code", "ev_pricing")
        ),
        *(
            gx.expectations.ExpectColumnValuesToNotBeNull(column=column)
            for column in ("state", "fuel_type_code")
        ),
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="state", value_set=["NY"]
        ),
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="fuel_type_code", value_set=["ELEC"]
        ),
        gx.expectations.ExpectColumnValuesToBeOfType(
            column="ev_pricing", type_="str"
        ),
    ]
    run_gx_validation(
        dataframe,
        expectations,
        suite_name="ev_charging_bronze_suite",
        layer="bronze",
    )


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
    parsed = parse_handler_result(result, expected_locations=1, expected_rows=1)
    target_date = parse_iso_date(result.get("collected_date"))
    if result.get("state") != "NY" or result.get("fuel_type_code") != "ELEC":
        raise ValueError("Handler의 state 또는 fuel_type_code가 올바르지 않습니다.")

    path = require_file(parsed.locations[0])
    try:
        collected_at = datetime.strptime(path.stem, "%Y%m%dT%H%M%SZ").replace(
            tzinfo=timezone.utc
        )
    except ValueError as exc:
        raise ValueError("Bronze 파일명의 수집시각 형식이 올바르지 않습니다.") from exc

    layout = importlib.import_module("lambda.functions.common.ev_charging_layout")
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
    if not isinstance(stations, list):
        raise ValueError("Bronze fuel_stations가 목록 형식이 아닙니다.")
    if isinstance(total_results, bool) or not isinstance(total_results, int):
        raise ValueError("Bronze total_results가 정수가 아닙니다.")
    if any(not isinstance(station, dict) for station in stations):
        raise ValueError("Bronze 충전소 데이터가 객체 형식이 아닙니다.")

    run_gx_bronze_validation(stations, total_results)
