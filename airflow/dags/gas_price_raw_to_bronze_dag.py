"""뉴욕주 정규 휘발유 가격을 매일 수집해 Bronze JSON으로 적재합니다."""

import importlib
import json
import logging
import math
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from airflow.sdk import dag, task
from common.validation import (
    parse_handler_result,
    parse_iso_date,
    require_file,
    run_gx_validation,
)

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


def run_gx_bronze_validation(record: dict, target_date: date) -> None:
    """Gas Price Bronze JSON의 데이터 품질 규칙을 GX로 검증합니다."""
    # DAG 파싱과 실제 검증 실행을 분리하기 위해 Task 실행 시점에 import합니다.
    import great_expectations as gx
    import pandas as pd

    dataframe = pd.DataFrame([record])
    required_columns = (
        "state",
        "fuel_type",
        "price_raw",
        "price_date_raw",
        "source_url",
        "collected_at",
    )

    raw_price = (
        dataframe["price_raw"]
        if "price_raw" in dataframe.columns
        else pd.Series([None], index=dataframe.index)
    )
    parsed_price = pd.to_numeric(
        raw_price.astype("string").str.replace("$", "", regex=False).str.strip(),
        errors="coerce",
    )
    dataframe["price_is_finite"] = parsed_price.map(
        lambda value: bool(pd.notna(value) and math.isfinite(value))
    )
    dataframe["parsed_price"] = parsed_price

    def parse_collected_at(value):
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except (TypeError, ValueError):
            return False, None
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            return False, None
        return True, parsed.astimezone(timezone.utc).date()

    collected_at = record.get("collected_at")
    has_timezone, collected_date_utc = parse_collected_at(collected_at)
    dataframe["collected_at_has_timezone"] = has_timezone
    dataframe["collected_date_utc"] = (
        collected_date_utc.isoformat() if collected_date_utc else None
    )

    # 파일 경계는 DAG에서 확인하고, Suite는 원문 필드와 값만 검증합니다.
    expectations = [
        *(
            gx.expectations.ExpectColumnToExist(column=column)
            for column in required_columns
        ),
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="price_is_finite", value_set=[True]
        ),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="parsed_price", min_value=0, strict_min=True
        ),
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="collected_at_has_timezone", value_set=[True]
        ),
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="collected_date_utc", value_set=[target_date.isoformat()]
        ),
    ]
    for column in required_columns:
        if column in dataframe.columns:
            expectations.append(
                gx.expectations.ExpectColumnValuesToNotBeNull(column=column)
            )
    if "state" in dataframe.columns:
        expectations.append(
            gx.expectations.ExpectColumnValuesToBeInSet(
                column="state", value_set=["NY"]
            )
        )
    if "fuel_type" in dataframe.columns:
        expectations.append(
            gx.expectations.ExpectColumnValuesToBeInSet(
                column="fuel_type", value_set=["regular"]
            )
        )
    if "price_raw" in dataframe.columns:
        expectations.append(
            gx.expectations.ExpectColumnValuesToMatchRegex(
                column="price_raw", regex=r"^\$\s*\d+(?:\.\d+)?$"
            )
        )
    if "price_date_raw" in dataframe.columns:
        expectations.append(
            gx.expectations.ExpectColumnValuesToMatchStrftimeFormat(
                column="price_date_raw", strftime_format="%m/%d/%y"
            )
        )
    if "source_url" in dataframe.columns:
        expectations.append(
            gx.expectations.ExpectColumnValuesToMatchRegex(
                column="source_url", regex=r"\S"
            )
        )

    run_gx_validation(
        dataframe,
        expectations,
        suite_name="gas_price_bronze_suite",
        layer="bronze",
    )


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
        parsed = parse_handler_result(result, expected_locations=1, expected_rows=1)
        collected_date = result.get("collected_date")
        target_date = parse_iso_date(collected_date)

        layout = importlib.import_module("lambda.functions.common.gas_price_layout")
        path = parsed.locations[0]
        expected = layout.bronze_file(BRONZE_DIR, collected_date)
        if path.resolve() != expected.resolve():
            raise ValueError(f"적재 경로가 예상과 다릅니다: {path}")
        require_file(path)

        try:
            record = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("Bronze JSON을 읽지 못했습니다.") from exc
        if not isinstance(record, dict):
            raise ValueError("Bronze JSON이 객체 형식이 아닙니다.")

        run_gx_bronze_validation(record, target_date)

    validate_bronze_task(raw_to_bronze_task())


gas_price_raw_to_bronze_dag = gas_price_raw_to_bronze_pipeline()
