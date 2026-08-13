"""EV Charging 일별 Bronze JSON을 월별 2컬럼 Silver로 변환합니다.

정기 실행은 매월 1일에 직전 완료 월을 처리합니다. 과거 월을 다시 처리하려면
DAG를 수동 실행하면서 ``collected_month``에 ``YYYY-MM``을 입력하세요.
"""

import importlib
import logging
import math
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.sdk import Param, dag, task
from common.validation import parse_handler_result, parse_year_month, read_parquet

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
SILVER_DIR = str(PROJECT_ROOT / "data" / "silver")


def lambda_handler_for(function_name: str):
    module = importlib.import_module(f"lambda.functions.{function_name}.handler")
    return module.lambda_handler


def previous_month(data_interval_end: datetime) -> str:
    return (data_interval_end.replace(day=1) - timedelta(days=1)).strftime("%Y-%m")


def run_gx_silver_validation(
    table,
    expected_columns: list[str],
    expected_rows: int,
    target_month: datetime,
    max_price: float,
) -> None:
    """EV Charging의 일별 2컬럼 Silver 계약을 GX로 검증합니다."""
    logging.getLogger("great_expectations").setLevel(logging.WARNING)

    import great_expectations as gx
    import pandas as pd

    dataframe = table.to_pandas()
    price = (
        dataframe["ev_price"]
        if "ev_price" in dataframe.columns
        else pd.Series([None] * len(dataframe), index=dataframe.index)
    )
    numeric_price = pd.to_numeric(price, errors="coerce")
    dataframe["ev_price_is_finite"] = numeric_price.map(
        lambda value: bool(pd.notna(value) and math.isfinite(value))
    )

    context = gx.get_context(mode="ephemeral")
    context.variables.progress_bars = {"globally": False}
    batch = (
        context.data_sources.add_pandas(name="ev_charging_silver_source")
        .add_dataframe_asset(name="ev_charging_silver_asset")
        .add_batch_definition_whole_dataframe("ev_charging_silver_batch")
        .get_batch(batch_parameters={"dataframe": dataframe})
    )

    next_month = (target_month.replace(day=28) + timedelta(days=4)).replace(day=1)
    expectations = [
        gx.expectations.ExpectTableRowCountToEqual(value=expected_rows),
        gx.expectations.ExpectTableRowCountToBeBetween(min_value=1),
        gx.expectations.ExpectTableColumnsToMatchOrderedList(
            column_list=[*expected_columns, "ev_price_is_finite"]
        ),
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="ev_price_is_finite", value_set=[True]
        ),
    ]
    for column in expected_columns:
        if column in dataframe.columns:
            expectations.append(
                gx.expectations.ExpectColumnValuesToNotBeNull(column=column)
            )
    if "date" in dataframe.columns:
        expectations.extend(
            [
                gx.expectations.ExpectColumnValuesToBeOfType(
                    column="date", type_="date"
                ),
                gx.expectations.ExpectColumnValuesToBeBetween(
                    column="date",
                    min_value=target_month.date(),
                    max_value=(next_month - timedelta(days=1)).date(),
                ),
                gx.expectations.ExpectColumnValuesToBeUnique(column="date"),
            ]
        )
    if "ev_price" in dataframe.columns:
        expectations.extend(
            [
                gx.expectations.ExpectColumnValuesToBeOfType(
                    column="ev_price", type_="float64"
                ),
                gx.expectations.ExpectColumnValuesToBeBetween(
                    column="ev_price",
                    min_value=0,
                    max_value=max_price,
                    strict_min=True,
                ),
            ]
        )

    validation = batch.validate(
        gx.ExpectationSuite(
            name="ev_charging_silver_suite", expectations=expectations
        ),
        result_format="SUMMARY",
    )
    failures = [result for result in validation.results if not result.success]
    for failure in failures:
        result = dict(failure.result)
        column = failure.expectation_config.kwargs.get("column") or "table"
        observed_value = result.get("observed_value")
        if observed_value is None:
            observed_value = result.get("partial_unexpected_list")
        logger.error(
            "gx_validation failed layer=silver expectation=%s column=%s "
            "unexpected_count=%s observed_value=%s",
            failure.expectation_config.type,
            column,
            result.get("unexpected_count"),
            observed_value,
        )
    if failures:
        rules = ", ".join(
            f"{failure.expectation_config.type}"
            f"[{failure.expectation_config.kwargs.get('column') or 'table'}]"
            for failure in failures
        )
        raise ValueError(f"EV Charging Silver GX 검증 실패: {rules}")

    logger.info(
        "gx_validation passed layer=silver expectations=%s",
        validation.statistics["evaluated_expectations"],
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
    dag_id="ev_charging_price_bronze_to_silver_pipeline",
    default_args=default_args,
    description="뉴욕시 평균 전기 요금 월별 Bronze -> Silver 파이프라인",
    schedule="0 10 1 * *",
    start_date=datetime(2026, 8, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["ev_charging", "bronze", "silver", "lambda"],
    params={
        "collected_month": Param(
            None,
            type=["null", "string"],
            pattern=r"^\d{4}-(0[1-9]|1[0-2])$",
            description="처리할 Bronze 수집월(YYYY-MM). 비우면 직전 완료 월입니다.",
        )
    },
)
def ev_charging_price_bronze_to_silver_pipeline():
    @task(task_id="bronze_to_silver")
    def bronze_to_silver_task(**context) -> dict:
        target_month = context.get("params", {}).get("collected_month")
        if not target_month:
            interval_end = context.get("data_interval_end") or datetime.now(
                timezone.utc
            )
            target_month = previous_month(interval_end)

        result = lambda_handler_for("ev_charging_stations_bronze_to_silver")(
            event={
                "collected_month": target_month,
                "bronze_dir": BRONZE_DIR,
                "silver_dir": SILVER_DIR,
            }
        )
        logger.info("Bronze -> Silver 완료: %s", result)
        return result

    @task(
        task_id="validate_silver",
        retries=1,
        retry_delay=timedelta(minutes=10),
        on_failure_callback=slack_failure_callback,
    )
    def validate_silver_task(result: dict) -> None:
        parsed = parse_handler_result(result, expected_locations=1)
        collected_month = parse_year_month(result.get("collected_month"))

        layout = importlib.import_module(
            "lambda.functions.common.ev_charging_layout"
        )
        path = parsed.locations[0]
        expected = layout.silver_file(SILVER_DIR, collected_month)
        if path.resolve() != expected.resolve():
            raise ValueError(f"적재 경로가 예상과 다릅니다: {path}")
        table = read_parquet(path)
        loader = importlib.import_module(
            "lambda.functions.ev_charging_stations_bronze_to_silver.loader"
        )
        transformer = importlib.import_module(
            "lambda.functions.ev_charging_stations_bronze_to_silver.transformer"
        )

        run_gx_silver_validation(
            table,
            loader.SCHEMA.names,
            parsed.row_count,
            datetime.strptime(collected_month, "%Y-%m"),
            transformer.MAX_USD_PER_KWH,
        )
        if table.schema != loader.SCHEMA:
            raise ValueError("Silver 스키마가 올바르지 않습니다.")

    validate_silver_task(bronze_to_silver_task())


ev_charging_price_bronze_to_silver_dag = (
    ev_charging_price_bronze_to_silver_pipeline()
)
