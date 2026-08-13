"""Gas Price 일별 Bronze JSON을 매월 Silver Parquet으로 변환합니다.

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
import pyarrow.parquet as pq

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
) -> None:
    """Gas Price Silver Parquet의 데이터 품질 규칙을 GX로 검증합니다."""
    logging.getLogger("great_expectations").setLevel(logging.WARNING)

    # DAG 파싱과 실제 검증 실행을 분리하기 위해 Task 실행 시점에 import합니다.
    import great_expectations as gx
    import pandas as pd

    dataframe = table.to_pandas()
    price = (
        dataframe["price_usd_per_gallon"]
        if "price_usd_per_gallon" in dataframe.columns
        else pd.Series([None] * len(dataframe), index=dataframe.index)
    )
    numeric_price = pd.to_numeric(price, errors="coerce")
    dataframe["price_is_finite"] = numeric_price.map(
        lambda value: bool(pd.notna(value) and math.isfinite(value))
    )

    collected_at = (
        dataframe["collected_at"]
        if "collected_at" in dataframe.columns
        else pd.Series([None] * len(dataframe), index=dataframe.index)
    )
    collected_at_utc = pd.to_datetime(collected_at, errors="coerce", utc=True)
    dataframe["collected_month_utc"] = collected_at_utc.dt.strftime("%Y-%m")
    dataframe["collected_date_utc"] = collected_at_utc.dt.date

    context = gx.get_context(mode="ephemeral")
    context.variables.progress_bars = {"globally": False}
    batch = (
        context.data_sources.add_pandas(name="gas_price_silver_source")
        .add_dataframe_asset(name="gas_price_silver_asset")
        .add_batch_definition_whole_dataframe("gas_price_silver_batch")
        .get_batch(batch_parameters={"dataframe": dataframe})
    )

    string_columns = ("state", "fuel_type", "source_url", "bronze_path")
    expectations = [
        gx.expectations.ExpectTableRowCountToEqual(value=expected_rows),
        gx.expectations.ExpectTableColumnsToMatchOrderedList(
            column_list=[
                *expected_columns,
                "price_is_finite",
                "collected_month_utc",
                "collected_date_utc",
            ]
        ),
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="price_is_finite", value_set=[True]
        ),
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="collected_month_utc",
            value_set=[target_month.strftime("%Y-%m")],
        ),
    ]
    for column in expected_columns:
        if column in dataframe.columns:
            expectations.append(
                gx.expectations.ExpectColumnValuesToNotBeNull(column=column)
            )
    for column in string_columns:
        if column in dataframe.columns:
            expectations.append(
                gx.expectations.ExpectColumnValuesToBeOfType(
                    column=column, type_="str"
                )
            )
    if "price_usd_per_gallon" in dataframe.columns:
        expectations.extend(
            [
                gx.expectations.ExpectColumnValuesToBeOfType(
                    column="price_usd_per_gallon", type_="float64"
                ),
                gx.expectations.ExpectColumnValuesToBeBetween(
                    column="price_usd_per_gallon", min_value=0, strict_min=True
                ),
            ]
        )
    if "price_date" in dataframe.columns:
        expectations.extend(
            [
                gx.expectations.ExpectColumnValuesToBeOfType(
                    column="price_date", type_="date"
                ),
                gx.expectations.ExpectColumnValuesToBeUnique(column="price_date"),
                gx.expectations.ExpectColumnPairValuesAToBeGreaterThanB(
                    column_A="collected_date_utc",
                    column_B="price_date",
                    or_equal=True,
                    ignore_row_if="neither",
                ),
            ]
        )
    if "collected_at" in dataframe.columns:
        expectations.append(
            gx.expectations.ExpectColumnValuesToBeOfType(
                column="collected_at", type_="Timestamp"
            )
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
    for column in ("source_url", "bronze_path"):
        if column in dataframe.columns:
            expectations.append(
                gx.expectations.ExpectColumnValuesToMatchRegex(
                    column=column, regex=r"\S"
                )
            )

    validation = batch.validate(
        gx.ExpectationSuite(
            name="gas_price_silver_suite", expectations=expectations
        ),
        result_format="SUMMARY",
    )
    failures = [result for result in validation.results if not result.success]

    def failure_column(failure) -> str:
        kwargs = failure.expectation_config.kwargs
        return kwargs.get("column") or "/".join(
            filter(None, (kwargs.get("column_A"), kwargs.get("column_B")))
        ) or "table"

    for failure in failures:
        result = dict(failure.result)
        observed_value = result.get("observed_value")
        if observed_value is None:
            observed_value = result.get("partial_unexpected_list")
        logger.error(
            "gx_validation failed layer=silver expectation=%s column=%s "
            "unexpected_count=%s observed_value=%s",
            failure.expectation_config.type,
            failure_column(failure),
            result.get("unexpected_count"),
            observed_value,
        )
    if failures:
        rules = ", ".join(
            f"{failure.expectation_config.type}"
            f"[{failure_column(failure)}]"
            for failure in failures
        )
        raise ValueError(f"Gas Price Silver GX 검증 실패: {rules}")

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
    dag_id="gas_price_bronze_to_silver_pipeline",
    default_args=default_args,
    description="뉴욕주 정규 휘발유 가격 월별 Bronze -> Silver 파이프라인",
    schedule="0 10 1 * *",
    start_date=datetime(2026, 8, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["gas_price", "bronze", "silver", "lambda"],
    params={
        "collected_month": Param(
            None,
            type=["null", "string"],
            pattern=r"^\d{4}-(0[1-9]|1[0-2])$",
            description="처리할 Bronze 수집월(YYYY-MM). 비우면 직전 완료 월입니다.",
        )
    },
)
def gas_price_bronze_to_silver_pipeline():
    @task(task_id="bronze_to_silver")
    def bronze_to_silver_task(**context) -> dict:
        target_month = context.get("params", {}).get("collected_month")
        if not target_month:
            interval_end = context.get("data_interval_end") or datetime.now(
                timezone.utc
            )
            target_month = previous_month(interval_end)

        result = lambda_handler_for("gas_price_bronze_to_silver")(
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
        if not isinstance(result, dict):
            raise TypeError("Handler 결과가 dict가 아닙니다.")

        row_count = result.get("row_count")
        locations = result.get("locations")
        collected_month = result.get("collected_month")
        if isinstance(row_count, bool) or not isinstance(row_count, int):
            raise ValueError("row_count가 정수가 아닙니다.")
        if row_count <= 0:
            raise ValueError("Silver row_count는 1 이상이어야 합니다.")
        if not isinstance(locations, list) or len(locations) != 1:
            raise ValueError("locations에는 파일 경로가 하나 있어야 합니다.")
        if not isinstance(collected_month, str):
            raise ValueError("collected_month가 문자열이 아닙니다.")
        try:
            target_month = datetime.strptime(collected_month, "%Y-%m")
        except ValueError as exc:
            raise ValueError("collected_month는 YYYY-MM 형식이어야 합니다.") from exc
        if target_month.strftime("%Y-%m") != collected_month:
            raise ValueError("collected_month는 YYYY-MM 형식이어야 합니다.")

        layout = importlib.import_module("lambda.functions.common.gas_price_layout")
        path = Path(locations[0])
        expected = layout.silver_file(SILVER_DIR, collected_month)
        if path.resolve() != expected.resolve():
            raise ValueError(f"적재 경로가 예상과 다릅니다: {path}")
        if not path.is_file():
            raise FileNotFoundError(f"적재 파일이 없습니다: {path}")

        try:
            table = pq.ParquetFile(path).read()
        except Exception as exc:
            raise RuntimeError(f"Silver Parquet을 읽지 못했습니다: {path}") from exc
        loader = importlib.import_module(
            "lambda.functions.gas_price_bronze_to_silver.loader"
        )

        run_gx_silver_validation(
            table, loader.SCHEMA.names, row_count, target_month
        )
        if table.schema != loader.SCHEMA:
            raise ValueError("Silver 스키마가 올바르지 않습니다.")

    validate_silver_task(bronze_to_silver_task())


gas_price_bronze_to_silver_dag = gas_price_bronze_to_silver_pipeline()
