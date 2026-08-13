"""Gas·EV 일별 Bronze를 월별 개별·통합 Silver로 변환합니다.

정기 실행은 매월 1일에 직전 완료 월을 처리합니다. 과거 월을 다시 처리하려면
DAG를 수동 실행하면서 ``collected_month``에 ``YYYY-MM``을 입력하세요.
"""

import importlib
import logging
import math
import sys
from calendar import monthrange
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq
from airflow.sdk import Param, dag, task
from common.validation import (
    parse_handler_result,
    parse_year_month,
    read_parquet,
    run_gx_validation,
)

logger = logging.getLogger(__name__)

try:
    from common.slack_failure_callback import (
        slack_failure_callback,
        slack_retry_alert_callback,
    )
except Exception as exc:
    logger.warning("Slack 실패 콜백을 불러오지 못했습니다: %s", exc)

    def slack_retry_alert_callback(context):
        task_instance = context.get("task_instance")
        logger.warning(
            "Task 재시도 예정: %s",
            task_instance.task_id if task_instance else "unknown",
        )

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
INTEGRATED_DATASET = "gas_ev_price"
INTEGRATED_FILE_NAME = "gas_ev_price.parquet"
INTEGRATED_SCHEMA = pa.schema(
    [
        ("date", pa.date32()),
        ("gas_price", pa.float64()),
        ("ev_price", pa.float64()),
    ]
)


def lambda_handler_for(function_name: str):
    module = importlib.import_module(f"lambda.functions.{function_name}.handler")
    return module.lambda_handler


def previous_month(data_interval_end: datetime) -> str:
    return (data_interval_end.replace(day=1) - timedelta(days=1)).strftime("%Y-%m")


def target_month(context: dict) -> str:
    configured = context.get("params", {}).get("collected_month")
    if configured:
        return str(configured).strip()
    interval_end = context.get("data_interval_end") or datetime.now(timezone.utc)
    return previous_month(interval_end)


def integrated_silver_file(base_dir: str, collected_month: str) -> Path:
    return (
        Path(base_dir)
        / INTEGRATED_DATASET
        / f"collected_month={collected_month}"
        / INTEGRATED_FILE_NAME
    )


def find_missing_bronze_dates(
    base_dir: str, collected_month: str
) -> dict[str, list[str]]:
    """대상 월에서 실제 Bronze 파일이 없는 Gas·EV 수집일을 반환합니다."""
    collected_month = parse_year_month(collected_month)
    year, month = map(int, collected_month.split("-"))
    dates = [
        f"{collected_month}-{day:02d}"
        for day in range(1, monthrange(year, month)[1] + 1)
    ]
    gas_layout = importlib.import_module("lambda.functions.common.gas_price_layout")
    ev_layout = importlib.import_module(
        "lambda.functions.common.ev_charging_layout"
    )

    gas_missing = [
        collected_date
        for collected_date in dates
        if not (
            (path := gas_layout.bronze_file(base_dir, collected_date)).is_file()
            and path.stat().st_size > 0
        )
    ]
    ev_missing = [
        collected_date
        for collected_date in dates
        if not any(
            path.is_file() and path.stat().st_size > 0
            for path in ev_layout.bronze_partition(
                base_dir, collected_date
            ).glob("*.json")
        )
    ]
    return {
        "gas_price": gas_missing,
        "ev_charging_price": ev_missing,
    }


def run_gx_price_validation(
    table: pa.Table,
    expected_columns: list[str],
    expected_rows: int,
    collected_month: str,
    price_limits: dict[str, float | None],
    layer: str,
) -> None:
    """월별 가격 테이블의 행·컬럼·날짜·가격 품질을 GX로 검증합니다."""
    import great_expectations as gx
    import pandas as pd

    dataframe = table.to_pandas()
    derived_columns: list[str] = []
    for column in price_limits:
        values = (
            dataframe[column]
            if column in dataframe.columns
            else pd.Series([None] * len(dataframe), index=dataframe.index)
        )
        numeric = pd.to_numeric(values, errors="coerce")
        derived = f"{column}_is_finite"
        dataframe[derived] = numeric.map(
            lambda value: bool(pd.notna(value) and math.isfinite(value))
        )
        derived_columns.append(derived)

    target = datetime.strptime(collected_month, "%Y-%m")
    next_month = (target.replace(day=28) + timedelta(days=4)).replace(day=1)
    expectations = [
        gx.expectations.ExpectTableRowCountToEqual(value=expected_rows),
        gx.expectations.ExpectTableRowCountToBeBetween(min_value=1),
        gx.expectations.ExpectTableColumnsToMatchOrderedList(
            column_list=[*expected_columns, *derived_columns]
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
                    min_value=target.date(),
                    max_value=(next_month - timedelta(days=1)).date(),
                ),
                gx.expectations.ExpectColumnValuesToBeUnique(column="date"),
            ]
        )
    for column, maximum in price_limits.items():
        if column in dataframe.columns:
            expectations.extend(
                [
                    gx.expectations.ExpectColumnValuesToBeOfType(
                        column=column, type_="float64"
                    ),
                    gx.expectations.ExpectColumnValuesToBeBetween(
                        column=column,
                        min_value=0,
                        max_value=maximum,
                        strict_min=True,
                    ),
                ]
            )
        expectations.append(
            gx.expectations.ExpectColumnValuesToBeInSet(
                column=f"{column}_is_finite", value_set=[True]
            )
        )

    run_gx_validation(
        dataframe,
        expectations,
        suite_name=f"{layer}_suite",
        layer=layer,
    )


def combine_price_tables(gas_table: pa.Table, ev_table: pa.Table) -> pa.Table:
    """Gas·EV Silver를 날짜 집합 손실 없이 1:1 결합합니다."""
    gas_rows = gas_table.to_pylist()
    ev_rows = ev_table.to_pylist()
    gas_by_date = {row["date"]: row["gas_price"] for row in gas_rows}
    ev_by_date = {row["date"]: row["ev_price"] for row in ev_rows}

    if len(gas_by_date) != len(gas_rows) or len(ev_by_date) != len(ev_rows):
        raise ValueError("Gas·EV Silver에 중복 날짜가 있습니다.")
    if gas_by_date.keys() != ev_by_date.keys():
        raise ValueError("Gas·EV Silver 날짜 집합이 다릅니다.")

    rows = [
        {
            "date": target_date,
            "gas_price": gas_by_date[target_date],
            "ev_price": ev_by_date[target_date],
        }
        for target_date in sorted(gas_by_date)
    ]
    if not rows:
        raise ValueError("통합할 Gas·EV Silver 데이터가 없습니다.")
    return pa.Table.from_pylist(rows, schema=INTEGRATED_SCHEMA)


def write_integrated_silver(
    table: pa.Table, base_dir: str, collected_month: str
) -> Path:
    """통합 Silver를 월별 고정 파일 하나로 원자적으로 교체합니다."""
    path = integrated_silver_file(base_dir, collected_month)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        pq.write_table(table, temporary_path, compression="snappy")
        temporary_path.replace(path)
    finally:
        temporary_path.unlink(missing_ok=True)
    return path


default_args = {
    "owner": "DE_team1",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=15),
    "on_retry_callback": slack_retry_alert_callback,
    "on_failure_callback": slack_failure_callback,
}


@dag(
    dag_id="gas_ev_price_bronze_to_silver_pipeline",
    default_args=default_args,
    description="Gas·EV 월별 Bronze -> 개별·통합 Silver 파이프라인",
    schedule="0 10 1 * *",
    start_date=datetime(2026, 8, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["gas_price", "ev_charging", "bronze", "silver", "lambda"],
    params={
        "collected_month": Param(
            None,
            type=["null", "string"],
            pattern=r"^\d{4}-(0[1-9]|1[0-2])$",
            description="처리할 Bronze 수집월(YYYY-MM). 비우면 직전 완료 월입니다.",
        ),
        "require_complete_month": Param(
            1,
            type="integer",
            enum=[0, 1],
            description="1이면 일별 Bronze가 모두 있어야 하며, 0이면 부분 월을 허용합니다.",
        ),
    },
)
def gas_ev_price_bronze_to_silver_pipeline():
    @task(task_id="check_month_completeness")
    def check_month_completeness_task(**context) -> None:
        collected_month = target_month(context)
        required = context.get("params", {}).get("require_complete_month", 1)
        if isinstance(required, bool) or required not in (0, 1):
            raise ValueError("require_complete_month는 0 또는 1이어야 합니다.")

        missing = find_missing_bronze_dates(BRONZE_DIR, collected_month)
        missing = {dataset: dates for dataset, dates in missing.items() if dates}
        if not missing:
            logger.info("월 Bronze 완결성 확인 완료: month=%s", collected_month)
            return

        details = " ".join(
            f"{dataset}={','.join(dates)}" for dataset, dates in missing.items()
        )
        if required == 1:
            raise ValueError(
                f"월 Bronze 완결성 검사 실패: month={collected_month} {details}"
            )
        logger.warning(
            "월 Bronze 누락이 있지만 부분 월 처리를 허용합니다: month=%s %s",
            collected_month,
            details,
        )

    @task(task_id="gas_bronze_to_silver")
    def gas_bronze_to_silver_task(**context) -> dict:
        result = lambda_handler_for("gas_price_bronze_to_silver")(
            event={
                "collected_month": target_month(context),
                "bronze_dir": BRONZE_DIR,
                "silver_dir": SILVER_DIR,
            }
        )
        logger.info("Gas Bronze -> Silver 완료: %s", result)
        return result

    @task(task_id="ev_bronze_to_silver")
    def ev_bronze_to_silver_task(**context) -> dict:
        result = lambda_handler_for("ev_charging_stations_bronze_to_silver")(
            event={
                "collected_month": target_month(context),
                "bronze_dir": BRONZE_DIR,
                "silver_dir": SILVER_DIR,
            }
        )
        logger.info("EV Bronze -> Silver 완료: %s", result)
        return result

    @task(
        task_id="validate_gas_silver",
        retries=1,
        retry_delay=timedelta(minutes=10),
        on_failure_callback=slack_failure_callback,
    )
    def validate_gas_silver_task(result: dict) -> None:
        parsed = parse_handler_result(result, expected_locations=1)
        collected_month = parse_year_month(result.get("collected_month"))
        layout = importlib.import_module("lambda.functions.common.gas_price_layout")
        loader = importlib.import_module(
            "lambda.functions.gas_price_bronze_to_silver.loader"
        )
        path = parsed.locations[0]
        if path.resolve() != layout.silver_file(SILVER_DIR, collected_month).resolve():
            raise ValueError(f"Gas Silver 적재 경로가 예상과 다릅니다: {path}")
        table = read_parquet(path)
        run_gx_price_validation(
            table,
            loader.SCHEMA.names,
            parsed.row_count,
            collected_month,
            {"gas_price": None},
            "gas_silver",
        )

    @task(
        task_id="validate_ev_silver",
        retries=1,
        retry_delay=timedelta(minutes=10),
        on_failure_callback=slack_failure_callback,
    )
    def validate_ev_silver_task(result: dict) -> None:
        parsed = parse_handler_result(result, expected_locations=1)
        collected_month = parse_year_month(result.get("collected_month"))
        layout = importlib.import_module(
            "lambda.functions.common.ev_charging_layout"
        )
        loader = importlib.import_module(
            "lambda.functions.ev_charging_stations_bronze_to_silver.loader"
        )
        path = parsed.locations[0]
        if path.resolve() != layout.silver_file(SILVER_DIR, collected_month).resolve():
            raise ValueError(f"EV Silver 적재 경로가 예상과 다릅니다: {path}")
        table = read_parquet(path)
        run_gx_price_validation(
            table,
            loader.SCHEMA.names,
            parsed.row_count,
            collected_month,
            {"ev_price": 5.0},
            "ev_silver",
        )

    @task(task_id="integrate_silver")
    def integrate_silver_task(gas_result: dict, ev_result: dict) -> dict:
        gas = parse_handler_result(gas_result, expected_locations=1)
        ev = parse_handler_result(ev_result, expected_locations=1)
        gas_month = parse_year_month(gas_result.get("collected_month"))
        ev_month = parse_year_month(ev_result.get("collected_month"))
        if gas_month != ev_month:
            raise ValueError("Gas·EV Silver의 collected_month가 다릅니다.")

        table = combine_price_tables(
            read_parquet(gas.locations[0]), read_parquet(ev.locations[0])
        )
        path = write_integrated_silver(table, SILVER_DIR, gas_month)
        return {
            "row_count": table.num_rows,
            "locations": [str(path)],
            "collected_month": gas_month,
        }

    @task(
        task_id="validate_integrated_silver",
        retries=1,
        retry_delay=timedelta(minutes=10),
        on_failure_callback=slack_failure_callback,
    )
    def validate_integrated_silver_task(result: dict) -> None:
        parsed = parse_handler_result(result, expected_locations=1)
        collected_month = parse_year_month(result.get("collected_month"))
        path = parsed.locations[0]
        expected = integrated_silver_file(SILVER_DIR, collected_month)
        if path.resolve() != expected.resolve():
            raise ValueError(f"통합 Silver 적재 경로가 예상과 다릅니다: {path}")
        table = read_parquet(path)
        run_gx_price_validation(
            table,
            INTEGRATED_SCHEMA.names,
            parsed.row_count,
            collected_month,
            {"gas_price": None, "ev_price": 5.0},
            "integrated_silver",
        )

    completeness_checked = check_month_completeness_task()
    gas_result = gas_bronze_to_silver_task()
    gas_validated = validate_gas_silver_task(gas_result)
    ev_result = ev_bronze_to_silver_task()
    ev_validated = validate_ev_silver_task(ev_result)
    completeness_checked >> [gas_result, ev_result]
    integrated_result = integrate_silver_task(gas_result, ev_result)
    [gas_validated, ev_validated] >> integrated_result
    validate_integrated_silver_task(integrated_result)


gas_ev_price_bronze_to_silver_dag = gas_ev_price_bronze_to_silver_pipeline()
