"""EV Charging 일별 Bronze JSON을 매월 Silver Parquet으로 변환합니다.

정기 실행은 매월 1일에 직전 완료 월을 처리합니다. 과거 월을 다시 처리하려면
DAG를 수동 실행하면서 ``collected_month``에 ``YYYY-MM``을 입력하세요.
"""

import importlib
import logging
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
) -> None:
    """EV 충전요금 Silver의 데이터 품질 규칙을 GX로 검증합니다."""
    logging.getLogger("great_expectations").setLevel(logging.WARNING)

    # DAG 파싱과 실제 검증 실행을 분리하기 위해 Task 실행 시점에 import합니다.
    import great_expectations as gx
    import pandas as pd

    dataframe = table.to_pandas()

    count_columns = (
        "nyc_station_count",
        "normalized_price_count",
        "free_station_count",
        "missing_price_count",
        "unsupported_price_count",
    )

    classified_columns = count_columns[1:]

    # 파생 컬럼은 분류 건수 합계와 수집일 관계를 GX로 비교하기 위해서만 사용합니다.
    if all(column in dataframe.columns for column in classified_columns):
        numeric_counts = dataframe[list(classified_columns)].apply(
            pd.to_numeric, errors="coerce"
        )
        dataframe["classified_station_count"] = numeric_counts.sum(
            axis=1, min_count=len(classified_columns)
        )
    else:
        dataframe["classified_station_count"] = None

    if "collected_at" in dataframe.columns:
        dataframe["collected_date"] = pd.to_datetime(
            dataframe["collected_at"], errors="coerce", utc=True
        ).dt.date
    else:
        dataframe["collected_date"] = None

    # Suite를 코드로 관리하므로 디스크에 설정을 남기지 않는 Context를 사용합니다.
    context = gx.get_context(mode="ephemeral")
    context.variables.progress_bars = {"globally": False}

    # 현재 월 Silver 전체를 이번 실행의 단일 Batch로 등록합니다.
    batch = (
        context.data_sources.add_pandas(name="ev_charging_silver_source")
        .add_dataframe_asset(name="ev_charging_silver_asset")
        .add_batch_definition_whole_dataframe("ev_charging_silver_batch")
        .get_batch(batch_parameters={"dataframe": dataframe})
    )

    string_columns = (
        "city",
        "state",
        "fuel_type_code",
        "currency",
        "price_unit",
        "source_url",
        "bronze_path",
    )

    next_month = (target_month.replace(day=28) + timedelta(days=4)).replace(day=1)

    # 행/스키마, 타입, 도메인 값, 날짜, 집계 관계를 하나의 Suite로 검증합니다.
    expectations = [
        # 행 수와 컬럼 계약
        gx.expectations.ExpectTableRowCountToEqual(value=expected_rows),
        gx.expectations.ExpectTableColumnsToMatchOrderedList(
            column_list=[
                *expected_columns,
                "classified_station_count",
                "collected_date",
            ]
        ),
        *(
            gx.expectations.ExpectColumnValuesToNotBeNull(column=column)
            for column in expected_columns
        ),
        # 논리 타입 계약
        *(
            gx.expectations.ExpectColumnValuesToBeOfType(
                column=column, type_="str"
            )
            for column in string_columns
        ),
        gx.expectations.ExpectColumnValuesToBeOfType(
            column="average_price_usd_per_kwh", type_="float64"
        ),
        gx.expectations.ExpectColumnValuesToBeOfType(
            column="price_date", type_="date"
        ),
        *(
            gx.expectations.ExpectColumnValuesToBeOfType(
                column=column, type_="int64"
            )
            for column in count_columns
        ),
        gx.expectations.ExpectColumnValuesToBeOfType(
            column="collected_at", type_="Timestamp"
        ),
        # 데이터셋 고정값 계약
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="city", value_set=["New York City"]
        ),
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="state", value_set=["NY"]
        ),
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="fuel_type_code", value_set=["ELEC"]
        ),
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="currency", value_set=["USD"]
        ),
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="price_unit", value_set=["kWh"]
        ),
        # 요금과 기준일 계약
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="average_price_usd_per_kwh", min_value=0, max_value=5
        ),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="price_date",
            min_value=target_month.date(),
            max_value=(next_month - timedelta(days=1)).date(),
        ),
        gx.expectations.ExpectColumnValuesToBeUnique(column="price_date"),
        # 건수와 lineage 계약
        *(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column=column, min_value=0
            )
            for column in count_columns
        ),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="normalized_price_count", min_value=1
        ),
        gx.expectations.ExpectColumnPairValuesToBeEqual(
            column_A="classified_station_count",
            column_B="nyc_station_count",
            ignore_row_if="neither",
        ),
        gx.expectations.ExpectColumnPairValuesToBeEqual(
            column_A="collected_date",
            column_B="price_date",
            ignore_row_if="neither",
        ),
        gx.expectations.ExpectColumnValuesToMatchRegex(
            column="source_url", regex=r"\S"
        ),
        gx.expectations.ExpectColumnValuesToMatchRegex(
            column="bronze_path", regex=r"\S"
        ),
    ]

    # SUMMARY로 원문 전체 대신 실패 건수와 일부 예시만 받습니다.
    validation = batch.validate(
        gx.ExpectationSuite(
            name="ev_charging_silver_suite", expectations=expectations
        ),
        result_format="SUMMARY",
    )

    failures = [result for result in validation.results if not result.success]
    for failure in failures:
        result = dict(failure.result)
        kwargs = failure.expectation_config.kwargs
        column = kwargs.get("column") or "/".join(
            filter(None, (kwargs.get("column_A"), kwargs.get("column_B")))
        )
        observed_value = result.get("observed_value")
        if observed_value is None:
            observed_value = result.get("partial_unexpected_list")
        logger.error(
            "gx_validation failed layer=silver expectation=%s column=%s "
            "unexpected_count=%s observed_value=%s",
            failure.expectation_config.type,
            column or "table",
            result.get("unexpected_count"),
            observed_value,
        )

    # 실패를 예외로 전파해야 Airflow 재시도와 Slack 콜백이 동작합니다.
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
        if not isinstance(result, dict):
            raise TypeError("Handler 결과가 dict가 아닙니다.")

        row_count = result.get("row_count")
        locations = result.get("locations")
        collected_month = result.get("collected_month")
        if isinstance(row_count, bool) or not isinstance(row_count, int):
            raise ValueError("row_count가 정수가 아닙니다.")
        if row_count <= 0:
            raise ValueError("Silver row_count는 1 이상이어야 합니다.")
        if (
            not isinstance(locations, list)
            or len(locations) != 1
            or not isinstance(locations[0], str)
            or not locations[0]
        ):
            raise ValueError("locations에는 파일 경로가 하나 있어야 합니다.")
        if not isinstance(collected_month, str):
            raise ValueError("collected_month가 문자열이 아닙니다.")
        try:
            target_month = datetime.strptime(collected_month, "%Y-%m")
        except ValueError as exc:
            raise ValueError("collected_month는 YYYY-MM 형식이어야 합니다.") from exc
        if target_month.strftime("%Y-%m") != collected_month:
            raise ValueError("collected_month는 YYYY-MM 형식이어야 합니다.")

        layout = importlib.import_module(
            "lambda.functions.common.ev_charging_layout"
        )
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
            "lambda.functions.ev_charging_stations_bronze_to_silver.loader"
        )

        # 위에서는 Handler 응답·경로·파일 존재와 Parquet 읽기를 확인했고,
        # 여기서는 Parquet 안의 행·값·날짜·집계 품질 규칙을 GX로 검증합니다.
        run_gx_silver_validation(
            table, loader.SCHEMA.names, row_count, target_month
        )
        # Pandas 기반 GX 타입은 Arrow timestamp 단위와 timezone까지 구분하지 못하므로,
        # Loader가 정의한 정확한 Arrow 물리 스키마 비교는 경계 검사로 유지합니다.
        if table.schema != loader.SCHEMA:
            raise ValueError("Silver 스키마가 올바르지 않습니다.")

    validate_silver_task(bronze_to_silver_task())


ev_charging_price_bronze_to_silver_dag = (
    ev_charging_price_bronze_to_silver_pipeline()
)
