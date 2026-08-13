"""fueleconomy.gov 차종별 제원 Raw -> Bronze -> Silver 월 1회 파이프라인.

EPA/DOE 벌크 CSV 전량을 Bronze 에 원본 그대로 적재하고, 조인 키와 연비/전비를
정제해 Silver 로 변환합니다. 두 단계 모두 lambda/functions 의 핸들러를 그대로
호출합니다.

제원 값 자체는 거의 안 바뀌고 신규 차종이 추가될 뿐입니다. 그럼에도 월 1회로
도는 이유는 **신규 차종이 `vehicle_master` 에 붙기까지의 지연**을 한 달로
묶기 위해서입니다. 연 1회로 두면 그 지연이 최대 1년이고, 그동안 리스 대장에
새로 들어온 차는 연비 없이(`spec_match_level=NONE`) 추천 대상에서 밀립니다.
매 실행은 전량 스냅샷을 새 파티션에 씁니다.

주의: `catchup=False` + 월 1회 스케줄이라 배포 직후에는 실행되지 않습니다.
다음 스케줄이 최대 한 달 뒤이므로, 처음 한 번은 Airflow UI 에서 수동 트리거하세요.

이미 적재된 Bronze 를 다시 변환하려면 수동 트리거하면서 `collected_date`
파라미터에 대상 수집일(예: "2026-08-01")을 넣으세요.
"""

import importlib
import logging
import math
import os
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from airflow.sdk import Param, dag, task
from common import assets
from common.validation import (
    parse_handler_result,
    parse_iso_date,
    read_parquet,
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

# Airflow 이미지에는 pipeline-core가 설치돼 있지 않아 경로로 참조(이후 변경 필요)
for path in (PROJECT_ROOT, PROJECT_ROOT / "libs" / "pipeline_core"):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

DEFAULT_BRONZE_DIR = os.getenv("BRONZE_DIR", str(PROJECT_ROOT / "data" / "bronze"))
DEFAULT_SILVER_DIR = os.getenv("SILVER_DIR", str(PROJECT_ROOT / "data" / "silver"))


def lambda_handler_for(function_name: str):
    """`lambda`가 파이썬 예약어라 정적 import가 안 돼 동적으로 불러옵니다."""
    module = importlib.import_module(f"lambda.functions.{function_name}.handler")
    return module.lambda_handler


def run_gx_bronze_validation(
    table,
    expected_rows: int,
    required_columns: tuple[str, ...],
    target_date: date,
    min_model_year: int,
    max_model_year: int,
    max_skip_ratio: float,
) -> None:
    """Bronze의 Silver 입력 컬럼과 변환 가능한 행 비율을 검증합니다."""
    import great_expectations as gx
    import pandas as pd

    dataframe = table.to_pandas()

    def series(column: str):
        if column in dataframe.columns:
            return dataframe[column]
        return pd.Series([None] * len(dataframe), index=dataframe.index)

    def nonblank(value: object) -> bool:
        return bool(pd.notna(value) and str(value).strip())

    def optional_nonnegative_number(value: object) -> bool:
        if pd.isna(value) or not str(value).strip():
            return True
        try:
            number = float(str(value).strip())
        except (TypeError, ValueError):
            return False
        return math.isfinite(number) and number >= 0

    year = pd.to_numeric(series("year"), errors="coerce")
    dataframe["row_is_transformable"] = (
        series("id").map(nonblank)
        & series("make").map(nonblank)
        & series("model").map(nonblank)
        & year.between(min_model_year, max_model_year)
        & series("comb08").map(optional_nonnegative_number)
        & series("combE").map(optional_nonnegative_number)
        & series("range").map(optional_nonnegative_number)
    )

    collected_at = series("collected_at")
    dataframe["collected_at_has_timezone"] = collected_at.map(
        lambda value: bool(
            pd.notna(value)
            and getattr(value, "tzinfo", None) is not None
            and value.utcoffset() is not None
        )
    )
    collected_at_type = (
        table.schema.field("collected_at").type
        if "collected_at" in table.column_names
        else None
    )
    dataframe["collected_at_timezone_is_utc"] = (
        getattr(collected_at_type, "tz", None) == "UTC"
    )
    dataframe["collected_date_utc"] = pd.to_datetime(
        collected_at, errors="coerce", utc=True
    ).dt.strftime("%Y-%m-%d")

    derived_columns = [
        "row_is_transformable",
        "collected_at_has_timezone",
        "collected_at_timezone_is_utc",
        "collected_date_utc",
    ]
    expectations = [
        gx.expectations.ExpectTableRowCountToEqual(value=expected_rows),
        gx.expectations.ExpectTableColumnsToMatchOrderedList(
            column_list=[*table.column_names, *derived_columns]
        ),
        *(gx.expectations.ExpectColumnToExist(column=column) for column in required_columns),
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="row_is_transformable",
            value_set=[True],
            mostly=1 - max_skip_ratio,
        ),
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="collected_at_has_timezone", value_set=[True]
        ),
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="collected_at_timezone_is_utc", value_set=[True]
        ),
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="collected_date_utc", value_set=[target_date.isoformat()]
        ),
    ]
    for column in required_columns:
        if column not in dataframe.columns:
            continue
        expectations.append(
            gx.expectations.ExpectColumnValuesToBeOfType(
                column=column,
                type_="Timestamp" if column == "collected_at" else "str",
            )
        )

    run_gx_validation(
        dataframe,
        expectations,
        suite_name="fueleconomy_vehicle_specs_bronze_suite",
        layer="bronze",
    )


def run_gx_silver_validation(
    table,
    expected_columns: list[str],
    min_model_year: int,
    max_model_year: int,
) -> None:
    """Silver의 조인 키·연식·제원 값·출처 ID를 검증합니다."""
    import great_expectations as gx
    import pandas as pd

    dataframe = table.to_pandas()
    metric_columns = (
        "combined_mpg",
        "combined_kwh_per_100mi",
        "range_miles",
    )
    for column in metric_columns:
        values = (
            table[column].to_pylist()
            if column in table.column_names
            else [None] * table.num_rows
        )
        dataframe[f"{column}_is_valid"] = [
            value is None
            or (
                isinstance(value, (int, float))
                and math.isfinite(value)
                and value >= 0
            )
            for value in values
        ]

    derived_columns = [f"{column}_is_valid" for column in metric_columns]
    expectations = [
        gx.expectations.ExpectTableRowCountToBeBetween(min_value=1),
        gx.expectations.ExpectTableColumnsToMatchOrderedList(
            column_list=[*expected_columns, *derived_columns]
        ),
    ]
    required_columns = ("source_id", "year", "make_key", "model_key", "bronze_path")
    for column in required_columns:
        if column in dataframe.columns:
            expectations.append(
                gx.expectations.ExpectColumnValuesToNotBeNull(column=column)
            )
    string_columns = (
        "source_id",
        "make_key",
        "model_key",
        "base_model_key",
        "atv_type",
        "bronze_path",
    )
    for column in string_columns:
        if column in dataframe.columns:
            expectations.append(
                gx.expectations.ExpectColumnValuesToBeOfType(column=column, type_="str")
            )
    for column in ("source_id", "make_key", "model_key", "bronze_path"):
        if column in dataframe.columns:
            expectations.append(
                gx.expectations.ExpectColumnValuesToMatchRegex(
                    column=column, regex=r"\S"
                )
            )
    if "year" in dataframe.columns:
        expectations.extend(
            [
                gx.expectations.ExpectColumnValuesToBeOfType(
                    column="year", type_="int16"
                ),
                gx.expectations.ExpectColumnValuesToBeBetween(
                    column="year",
                    min_value=min_model_year,
                    max_value=max_model_year,
                ),
            ]
        )
    for column in metric_columns:
        if column in dataframe.columns:
            expectations.extend(
                [
                    gx.expectations.ExpectColumnValuesToBeOfType(
                        column=column, type_="float64"
                    ),
                    gx.expectations.ExpectColumnValuesToBeBetween(
                        column=column, min_value=0
                    ),
                ]
            )
        expectations.append(
            gx.expectations.ExpectColumnValuesToBeInSet(
                column=f"{column}_is_valid", value_set=[True]
            )
        )
    if "source_id" in dataframe.columns:
        expectations.append(
            gx.expectations.ExpectColumnValuesToBeUnique(column="source_id")
        )

    run_gx_validation(
        dataframe,
        expectations,
        suite_name="fueleconomy_vehicle_specs_silver_suite",
        layer="silver",
    )


default_args = {
    "owner": "DE_team1",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=15),
    "execution_timeout": timedelta(minutes=15),
    "on_failure_callback": slack_failure_callback,
}


@dag(
    dag_id="fueleconomy_vehicle_specs_raw_to_silver_pipeline",
    default_args=default_args,
    description="fueleconomy.gov 차종별 제원 Raw -> Bronze -> Silver 월 1회 파이프라인",
    schedule="0 4 1 * *",  # 매월 1일 04:00 UTC
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["vehicle_specs", "raw", "bronze", "silver", "lambda"],
    params={
        "collected_date": Param(
            None,
            type=["null", "string"],
            pattern=r"^\d{4}-\d{2}-\d{2}$",
            description=(
                "이미 적재된 Bronze 를 다시 변환할 때만 지정 (예: '2026-01-01'). "
                "비워두면 이번 실행이 적재한 수집일을 그대로 씁니다."
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
def fueleconomy_vehicle_specs_raw_to_silver_pipeline():
    @task(task_id="raw_to_bronze")
    def raw_to_bronze_task(**context) -> dict:
        """벌크 CSV 를 받아 원본 컬럼 그대로 Bronze 에 적재합니다."""
        params = context.get("params", {})
        result = lambda_handler_for("fueleconomy_vehicle_specs_raw_to_bronze")(
            event={"base_dir": params.get("bronze_dir") or DEFAULT_BRONZE_DIR}
        )
        logger.info("Raw -> Bronze 완료: %s", result)
        return result

    @task(task_id="bronze_to_silver")
    def bronze_to_silver_task(raw_result: dict, **context) -> dict:
        """Bronze 제원의 조인 키와 연비/전비를 정제해 Silver 로 적재합니다."""
        params = context.get("params", {})
        # Bronze 핸들러는 실행 시각으로 파티션을 정하므로 DAG 가 수집일을 따로
        # 계산하면 자정 근처에서 어긋납니다. Bronze 가 알려준 값을 그대로 씁니다.
        collected_date = (params.get("collected_date") or "").strip() or raw_result[
            "collected_date"
        ]

        result = lambda_handler_for("fueleconomy_vehicle_specs_bronze_to_silver")(
            event={
                "collected_date": collected_date,
                "bronze_dir": params.get("bronze_dir") or DEFAULT_BRONZE_DIR,
                "silver_dir": params.get("silver_dir") or DEFAULT_SILVER_DIR,
            }
        )
        logger.info("Bronze -> Silver 완료: %s", result)
        return result

    # 데이터셋 규칙은 이 파일에 두고, GX 실행과 결과 기록만 공통 모듈에 맡깁니다.
    @task(
        task_id="validate_bronze",
        retries=1,
        retry_delay=timedelta(minutes=10),
        on_failure_callback=slack_failure_callback,
    )
    def validate_bronze_task(result: dict, **context) -> None:
        """Bronze 적재 경계를 확인한 뒤 Silver 입력 품질을 GX로 검증합니다."""
        params = context.get("params", {})
        bronze_dir = params.get("bronze_dir") or DEFAULT_BRONZE_DIR
        layout = importlib.import_module("lambda.functions.common.vehicle_specs_layout")
        extractor = importlib.import_module(
            "lambda.functions.fueleconomy_vehicle_specs_bronze_to_silver.extractor"
        )
        transformer = importlib.import_module(
            "lambda.functions.fueleconomy_vehicle_specs_bronze_to_silver.transformer"
        )

        parsed = parse_handler_result(result, expected_locations=1)
        target_date = parse_iso_date(result.get("collected_date"))
        path = require_file(parsed.locations[0])

        # 파일명이 수집 시각입니다. 여기서 되짚어 layout 이 정한 자리와 맞춰봅니다.
        try:
            collected_at = datetime.strptime(path.stem, "%Y%m%dT%H%M%SZ").replace(
                tzinfo=timezone.utc
            )
        except ValueError as exc:
            raise ValueError(
                f"Bronze 파일명이 수집시각 형식이 아닙니다: {path.name}"
            ) from exc

        source = layout.source_from_partition(path.parent)
        expected = layout.bronze_file(bronze_dir, source, collected_at)
        if path.resolve() != expected.resolve():
            raise ValueError(f"적재 경로가 layout 규칙과 다릅니다: {path} != {expected}")
        if collected_at.date() != target_date:
            raise ValueError(
                f"파일명의 수집일과 collected_date 가 다릅니다: "
                f"{path.name} != {target_date.isoformat()}"
            )

        table = read_parquet(path)
        run_gx_bronze_validation(
            table,
            parsed.row_count,
            extractor.NEEDED_COLUMNS,
            target_date,
            transformer.MIN_MODEL_YEAR,
            transformer.MAX_MODEL_YEAR,
            transformer.MAX_SKIP_RATIO,
        )
        logger.info(
            "Bronze 검증 통과: source=%s rows=%d", source, parsed.row_count
        )

    @task(
        task_id="validate_silver",
        # 검증을 통과했을 때만 Asset 이벤트를 냅니다 — 이걸 적재 태스크에
        # 달면 깨진 Silver 로 vehicle_master 조립이 돌아갑니다.
        outlets=[assets.FUELECONOMY_VEHICLE_SPECS_SILVER],
        retries=1,
        retry_delay=timedelta(minutes=10),
        on_failure_callback=slack_failure_callback,
    )
    def validate_silver_task(result: dict, **context) -> None:
        """Silver 는 출처별로 파일을 씁니다. 그중 하나가 비어도 잡아냅니다."""
        params = context.get("params", {})
        silver_dir = params.get("silver_dir") or DEFAULT_SILVER_DIR
        layout = importlib.import_module("lambda.functions.common.vehicle_specs_layout")
        loader = importlib.import_module(
            "lambda.functions.fueleconomy_vehicle_specs_bronze_to_silver.loader"
        )
        transformer = importlib.import_module(
            "lambda.functions.fueleconomy_vehicle_specs_bronze_to_silver.transformer"
        )

        parsed = parse_handler_result(result)
        target_date = parse_iso_date(result.get("collected_date"))

        total_rows = 0
        seen_sources: set[str] = set()
        for path in parsed.locations:
            require_file(path)

            source = layout.source_from_partition(path.parent)
            if source in seen_sources:
                raise ValueError(f"같은 출처가 두 번 적재됐습니다: {source}")
            seen_sources.add(source)

            expected = layout.silver_file(silver_dir, target_date, source)
            if path.resolve() != expected.resolve():
                raise ValueError(
                    f"적재 경로가 layout 규칙과 다릅니다: {path} != {expected}"
                )

            table = read_parquet(path)
            run_gx_silver_validation(
                table,
                loader.SCHEMA.names,
                transformer.MIN_MODEL_YEAR,
                transformer.MAX_MODEL_YEAR,
            )
            total_rows += table.num_rows

        if total_rows != parsed.row_count:
            raise ValueError(
                "Silver 행 수 합계가 row_count 와 다릅니다: "
                f"{total_rows} != {parsed.row_count}"
            )
        logger.info(
            "Silver 검증 통과: sources=%d rows=%d", len(seen_sources), total_rows
        )

    raw_result = raw_to_bronze_task()
    bronze_checked = validate_bronze_task(raw_result)
    silver_result = bronze_to_silver_task(raw_result)
    # Bronze 가 검증을 통과한 뒤에 변환합니다. 안 걸어두면 깨진 Bronze 를 그대로 읽습니다.
    bronze_checked >> silver_result
    validate_silver_task(silver_result)


fueleconomy_vehicle_specs_dag = fueleconomy_vehicle_specs_raw_to_silver_pipeline()
