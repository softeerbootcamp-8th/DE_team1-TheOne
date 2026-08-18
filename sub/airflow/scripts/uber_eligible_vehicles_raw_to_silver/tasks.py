"""Uber 자격 차량 DAG의 실행·검증 함수."""

import importlib
import logging
import os
from datetime import date, datetime, timedelta, timezone

from airflow.sdk import task

from shared.airflow.common import assets
from shared.airflow.common.lambda_runtime import lambda_handler_for
from shared.airflow.common.project_paths import PROJECT_ROOT
from shared.airflow.common.slack_failure_callback import slack_failure_callback
from shared.airflow.common.validation import (
    parse_handler_result,
    parse_iso_date,
    read_parquet,
    require_file,
    run_gx_validation,
)


logger = logging.getLogger(__name__)

DEFAULT_BRONZE_DIR = os.getenv("BRONZE_DIR", str(PROJECT_ROOT / "data" / "bronze"))
DEFAULT_SILVER_DIR = os.getenv("SILVER_DIR", str(PROJECT_ROOT / "data" / "silver"))
DEFAULT_CITY_SLUG = os.getenv("UBER_CITY_SLUG", "new-york")


def run_gx_bronze_validation(
    table,
    expected_columns: list[str],
    expected_rows: int,
    requested_city: str,
    target_date: date,
    min_model_year: int,
    max_model_year: int,
) -> None:
    """Uber Bronze의 행 수·필수 차량값·도시·수집일을 검증합니다."""
    import great_expectations as gx
    import numpy as np
    import pandas as pd

    dataframe = table.to_pandas()
    products = (
        dataframe["products"]
        if "products" in dataframe.columns
        else pd.Series([None] * len(dataframe), index=dataframe.index)
    )

    def is_product_list(value) -> bool:
        return isinstance(value, (list, tuple, np.ndarray))

    dataframe["products_count"] = products.map(
        lambda value: len(value) if is_product_list(value) else None
    )
    dataframe["products_nonblank"] = products.map(
        lambda value: bool(
            is_product_list(value)
            and len(value) > 0
            and all(isinstance(item, str) and item.strip() for item in value)
        )
    )
    collected_at = (
        dataframe["collected_at"]
        if "collected_at" in dataframe.columns
        else pd.Series([None] * len(dataframe), index=dataframe.index)
    )
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
        "products_count",
        "products_nonblank",
        "collected_at_has_timezone",
        "collected_at_timezone_is_utc",
        "collected_date_utc",
    ]
    expectations = [
        gx.expectations.ExpectTableRowCountToEqual(value=expected_rows),
        gx.expectations.ExpectTableColumnsToMatchOrderedList(
            column_list=[*expected_columns, *derived_columns]
        ),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="products_count", min_value=1
        ),
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="products_nonblank", value_set=[True]
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
    for column in expected_columns:
        if column in dataframe.columns:
            expectations.append(
                gx.expectations.ExpectColumnValuesToNotBeNull(column=column)
            )
    for column in ("city_slug", "make", "model", "raw_eligibility"):
        if column in dataframe.columns:
            expectations.extend(
                [
                    gx.expectations.ExpectColumnValuesToBeOfType(
                        column=column, type_="str"
                    ),
                    gx.expectations.ExpectColumnValuesToMatchRegex(
                        column=column, regex=r"\S"
                    ),
                ]
            )
    if "city_slug" in dataframe.columns:
        expectations.append(
            gx.expectations.ExpectColumnValuesToBeInSet(
                column="city_slug", value_set=[requested_city]
            )
        )
    if "min_year" in dataframe.columns:
        expectations.extend(
            [
                gx.expectations.ExpectColumnValuesToBeOfType(
                    column="min_year", type_="int16"
                ),
                gx.expectations.ExpectColumnValuesToBeBetween(
                    column="min_year",
                    min_value=min_model_year,
                    max_value=max_model_year,
                ),
            ]
        )
    if "collected_at" in dataframe.columns:
        expectations.append(
            gx.expectations.ExpectColumnValuesToBeOfType(
                column="collected_at", type_="Timestamp"
            )
        )
    run_gx_validation(
        dataframe,
        expectations,
        suite_name="uber_eligible_vehicles_bronze_suite",
        layer="bronze",
    )


def run_gx_silver_validation(
    table,
    expected_columns: list[str],
    min_model_year: int,
    max_model_year: int,
) -> None:
    """도시별 Uber Silver의 차량 자격 데이터 품질을 검증합니다."""
    import great_expectations as gx

    dataframe = table.to_pandas()
    expectations = [
        gx.expectations.ExpectTableRowCountToBeBetween(min_value=1),
        gx.expectations.ExpectTableColumnsToMatchOrderedList(
            column_list=expected_columns
        ),
    ]
    for column in expected_columns:
        if column in dataframe.columns:
            expectations.append(
                gx.expectations.ExpectColumnValuesToNotBeNull(column=column)
            )
    for column in ("make_key", "model_key", "product", "bronze_path"):
        if column in dataframe.columns:
            expectations.extend(
                [
                    gx.expectations.ExpectColumnValuesToBeOfType(
                        column=column, type_="str"
                    ),
                    gx.expectations.ExpectColumnValuesToMatchRegex(
                        column=column, regex=r"\S"
                    ),
                ]
            )
    if "min_year" in dataframe.columns:
        expectations.extend(
            [
                gx.expectations.ExpectColumnValuesToBeOfType(
                    column="min_year", type_="int16"
                ),
                gx.expectations.ExpectColumnValuesToBeBetween(
                    column="min_year",
                    min_value=min_model_year,
                    max_value=max_model_year,
                ),
            ]
        )
    identity_columns = ["make_key", "model_key", "product"]
    if all(column in dataframe.columns for column in identity_columns):
        expectations.append(
            gx.expectations.ExpectCompoundColumnsToBeUnique(
                column_list=identity_columns
            )
        )
    run_gx_validation(
        dataframe,
        expectations,
        suite_name="uber_eligible_vehicles_silver_suite",
        layer="silver",
    )


@task(task_id="raw_to_bronze")
def raw_to_bronze_task(**context) -> dict:
    """Uber 자격 페이지를 수집해 Bronze 에 적재합니다."""
    params = context.get("params", {})
    result = lambda_handler_for("uber_eligible_vehicles_raw_to_bronze", package="sub.lambda_runtime.functions")(
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
    collected_date = (params.get("collected_date") or "").strip() or raw_result[
        "collected_date"
    ]
    result = lambda_handler_for("uber_eligible_vehicles_bronze_to_silver", package="sub.lambda_runtime.functions")(
        event={
            "collected_date": collected_date,
            "bronze_dir": params.get("bronze_dir") or DEFAULT_BRONZE_DIR,
            "silver_dir": params.get("silver_dir") or DEFAULT_SILVER_DIR,
        }
    )
    logger.info("Bronze -> Silver 완료: %s", result)
    return result


@task(
    task_id="validate_bronze",
    retries=1,
    retry_delay=timedelta(minutes=10),
    on_failure_callback=slack_failure_callback,
)
def validate_bronze_task(result: dict, **context) -> None:
    """Bronze 적재 결과가 layout 규칙과 맞는지, 요청한 도시를 긁었는지 봅니다."""
    params = context.get("params", {})
    bronze_dir = params.get("bronze_dir") or DEFAULT_BRONZE_DIR
    requested_city = params.get("city_slug") or DEFAULT_CITY_SLUG
    layout = importlib.import_module(
        "shared.lambda_runtime.common.uber_eligible_vehicles_layout"
    )
    loader = importlib.import_module(
        "sub.lambda_runtime.functions.uber_eligible_vehicles_raw_to_bronze.loader"
    )
    transformer = importlib.import_module(
        "sub.lambda_runtime.functions.uber_eligible_vehicles_bronze_to_silver.transformer"
    )
    parsed = parse_handler_result(result, expected_locations=1)
    collected_date = parse_iso_date(result.get("collected_date"))
    if result.get("city_slug") != requested_city:
        raise ValueError(
            f"요청한 도시와 수집한 도시가 다릅니다: "
            f"{requested_city} != {result.get('city_slug')!r}"
        )

    path = parsed.locations[0]
    require_file(path)
    try:
        collected_at = datetime.strptime(path.stem, "%Y%m%dT%H%M%SZ").replace(
            tzinfo=timezone.utc
        )
    except ValueError as exc:
        raise ValueError(
            f"Bronze 파일명이 수집시각 형식이 아닙니다: {path.name}"
        ) from exc
    expected = layout.bronze_file(bronze_dir, requested_city, collected_at)
    if path.resolve() != expected.resolve():
        raise ValueError(f"적재 경로가 layout 규칙과 다릅니다: {path} != {expected}")
    if collected_at.date() != collected_date:
        raise ValueError(
            f"파일명의 수집일과 collected_date 가 다릅니다: "
            f"{path.name} != {collected_date}"
        )

    table = read_parquet(path)
    run_gx_bronze_validation(
        table,
        loader.SCHEMA.names,
        parsed.row_count,
        requested_city,
        collected_date,
        transformer.MIN_MODEL_YEAR,
        transformer.MAX_MODEL_YEAR,
    )


@task(
    task_id="validate_silver",
    outlets=[assets.UBER_ELIGIBLE_VEHICLES_SILVER],
    retries=1,
    retry_delay=timedelta(minutes=10),
    on_failure_callback=slack_failure_callback,
)
def validate_silver_task(result: dict, **context) -> None:
    """Silver 는 도시별로 파일을 씁니다. 그중 하나가 비어도 잡아냅니다."""
    params = context.get("params", {})
    silver_dir = params.get("silver_dir") or DEFAULT_SILVER_DIR
    layout = importlib.import_module(
        "shared.lambda_runtime.common.uber_eligible_vehicles_layout"
    )
    loader = importlib.import_module(
        "sub.lambda_runtime.functions.uber_eligible_vehicles_bronze_to_silver.loader"
    )
    transformer = importlib.import_module(
        "sub.lambda_runtime.functions.uber_eligible_vehicles_bronze_to_silver.transformer"
    )
    parsed = parse_handler_result(result)
    target_date = parse_iso_date(result.get("collected_date"))

    total_rows = 0
    seen_cities: set[str] = set()
    for path in parsed.locations:
        require_file(path)
        city = layout.city_from_partition(path.parent)
        if city in seen_cities:
            raise ValueError(f"같은 도시가 두 번 적재됐습니다: {city}")
        seen_cities.add(city)
        expected = layout.silver_file(silver_dir, target_date, city)
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
            f"Silver 행 수 합계가 row_count 와 다릅니다: "
            f"{total_rows} != {parsed.row_count}"
        )
    logger.info("Silver 검증 통과: cities=%d rows=%d", len(seen_cities), total_rows)
