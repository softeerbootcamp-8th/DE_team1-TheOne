"""리스 업체 차량 대장 DAG의 실행·검증 함수."""

import importlib
import logging
import math
import os
from datetime import date, datetime, timedelta, timezone

from airflow.sdk import task

from sub.airflow.common import assets
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


def run_gx_bronze_validation(
    table,
    expected_columns: list[str],
    expected_rows: int,
    vendor: str,
    expected_vendor: str,
    target_date: date,
    expected_price_period: str,
    min_price: float,
    max_price: float,
) -> None:
    """Bronze의 원본 컬럼·업체·주간 요금·수집일을 검증합니다."""
    import great_expectations as gx
    import pandas as pd

    dataframe = table.to_pandas()
    dataframe["vendor"] = vendor
    price_values = (
        table["price_usd"].to_pylist()
        if "price_usd" in table.column_names
        else [None] * table.num_rows
    )
    dataframe["price_is_finite"] = [
        isinstance(value, (int, float)) and math.isfinite(value)
        for value in price_values
    ]
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
        "vendor",
        "price_is_finite",
        "collected_at_has_timezone",
        "collected_at_timezone_is_utc",
        "collected_date_utc",
    ]
    expectations = [
        gx.expectations.ExpectTableRowCountToEqual(value=expected_rows),
        gx.expectations.ExpectTableColumnsToMatchOrderedList(
            column_list=[*expected_columns, *derived_columns]
        ),
    ]
    required_columns = {
        "make",
        "model",
        "price_usd",
        "price_period",
        "image_url",
        "source_url",
        "source_html_path",
        "source_image_path",
        "collected_at",
    }
    for column in required_columns:
        if column in dataframe.columns:
            expectations.append(
                gx.expectations.ExpectColumnValuesToNotBeNull(column=column)
            )
    string_columns = [
        column
        for column in expected_columns
        if column not in {"price_usd", "collected_at"}
    ]
    for column in string_columns:
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
    if "price_usd" in dataframe.columns:
        expectations.extend(
            [
                gx.expectations.ExpectColumnValuesToBeOfType(
                    column="price_usd", type_="float64"
                ),
                gx.expectations.ExpectColumnValuesToBeBetween(
                    column="price_usd", min_value=min_price, max_value=max_price
                ),
            ]
        )
    if "collected_at" in dataframe.columns:
        expectations.append(
            gx.expectations.ExpectColumnValuesToBeOfType(
                column="collected_at", type_="Timestamp"
            )
        )
    expectations.append(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="vendor", value_set=[expected_vendor]
        )
    )
    if "price_period" in dataframe.columns:
        expectations.append(
            gx.expectations.ExpectColumnValuesToBeInSet(
                column="price_period", value_set=[expected_price_period]
            )
        )
    expectations.extend(
        [
            gx.expectations.ExpectColumnValuesToBeInSet(
                column="price_is_finite", value_set=[True]
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
    )
    run_gx_validation(
        dataframe,
        expectations,
        suite_name="vehicle_catalog_bronze_suite",
        layer="bronze",
    )


def run_gx_silver_validation(
    table,
    expected_columns: list[str],
    min_price: float,
    max_price: float,
) -> None:
    """Silver의 조인 키·주간 요금·차량 중복을 검증합니다."""
    import great_expectations as gx

    dataframe = table.to_pandas()
    price_values = (
        table["weekly_lease_fee"].to_pylist()
        if "weekly_lease_fee" in table.column_names
        else [None] * table.num_rows
    )
    dataframe["weekly_price_is_finite"] = [
        isinstance(value, (int, float)) and math.isfinite(value)
        for value in price_values
    ]
    expectations = [
        gx.expectations.ExpectTableRowCountToBeBetween(min_value=1),
        gx.expectations.ExpectTableColumnsToMatchOrderedList(
            column_list=[*expected_columns, "weekly_price_is_finite"]
        ),
    ]
    for column in expected_columns:
        if column in dataframe.columns:
            expectations.append(
                gx.expectations.ExpectColumnValuesToNotBeNull(column=column)
            )
    for column in ("make_key", "model_key", "bronze_path"):
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
    if "weekly_lease_fee" in dataframe.columns:
        expectations.extend(
            [
                gx.expectations.ExpectColumnValuesToBeOfType(
                    column="weekly_lease_fee", type_="float64"
                ),
                gx.expectations.ExpectColumnValuesToBeBetween(
                    column="weekly_lease_fee",
                    min_value=min_price,
                    max_value=max_price,
                ),
            ]
        )
    expectations.append(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="weekly_price_is_finite", value_set=[True]
        )
    )
    if {"make_key", "model_key"}.issubset(dataframe.columns):
        expectations.append(
            gx.expectations.ExpectCompoundColumnsToBeUnique(
                column_list=["make_key", "model_key"]
            )
        )
    run_gx_validation(
        dataframe,
        expectations,
        suite_name="vehicle_catalog_silver_suite",
        layer="silver",
    )


@task(task_id="raw_to_bronze")
def raw_to_bronze_task(**context) -> dict:
    """렌탈 업체 사이트를 수집해 Bronze 에 적재합니다."""
    params = context.get("params", {})
    result = lambda_handler_for("vehicle_catalog_raw_to_bronze", package="sub.aws_lambda.functions")(
        event={"base_dir": params.get("bronze_dir") or DEFAULT_BRONZE_DIR}
    )
    logger.info("Raw -> Bronze 완료: %s", result)
    return result


@task(task_id="bronze_to_silver")
def bronze_to_silver_task(raw_result: dict, **context) -> dict:
    """Bronze 차량 대장의 조인 키를 정규화해 Silver 로 적재합니다."""
    params = context.get("params", {})
    collected_date = (params.get("collected_date") or "").strip() or raw_result[
        "collected_date"
    ]
    result = lambda_handler_for("vehicle_catalog_bronze_to_silver", package="sub.aws_lambda.functions")(
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
    """Bronze 적재 경계를 확인한 뒤 원본 품질을 GX로 검증합니다."""
    params = context.get("params", {})
    bronze_dir = params.get("bronze_dir") or DEFAULT_BRONZE_DIR
    layout = importlib.import_module("sub.aws_lambda.common.vehicle_catalog_layout")
    extractor = importlib.import_module(
        "sub.aws_lambda.functions.vehicle_catalog_raw_to_bronze.extractor"
    )
    loader = importlib.import_module(
        "sub.aws_lambda.functions.vehicle_catalog_raw_to_bronze.loader"
    )
    transformer = importlib.import_module(
        "sub.aws_lambda.functions.vehicle_catalog_bronze_to_silver.transformer"
    )

    parsed = parse_handler_result(result, expected_locations=1)
    target_date = parse_iso_date(result.get("collected_date"))
    path = require_file(parsed.locations[0])
    try:
        collected_at = datetime.strptime(path.stem, "%Y%m%dT%H%M%SZ").replace(
            tzinfo=timezone.utc
        )
    except ValueError as exc:
        raise ValueError(
            f"Bronze 파일명이 수집시각 형식이 아닙니다: {path.name}"
        ) from exc

    vendor = layout.vendor_from_partition(path.parent)
    expected = layout.bronze_file(bronze_dir, vendor, collected_at)
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
        loader.SCHEMA.names,
        parsed.row_count,
        vendor,
        extractor.VENDOR,
        target_date,
        transformer.EXPECTED_PRICE_PERIOD,
        transformer.MIN_WEEKLY_PRICE_USD,
        transformer.MAX_WEEKLY_PRICE_USD,
    )
    logger.info("Bronze 검증 통과: vendor=%s rows=%d", vendor, parsed.row_count)


@task(
    task_id="validate_silver",
    outlets=[assets.VEHICLE_CATALOG_SILVER],
    retries=1,
    retry_delay=timedelta(minutes=10),
    on_failure_callback=slack_failure_callback,
)
def validate_silver_task(result: dict, **context) -> None:
    """Silver 는 업체별로 파일을 씁니다. 그중 하나가 비어도 잡아냅니다."""
    params = context.get("params", {})
    silver_dir = params.get("silver_dir") or DEFAULT_SILVER_DIR
    layout = importlib.import_module("sub.aws_lambda.common.vehicle_catalog_layout")
    loader = importlib.import_module(
        "sub.aws_lambda.functions.vehicle_catalog_bronze_to_silver.loader"
    )
    transformer = importlib.import_module(
        "sub.aws_lambda.functions.vehicle_catalog_bronze_to_silver.transformer"
    )
    parsed = parse_handler_result(result)
    target_date = parse_iso_date(result.get("collected_date"))

    total_rows = 0
    seen_vendors: set[str] = set()
    for path in parsed.locations:
        require_file(path)
        vendor = layout.vendor_from_partition(path.parent)
        if vendor in seen_vendors:
            raise ValueError(f"같은 업체가 두 번 적재됐습니다: {vendor}")
        seen_vendors.add(vendor)

        expected = layout.silver_file(silver_dir, target_date, vendor)
        if path.resolve() != expected.resolve():
            raise ValueError(
                f"적재 경로가 layout 규칙과 다릅니다: {path} != {expected}"
            )

        table = read_parquet(path)
        run_gx_silver_validation(
            table,
            loader.SCHEMA.names,
            transformer.MIN_WEEKLY_PRICE_USD,
            transformer.MAX_WEEKLY_PRICE_USD,
        )
        total_rows += table.num_rows

    if total_rows != parsed.row_count:
        raise ValueError(
            "Silver 행 수 합계가 row_count 와 다릅니다: "
            f"{total_rows} != {parsed.row_count}"
        )
    logger.info("Silver 검증 통과: vendors=%d rows=%d", len(seen_vendors), total_rows)
