"""Uber 배차 가능 차량 목록 Raw -> Bronze -> Silver 파이프라인 DAG.

Uber 자격 안내 페이지를 수집해 Bronze 에 원문 그대로 적재하고, (차종, 상품) 단위로
펼쳐 조인 키를 정규화한 뒤 Silver 로 변환합니다. 두 단계 모두 lambda/functions 의
핸들러를 그대로 호출합니다.

Lyft DAG(`lyft_eligible_vehicles_raw_to_silver_dag`) 와 같은 모양이지만 **별도로
둡니다.** 두 플랫폼은 서로 다른 사이트를 긁고 데이터 의존이 없습니다. 한쪽 사이트가
개편돼 크롤링이 깨져도 다른 쪽은 끝까지 가야 합니다.

    Bronze  ZDX  "2010 (UberX, Comfort) / 2018 (Comfort Electric)"
    Silver  ZDX  UberX             min_year=2010
            ZDX  Comfort           min_year=2010
            ZDX  Comfort Electric  min_year=2018

Gold 조인 관점:
    - Silver 컬럼이 `lyft_eligible_vehicles` 와 동일합니다
      (make_key, model_key, product, min_year). 두 플랫폼을 union 해서 쓸 수 있습니다.
    - 다만 상품명이 겹칩니다. `Black` / `Black SUV` 는 Uber 에도 Lyft 에도 있습니다.
      union 할 때 어느 플랫폼인지 구분할 값을 읽는 쪽에서 채워야 합니다
      (데이터셋 경로가 다르므로 상수로 채울 수 있습니다).
    - 조인 키는 `lambda/functions/common/join_keys.py` 규칙을 따릅니다. 차량 대장과
      반드시 같은 규칙이어야 합니다 — 한쪽만 바뀌면 실패하지 않고 조인이 0건이 됩니다.

자격 기준은 Uber 가 정책을 바꿀 때만 움직여서 주 1회로 잡았습니다. 차량 대장
DAG(03:00) · Lyft DAG(04:00) 와 같은 요일에 두되 뒤로 밀어, 같은 날 파티션에
떨어지면서도 세 크롤링이 겹치지 않게 했습니다.

이미 적재된 Bronze 를 다시 변환하려면 수동 트리거하면서 `collected_date`
파라미터에 대상 수집일(예: "2026-08-11")을 넣으세요.
"""

import importlib
import logging
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
# 자격 페이지가 도시마다 다릅니다. 지금 대상은 뉴욕 하나뿐이지만 값을 박아두지 않고
# 파라미터로 빼서, 다른 도시를 볼 때 코드를 고치지 않고 수동 트리거할 수 있게 둡니다.
DEFAULT_CITY_SLUG = os.getenv("UBER_CITY_SLUG", "new-york")


def lambda_handler_for(function_name: str):
    """`lambda`가 파이썬 예약어라 정적 import가 안 돼 동적으로 불러옵니다."""
    module = importlib.import_module(f"lambda.functions.{function_name}.handler")
    return module.lambda_handler


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


default_args = {
    "owner": "DE_team1",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=15),
    "execution_timeout": timedelta(minutes=15),
    "on_failure_callback": slack_failure_callback,
}


@dag(
    dag_id="uber_eligible_vehicles_raw_to_silver_pipeline",
    default_args=default_args,
    description="Uber 배차 가능 차량 목록 Raw -> Bronze -> Silver 수집 및 정제 파이프라인",
    schedule="0 5 * * 1",  # 매주 월요일 05:00 UTC (차량 대장 03:00 / Lyft 04:00 다음)
    start_date=datetime(2026, 8, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["uber_eligible_vehicles", "raw", "bronze", "silver", "lambda"],
    params={
        "collected_date": Param(
            None,
            type=["null", "string"],
            pattern=r"^\d{4}-\d{2}-\d{2}$",
            description=(
                "이미 적재된 Bronze 를 다시 변환할 때만 지정 (예: '2026-08-11'). "
                "비워두면 이번 실행이 적재한 수집일을 그대로 씁니다."
            ),
        ),
        "city_slug": Param(
            DEFAULT_CITY_SLUG,
            type="string",
            description="Uber 자격 페이지의 도시 슬러그 (예: 'new-york')",
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
def uber_eligible_vehicles_raw_to_silver_pipeline():
    @task(task_id="raw_to_bronze")
    def raw_to_bronze_task(**context) -> dict:
        """Uber 자격 페이지를 수집해 Bronze 에 적재합니다."""
        params = context.get("params", {})
        # 이 핸들러는 Bronze 경로를 `base_dir` 로 받습니다. Param 이름은 `bronze_dir`
        # 이지만 event 키는 raw_to_bronze 공통 규칙을 따릅니다.
        result = lambda_handler_for("uber_eligible_vehicles_raw_to_bronze")(
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
        # Bronze 핸들러는 실행 시각으로 파티션을 정하므로 DAG 가 수집일을 따로
        # 계산하면 자정 근처에서 어긋납니다. Bronze 가 알려준 값을 그대로 씁니다.
        collected_date = (params.get("collected_date") or "").strip() or raw_result[
            "collected_date"
        ]

        # Silver 핸들러는 city_slug 를 받지 않습니다. 수집일 파티션 아래의 도시
        # 디렉터리를 전부 훑어서 한 번에 변환합니다.
        result = lambda_handler_for("uber_eligible_vehicles_bronze_to_silver")(
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
        """Bronze 적재 결과가 layout 규칙과 맞는지, 요청한 도시를 긁었는지 봅니다."""
        params = context.get("params", {})
        bronze_dir = params.get("bronze_dir") or DEFAULT_BRONZE_DIR
        requested_city = params.get("city_slug") or DEFAULT_CITY_SLUG
        layout = importlib.import_module(
            "lambda.functions.common.uber_eligible_vehicles_layout"
        )
        loader = importlib.import_module(
            "lambda.functions.uber_eligible_vehicles_raw_to_bronze.loader"
        )
        transformer = importlib.import_module(
            "lambda.functions.uber_eligible_vehicles_bronze_to_silver.transformer"
        )

        parsed = parse_handler_result(result, expected_locations=1)
        collected_date = parse_iso_date(result.get("collected_date"))

        # 요청한 도시와 다른 곳을 긁으면 조인 대상이 통째로 달라집니다.
        if result.get("city_slug") != requested_city:
            raise ValueError(
                f"요청한 도시와 수집한 도시가 다릅니다: "
                f"{requested_city} != {result.get('city_slug')!r}"
            )

        path = parsed.locations[0]
        require_file(path)

        # 파일명이 수집 시각입니다. 여기서 되짚어 layout 이 정한 자리와 맞춰봅니다.
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
        # 검증을 통과했을 때만 Asset 이벤트를 냅니다 — 이걸 적재 태스크에
        # 달면 깨진 Silver 로 vehicle_master 조립이 돌아갑니다.
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
            "lambda.functions.common.uber_eligible_vehicles_layout"
        )
        loader = importlib.import_module(
            "lambda.functions.uber_eligible_vehicles_bronze_to_silver.loader"
        )
        transformer = importlib.import_module(
            "lambda.functions.uber_eligible_vehicles_bronze_to_silver.transformer"
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

    raw_result = raw_to_bronze_task()
    bronze_checked = validate_bronze_task(raw_result)
    silver_result = bronze_to_silver_task(raw_result)
    # Bronze 가 검증을 통과한 뒤에 변환합니다. 안 걸어두면 깨진 Bronze 를 그대로 읽습니다.
    bronze_checked >> silver_result
    validate_silver_task(silver_result)


uber_eligible_vehicles_dag = uber_eligible_vehicles_raw_to_silver_pipeline()
