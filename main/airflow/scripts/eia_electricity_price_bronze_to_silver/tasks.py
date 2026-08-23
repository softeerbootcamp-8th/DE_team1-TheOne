"""EIA 전력 Bronze 를 일별 충전 단가 CLEAN Silver 로 변환하는 실행·검증 함수.

산출물은 `eia_electricity_price/year_month=YYYY-MM/` 입니다.

대상 월을 파라미터로 받는 이유
---------------------------
EIA 파일 하나에 이력이 통째로 들어 있어 **어느 달이든** 만들 수 있습니다. 그래서
"직전 달을 자동으로" 가 아니라 필요한 달을 지정하는 것이 기본 동작입니다.
"""

import calendar
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from airflow.sdk import task

from main.airflow.common.assets import join_segments, service_area_segment
from schema.silver import CLEAN_EV_CHARGING_PRICE_SCHEMA
from shared.airflow.common.lambda_runtime import lambda_handler_for
from shared.airflow.common.project_paths import PROJECT_ROOT
from shared.airflow.common.validation import (
    S3Location,
    commit_staged_file,
    layout_tail,
    parse_handler_result,
    parse_location,
    parse_year_month,
    read_parquet,
)

logger = logging.getLogger(__name__)

BRONZE_DIR = str(PROJECT_ROOT / "data" / "bronze")
SILVER_DIR = str(PROJECT_ROOT / "data" / "silver")
DATASET = "eia_electricity_price"
FILE_NAME = f"{DATASET}.parquet"
# 데이터가 나타내는 달. lambda loader 의 PARTITION_KEY 와 같아야 합니다.
SILVER_PARTITION_KEY = "year_month"

# EIA 전력 통계는 약 3개월 늦게 공개됩니다. 지정이 없으면 그만큼 물러선 달을 씁니다 —
# 직전 달을 잡으면 "아직 안 나온 달"을 요구하게 되어 매번 실패합니다.
PUBLICATION_LAG_MONTHS = 3


def default_year_month(reference: datetime) -> str:
    """지정이 없을 때 채울 달. 전력 공개 지연만큼 물러섭니다."""
    if reference.tzinfo is None:
        reference = reference.replace(tzinfo=timezone.utc)
    year, month = reference.year, reference.month - PUBLICATION_LAG_MONTHS
    while month < 1:
        year, month = year - 1, month + 12
    return f"{year:04d}-{month:02d}"


def resolve_year_month(context: dict) -> str:
    params = context.get("params") or {}
    year = str(params.get("year") or "").strip()
    month = str(params.get("month") or "").strip()
    if bool(year) != bool(month):
        raise ValueError("year와 month는 함께 지정해야 합니다")
    if year:
        year_month = f"{year}-{month.zfill(2)}"
        datetime.strptime(year_month, "%Y-%m")
        return year_month
    reference = context.get("data_interval_end") or datetime.now(timezone.utc)
    return default_year_month(reference)


def silver_file(base_dir: str, year_month: str, service_area: str | None = None) -> Path:
    dataset_root = Path(base_dir) / DATASET
    area = service_area_segment(service_area)
    return (
        (dataset_root / area if area else dataset_root)
        / f"{SILVER_PARTITION_KEY}={year_month}"
        / FILE_NAME
    )


def silver_key(year_month: str, service_area: str | None = None) -> str:
    return join_segments(
        "silver",
        DATASET,
        service_area_segment(service_area),
        f"{SILVER_PARTITION_KEY}={year_month}",
        FILE_NAME,
    )


def staged_silver_file(
    base_dir: str, year_month: str, service_area: str | None = None
) -> Path:
    """검증 전 위치. lambda loader의 `staged_silver_file`과 같은 규칙이어야
    합니다(#757) — 어긋나면 이 검증이 엉뚱한 자리를 보고도 통과합니다."""
    final = silver_file(base_dir, year_month, service_area)
    return final.parent / ".staging" / final.name


def staged_silver_key(year_month: str, service_area: str | None = None) -> str:
    final = silver_key(year_month, service_area)
    parent, name = final.rsplit("/", 1)
    return f"{parent}/.staging/{name}"


def month_day_count(year_month: str) -> int:
    year, month = (int(part) for part in year_month.split("-"))
    return calendar.monthrange(year, month)[1]


def validate_silver(result: object, service_area: str | None = None) -> None:
    """스키마·행 수·날짜 완결성을 확인합니다.

    날짜가 하루라도 비면 하류의 일자 조인에서 그 날이 통째로 매칭 실패하고, 그건
    실패가 아니라 **조용히 줄어든 집계**로 나타납니다.
    """
    year_month = parse_year_month(
        result.get("year_month") if isinstance(result, dict) else None,
        "year_month",
    )
    expected = month_day_count(year_month)
    parsed = parse_handler_result(result, expected_locations=1)
    path = parsed.locations[0]
    # 검증 전이라 아직 staged 위치입니다 — 최종 위치는 검증 통과 후 commit_staged_file
    # 로만 채워집니다(#757).
    expected_path = staged_silver_file("", year_month, service_area)
    if layout_tail(path, service_area=service_area) != layout_tail(
        expected_path, service_area=service_area
    ):
        raise ValueError(f"충전 단가 Silver 경로 규칙이 다릅니다: {path}")

    # `pq.read_table` 은 경로의 `year_month=` 를 파티션 컬럼으로 덧붙입니다.
    # 파일에 실제로 쓰인 것만 봐야 하므로 ParquetFile 로 직접 읽습니다.
    try:
        table = read_parquet(path)
    except FileNotFoundError as exc:
        raise FileNotFoundError(f"충전 단가 Silver 가 없습니다: {path}") from exc
    if table.schema.names != CLEAN_EV_CHARGING_PRICE_SCHEMA.names:
        raise ValueError(f"충전 단가 Silver 스키마가 다릅니다: {table.schema.names}")

    if table.num_rows != parsed.row_count:
        raise ValueError(
            f"충전 단가 Silver 파일은 {table.num_rows}행인데 "
            f"handler는 {parsed.row_count}행을 반환했습니다"
        )
    if table.num_rows != expected:
        raise ValueError(
            f"{year_month} 는 {expected}일이어야 하는데 {table.num_rows}행입니다"
        )
    if len({str(value) for value in table["date"].to_pylist()}) != expected:
        raise ValueError(f"{year_month} 일자에 중복이 있습니다")

    logger.info("EIA 충전 단가 Silver 검증 통과: %s rows=%d", path, table.num_rows)


@task(task_id="bronze_to_silver")
def bronze_to_silver_task(**context) -> dict:
    params = context["params"]
    year_month = resolve_year_month(context)
    logger.info("EIA 충전 단가 대상 월: %s", year_month)

    event = {
        "year_month": year_month,
        "bronze_dir": params["bronze_dir"],
        "silver_dir": params["silver_dir"],
        "markup": params["markup"],
        "service_area": params["service_area"],
    }
    result = lambda_handler_for("eia_electricity_price_bronze_to_silver")(
        event=event
    )
    return {"year_month": year_month, **result}


@task(task_id="validate_silver")
def validate_silver_task(**context) -> None:
    result = context["task_instance"].xcom_pull(task_ids="bronze_to_silver")
    service_area = context["params"]["service_area"]
    validate_silver(result, service_area)

    # 검증을 통과했으니 이제 최종 경로로 승격합니다 — 그 전에는 실패해도 최종
    # 경로가 이전 상태 그대로 남습니다(#757).
    year_month = result["year_month"]
    staged = parse_location(result["locations"][0])
    storage = os.getenv("BRONZE_STORAGE", "local")
    bucket = os.getenv("DATA_LAKE_S3_BUCKET")
    final = (
        S3Location(bucket, silver_key(year_month, service_area))
        if storage == "s3"
        else silver_file(context["params"]["silver_dir"], year_month, service_area)
    )
    commit_staged_file(staged, final)
