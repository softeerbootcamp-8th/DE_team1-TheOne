"""EIA 연료비 DAG의 실행·검증 함수.

크롤링(`gas_ev_price_bronze_to_silver`)이 **오늘 값만** 모으는 것과 달리, EIA 파일
하나에 이력이 통째로 들어 있어 어느 과거 달이든 만들 수 있습니다. 그래서 이 DAG 는
`year_month` 를 받아 그 달을 채우는 것이 기본 동작입니다.

두 경로가 같은 Silver 자리에 씁니다
---------------------------------
`gas_ev_price/collected_month=YYYY-MM/` — 담당하는 달이 겹치지 않습니다. 크롤링은
수집을 시작한 2026-08 이후, EIA 는 그 이전. 겹칠 때 무엇을 우선할지는 아직 정하지
않았고, 지금은 **덮어쓰기**라 나중에 실행한 쪽이 남습니다.
"""

import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow.parquet as pq
from airflow.sdk import task

from common.lambda_runtime import lambda_handler_for
from common.project_paths import PROJECT_ROOT
from schema.silver.gas_ev_price import EIA, SCHEMA

logger = logging.getLogger(__name__)

BRONZE_DIR = str(PROJECT_ROOT / "data" / "bronze")
SILVER_DIR = str(PROJECT_ROOT / "data" / "silver")
INTEGRATED_DATASET = "gas_ev_price"
INTEGRATED_FILE_NAME = "gas_ev_price.parquet"

# EIA 전력 통계는 약 3개월 늦게 공개됩니다. 지정이 없으면 그만큼 물러선 달을 씁니다 —
# 직전 달을 잡으면 "아직 안 나온 달"을 요구하게 되어 매번 실패합니다.
ELECTRICITY_PUBLICATION_LAG_MONTHS = 3


def default_year_month(reference: datetime) -> str:
    """지정이 없을 때 채울 달. 전력 공개 지연만큼 물러섭니다."""
    if reference.tzinfo is None:
        reference = reference.replace(tzinfo=timezone.utc)
    year, month = reference.year, reference.month - ELECTRICITY_PUBLICATION_LAG_MONTHS
    while month < 1:
        year, month = year - 1, month + 12
    return f"{year:04d}-{month:02d}"


def resolve_year_month(context: dict) -> str:
    configured = (context.get("params") or {}).get("year_month")
    if configured:
        year_month = str(configured).strip()
        datetime.strptime(year_month, "%Y-%m")
        return year_month
    reference = context.get("data_interval_end") or datetime.now(timezone.utc)
    return default_year_month(reference)


def integrated_silver_file(base_dir: str, year_month: str) -> Path:
    return (
        Path(base_dir)
        / INTEGRATED_DATASET
        / f"collected_month={year_month}"
        / INTEGRATED_FILE_NAME
    )


def month_day_count(year_month: str) -> int:
    import calendar

    year, month = (int(part) for part in year_month.split("-"))
    return calendar.monthrange(year, month)[1]


def validate_silver(base_dir: str, year_month: str) -> None:
    """스키마·행 수·날짜 완결성·출처를 확인합니다.

    날짜가 하루라도 비면 Gold 의 일자 조인에서 그 날 운행이 통째로 매칭 실패하고,
    그건 실패가 아니라 **조용히 줄어든 집계**로 나타납니다.
    """
    path = integrated_silver_file(base_dir, year_month)
    if not path.is_file():
        raise FileNotFoundError(f"통합 연료비 Silver 가 없습니다: {path}")

    # `pq.read_table` 은 경로의 `collected_month=` 를 파티션 컬럼으로 덧붙입니다.
    # 파일에 실제로 쓰인 것만 봐야 하므로 ParquetFile 로 직접 읽습니다.
    table = pq.ParquetFile(path).read()
    if table.schema.names != SCHEMA.names:
        raise ValueError(f"통합 Silver 스키마가 다릅니다: {table.schema.names}")

    expected = month_day_count(year_month)
    if table.num_rows != expected:
        raise ValueError(
            f"{year_month} 는 {expected}일이어야 하는데 {table.num_rows}행입니다"
        )

    dates = sorted(str(value) for value in table["date"].to_pylist())
    if len(set(dates)) != expected:
        raise ValueError(f"{year_month} 일자에 중복이 있습니다")

    sources = set(table["price_source"].to_pylist())
    if sources != {EIA}:
        raise ValueError(f"EIA 경로 산출물의 price_source 가 다릅니다: {sources}")

    logger.info("EIA 통합 Silver 검증 통과: %s rows=%d", path, table.num_rows)


@task(task_id="collect_bronze")
def collect_bronze_task(**context) -> dict:
    """EIA 원본 두 개를 각자 데이터셋에 적재합니다.

    한 태스크로 묶은 이유는 둘 다 없으면 변환을 못 하기 때문입니다 — 따로 두면 하나만
    성공한 상태로 다음이 돌다가 더 안쪽에서 실패합니다.
    """
    params = context["params"]
    base_dir = params["bronze_dir"]
    locations = {}
    for name in ("eia_gas_price_raw_to_bronze", "eia_electricity_price_raw_to_bronze"):
        result = lambda_handler_for(name)(event={"base_dir": base_dir})
        locations[name] = result["locations"][0]
        logger.info("%s 적재: %s", name, result["locations"][0])
    return locations


@task(task_id="bronze_to_silver")
def bronze_to_silver_task(**context) -> dict:
    params = context["params"]
    year_month = resolve_year_month(context)
    logger.info("EIA 연료비 대상 월: %s", year_month)

    result = lambda_handler_for("eia_fuel_price_bronze_to_silver")(
        event={
            "year_month": year_month,
            "bronze_dir": params["bronze_dir"],
            "silver_dir": params["silver_dir"],
            "markup": params["markup"],
        }
    )
    return {"year_month": year_month, **result}


@task(task_id="validate_silver")
def validate_silver_task(**context) -> None:
    result = context["task_instance"].xcom_pull(task_ids="bronze_to_silver")
    validate_silver(context["params"]["silver_dir"], result["year_month"])
