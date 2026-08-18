"""EIA 휘발유 Bronze 를 일별 단가 CLEAN Silver 로 변환하는 실행·검증 함수.

산출물은 `eia_gas_price/year_month=YYYY-MM/` 입니다.

대상 월을 파라미터로 받는 이유
---------------------------
EIA 파일 하나에 이력이 통째로 들어 있어 **어느 달이든** 만들 수 있습니다. 그래서
"직전 달을 자동으로" 가 아니라 필요한 달을 지정하는 것이 기본 동작입니다.
"""

import calendar
import importlib
import logging
from datetime import datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq
from airflow.sdk import task

from schema.silver.gas_price import SCHEMA
from shared.airflow.common.lambda_runtime import lambda_handler_for
from shared.airflow.common.project_paths import PROJECT_ROOT

logger = logging.getLogger(__name__)

BRONZE_DIR = str(PROJECT_ROOT / "data" / "bronze")
SILVER_DIR = str(PROJECT_ROOT / "data" / "silver")
DATASET = "eia_gas_price"
FILE_NAME = f"{DATASET}.parquet"
# 데이터가 나타내는 달. lambda loader 의 PARTITION_KEY 와 같아야 합니다.
SILVER_PARTITION_KEY = "year_month"

# 휘발유 주간 계열은 약 1주 지연으로 나옵니다. 전력(약 3개월 지연)과 달리 직전 달이면
# 그 달 전 주가 이미 채워져 있어서, 지정이 없으면 직전 달을 씁니다.
PUBLICATION_LAG_MONTHS = 1


def default_year_month(reference: datetime) -> str:
    """지정이 없을 때 채울 달. 휘발유는 직전 달이면 이미 다 나와 있습니다."""
    if reference.tzinfo is None:
        reference = reference.replace(tzinfo=timezone.utc)
    year, month = reference.year, reference.month - PUBLICATION_LAG_MONTHS
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


def silver_file(base_dir: str, year_month: str) -> Path:
    return (
        Path(base_dir) / DATASET / f"{SILVER_PARTITION_KEY}={year_month}" / FILE_NAME
    )


def month_day_count(year_month: str) -> int:
    year, month = (int(part) for part in year_month.split("-"))
    return calendar.monthrange(year, month)[1]


def require_bronze(base_dir: str, year_month: str) -> str:
    """원본이 있는지 변환 **전에** 확인합니다.

    `year_month` 로 원본을 걸러내지 않습니다 — 이력 파일이라 어느 수집분이든 여러 달을
    담고 있고, 실제로 대상 월이 들어있는지는 파일을 열어봐야 알 수 있어서 변환이
    판단합니다(없으면 관측 시작일을 알려주며 실패). 여기서는 존재 여부만 봅니다.
    """
    layout = importlib.import_module("main.aws_lambda.common.eia_fuel_price_layout")
    dag_id = "eia_gas_price_raw_to_bronze_pipeline"
    try:
        _, partition = layout.newest_bronze_partition(base_dir, layout.GAS_DATASET)
    except FileNotFoundError as exc:
        raise FileNotFoundError(f"{exc} — {dag_id} 을 먼저 돌리세요.") from exc

    path = partition / layout.GAS_FILE_NAME
    if not path.is_file():
        raise FileNotFoundError(f"EIA 휘발유 원본이 없습니다: {path} — {dag_id} 을 먼저 돌리세요.")

    logger.info("EIA 휘발유 원본 확인 (%s 대상): %s", year_month, path)
    return str(path)


def validate_silver(base_dir: str, year_month: str) -> None:
    """스키마·행 수·날짜 완결성을 확인합니다.

    날짜가 하루라도 비면 하류의 일자 조인에서 그 날이 통째로 매칭 실패하고, 그건
    실패가 아니라 **조용히 줄어든 집계**로 나타납니다.
    """
    path = silver_file(base_dir, year_month)
    if not path.is_file():
        raise FileNotFoundError(f"휘발유 단가 Silver 가 없습니다: {path}")

    # `pq.read_table` 은 경로의 `year_month=` 를 파티션 컬럼으로 덧붙입니다.
    # 파일에 실제로 쓰인 것만 봐야 하므로 ParquetFile 로 직접 읽습니다.
    table = pq.ParquetFile(path).read()
    if table.schema.names != SCHEMA.names:
        raise ValueError(f"휘발유 단가 Silver 스키마가 다릅니다: {table.schema.names}")

    expected = month_day_count(year_month)
    if table.num_rows != expected:
        raise ValueError(
            f"{year_month} 는 {expected}일이어야 하는데 {table.num_rows}행입니다"
        )
    if len({str(value) for value in table["date"].to_pylist()}) != expected:
        raise ValueError(f"{year_month} 일자에 중복이 있습니다")

    logger.info("EIA 휘발유 단가 Silver 검증 통과: %s rows=%d", path, table.num_rows)


@task(task_id="check_bronze")
def check_bronze_task(**context) -> str:
    year_month = resolve_year_month(context)
    logger.info("EIA 휘발유 단가 대상 월: %s", year_month)
    require_bronze(context["params"]["bronze_dir"], year_month)
    return year_month


@task(task_id="bronze_to_silver")
def bronze_to_silver_task(**context) -> dict:
    params = context["params"]
    year_month = context["task_instance"].xcom_pull(task_ids="check_bronze")

    result = lambda_handler_for("eia_gas_price_bronze_to_silver")(
        event={
            "year_month": year_month,
            "bronze_dir": params["bronze_dir"],
            "silver_dir": params["silver_dir"],
        }
    )
    return {"year_month": year_month, **result}


@task(task_id="validate_silver")
def validate_silver_task(**context) -> None:
    result = context["task_instance"].xcom_pull(task_ids="bronze_to_silver")
    validate_silver(context["params"]["silver_dir"], result["year_month"])
