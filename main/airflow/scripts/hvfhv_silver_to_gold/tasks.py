"""Silver 4종 → Gold DAG의 월 파티션 경로와 산출물을 검증합니다."""

import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from airflow.sdk import task

from shared.airflow.common.project_paths import PROJECT_ROOT

logger = logging.getLogger(__name__)

ROOT = PROJECT_ROOT
SILVER = ROOT / "data" / "silver"
DEFAULT_PATHS = {
    "hvfhv_path": str(SILVER / "hvfhv"),
    "driver_snapshot_path": str(SILVER / "driver_vehicle_monthly_snapshot"),
    "inventory_path": str(SILVER / "lease_vehicle_inventory"),
    "fuel_price_path": str(SILVER / "gas_ev_price"),
    "output_dir": str(ROOT / "data" / "gold"),
}
DATASETS = ("driver_aggregation", "driver_car_suggestion", "monthly_report")
# 산출물마다 "이건 반드시 있어야 한다" 는 컬럼. 전체 스키마는 schema/gold/*.py 가
# 소유하고, 여기서는 조인 키와 판단에 쓰이는 값만 봅니다.
REQUIRED_COLUMNS = {
    "driver_aggregation": {
        "driver_id", "year_month", "monthly_net_profit", "monthly_lease_fee",
    },
    "driver_car_suggestion": {
        "driver_id", "year_month", "vehicle_model_id", "manufacturer", "model_name",
        "expected_net_profit_increase", "recommendation_reason",
    },
    "monthly_report": {
        "year_month", "threshold_profit_increase", "recommended_driver_count",
        "avg_net_profit_increase_per_driver",
    },
}


def available_year_months(hvfhv_path: str | Path) -> list[str]:
    """HVFHV Silver에 실제로 있는 `year_month=` 파티션 목록입니다."""
    return sorted(
        partition.name.removeprefix("year_month=")
        for partition in Path(hvfhv_path).glob("year_month=*")
        if partition.is_dir()
    )


def resolve_target_year_month(logical_date: datetime, params: dict, hvfhv_path: str) -> str:
    """대상 연월. 파라미터가 있으면 그 값, 없으면 HVFHV 최신 월을 고릅니다.

    달력으로 직전 달을 계산하면 안 됩니다 — 배정이 도는 시점은 TLC 공개 지연에
    묶여 있어서(`hvfhv_raw_to_silver_dag` 참고) 직전 달 파티션이 없는 것이 정상입니다.
    있는 것 중 최신을 고르되 **기준일을 넘지 않습니다.** 과거 날짜로 다시 돌렸을 때
    그때 없던 달이 섞이면 결과를 재현할 수 없습니다.
    """
    year = str(params.get("year") or "").strip()
    month = str(params.get("month") or "").strip()
    if year and month:
        return f"{year}-{month.zfill(2)}"

    if logical_date.tzinfo is None:
        logical_date = logical_date.replace(tzinfo=timezone.utc)
    limit = f"{logical_date.year:04d}-{logical_date.month:02d}"
    candidates = [ym for ym in available_year_months(hvfhv_path) if ym <= limit]
    if not candidates:
        raise FileNotFoundError(
            f"기준일({limit}) 이하의 HVFHV Silver 파티션이 없습니다: {hvfhv_path}. "
            "hvfhv_raw_to_silver_pipeline 을 먼저 돌리세요."
        )
    return candidates[-1]


def resolve_input_paths(year_month: str, params: dict) -> dict:
    """Spark 잡에 넘길 같은 달의 Silver 4종 경로를 확인합니다."""
    datetime.strptime(year_month, "%Y-%m")

    hvfhv = Path(params["hvfhv_path"]) / f"year_month={year_month}"
    if not hvfhv.is_dir() or not any(hvfhv.glob("*.parquet")):
        raise FileNotFoundError(
            f"HVFHV Silver 파티션이 없거나 비어 있습니다: {hvfhv}. "
            "hvfhv_raw_to_silver_pipeline 을 먼저 돌리세요."
        )

    monthly_files = {
        "driver_snapshot_path": (
            "driver_vehicle_monthly_snapshot.parquet",
            "driver_vehicle_monthly_snapshot_raw_to_silver_pipeline",
        ),
        "inventory_path": (
            "lease_vehicle_inventory.parquet",
            "lease_vehicle_inventory_raw_to_silver_pipeline",
        ),
        "fuel_price_path": (
            "gas_ev_price.parquet",
            "eia_fuel_price_silver_pipeline",
        ),
    }
    resolved_files = {}
    for key, (file_name, upstream_dag) in monthly_files.items():
        path = Path(params[key]) / f"year_month={year_month}" / file_name
        if not path.is_file():
            raise FileNotFoundError(
                f"Silver 파일이 없습니다: {path}. {upstream_dag} 을 먼저 돌리세요."
            )
        resolved_files[key] = str(path)

    resolved = {
        "year_month": year_month,
        "year": year_month.split("-")[0],
        "month": str(int(year_month.split("-")[1])),
        "hvfhv_path": str(hvfhv),
        **resolved_files,
    }
    logger.info("Gold 입력 확정: %s", resolved)
    return resolved


def validate_gold_outputs(output_dir: str, year_month: str) -> None:
    """산출물 3종의 존재·행 수·필수 컬럼을 확인합니다."""
    for dataset in DATASETS:
        path = Path(output_dir) / dataset / f"year_month={year_month}" / f"{dataset}.csv"
        if not path.is_file():
            raise FileNotFoundError(f"Gold 산출물이 없습니다: {path}")
        frame = pd.read_csv(path)
        if frame.empty:
            raise ValueError(f"Gold 산출물이 비어 있습니다: {path}")
        missing = REQUIRED_COLUMNS[dataset] - set(frame.columns)
        if missing:
            raise ValueError(f"Gold 산출물 필수 컬럼 누락: {dataset}={sorted(missing)}")
        if (frame["year_month"] != year_month).any():
            raise ValueError(f"Gold 산출물에 다른 연월이 섞였습니다: {path}")
        logger.info("Gold 검증 통과: %s rows=%d", dataset, len(frame))


@task(task_id="validate_inputs")
def validate_inputs_task(**context) -> dict:
    params = context["params"]
    logical_date = context.get("logical_date") or datetime.now(timezone.utc)
    year_month = resolve_target_year_month(logical_date, params, params["hvfhv_path"])
    logger.info("Gold 대상 연월: %s", year_month)
    return resolve_input_paths(year_month, params)


@task(task_id="validate_gold")
def validate_gold_task(**context) -> None:
    resolved = context["task_instance"].xcom_pull(task_ids="validate_inputs")
    validate_gold_outputs(context["params"]["output_dir"], resolved["year_month"])
