"""Silver 4종 → Gold DAG의 월 파티션 경로와 산출물을 검증합니다."""

import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from airflow.sdk.exceptions import AirflowSkipException
from airflow.sdk import task

from main.airflow.common.monthly_bronze import TIMESTAMP_FILE_PATTERN
from shared.airflow.common.project_paths import PROJECT_ROOT

logger = logging.getLogger(__name__)

ROOT = PROJECT_ROOT
SILVER = ROOT / "data" / "silver"
DEFAULT_PATHS = {
    "monthly_taxi_trip_path": str(SILVER / "monthly_taxi_trip"),
    "driver_vehicle_monthly_snapshot_path": str(SILVER / "driver_vehicle_monthly_snapshot"),
    "lease_vehicle_inventory_path": str(SILVER / "lease_vehicle_inventory"),
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


def available_year_months(monthly_taxi_trip_path: str | Path) -> list[str]:
    """월별 택시 운행 기록 Silver에 실제로 있는 `year_month=` 파티션 목록입니다."""
    return sorted(
        partition.name.removeprefix("year_month=")
        for partition in Path(monthly_taxi_trip_path).glob("year_month=*")
        if partition.is_dir()
        and (
            _latest_version(partition) is not None
            or any(partition.glob("part-*.parquet"))
        )
    )


def _latest_version(partition: Path) -> Path | None:
    versions = [
        path
        for path in partition.glob("*.parquet")
        if TIMESTAMP_FILE_PATTERN.fullmatch(path.name)
    ]
    return sorted(versions)[-1] if versions else None


def _resolve_versioned_file(
    root: str | Path,
    year_month: str,
    *,
    legacy_file_name: str,
    upstream_dag: str,
) -> str:
    partition = Path(root) / f"year_month={year_month}"
    latest = _latest_version(partition)
    if latest is not None:
        return str(latest)
    legacy = partition / legacy_file_name
    if legacy.is_file():
        return str(legacy)
    raise FileNotFoundError(
        f"Silver 버전이 없습니다: {partition}. {upstream_dag} 을 먼저 돌리세요."
    )


def resolve_target_year_month(
    logical_date: datetime,
    params: dict,
    monthly_taxi_trip_path: str,
    partition_key: str | None = None,
) -> str:
    """대상 연월. 수동 파라미터, Asset 파티션 키, HVFHV 최신 월 순으로 고릅니다.

    최신 월 탐색은 파티션 키가 없는 수동 실행의 폴백입니다. 달력으로 직전 달을
    계산하면 안 됩니다 — 배정이 도는 시점은 TLC 공개 지연에 묶여 있어서
    (`hvfhv_raw_to_silver_dag` 참고) 직전 달 파티션이 없는 것이 정상입니다. 있는 것 중
    최신을 고르되 **기준일을 넘지 않습니다.** 과거 날짜로 다시 돌렸을 때 그때 없던
    달이 섞이면 결과를 재현할 수 없습니다.
    """
    year = str(params.get("year") or "").strip()
    month = str(params.get("month") or "").strip()
    if year and month:
        return f"{year}-{month.zfill(2)}"

    if partition_key:
        datetime.strptime(partition_key, "%Y-%m")
        return partition_key

    if logical_date.tzinfo is None:
        logical_date = logical_date.replace(tzinfo=timezone.utc)
    limit = f"{logical_date.year:04d}-{logical_date.month:02d}"
    candidates = [ym for ym in available_year_months(monthly_taxi_trip_path) if ym <= limit]
    if not candidates:
        raise FileNotFoundError(
            f"기준일({limit}) 이하의 월별 택시 운행 기록 Silver 파티션이 없습니다: {monthly_taxi_trip_path}. "
            "hvfhv_raw_to_silver_pipeline 을 먼저 돌리세요."
        )
    return candidates[-1]


def resolve_input_paths(year_month: str, params: dict) -> dict:
    """Spark 잡에 넘길 같은 달의 Silver 4종 경로를 확인합니다."""
    datetime.strptime(year_month, "%Y-%m")

    monthly_taxi_trip_partition = (
        Path(params["monthly_taxi_trip_path"]) / f"year_month={year_month}"
    )
    latest_monthly_taxi_trip = _latest_version(monthly_taxi_trip_partition)
    if latest_monthly_taxi_trip is not None:
        monthly_taxi_trip = str(latest_monthly_taxi_trip)
    elif any(monthly_taxi_trip_partition.glob("part-*.parquet")):
        # 구 레이아웃의 Spark part 파일만 읽습니다. 같은 디렉터리의 미완료
        # collected_at 파일이 섞이지 않도록 디렉터리 자체를 넘기지 않습니다.
        monthly_taxi_trip = str(monthly_taxi_trip_partition / "part-*.parquet")
    else:
        raise FileNotFoundError(
            f"월별 택시 운행 기록 Silver 버전이 없습니다: {monthly_taxi_trip_partition}. "
            "hvfhv_raw_to_silver_pipeline 을 먼저 돌리세요."
        )

    versioned_files = {
        "driver_vehicle_monthly_snapshot_path": (
            "driver_vehicle_monthly_snapshot.parquet",
            "driver_vehicle_monthly_snapshot_raw_to_silver_pipeline",
        ),
        "lease_vehicle_inventory_path": (
            "lease_vehicle_inventory.parquet",
            "lease_vehicle_inventory_raw_to_silver_pipeline",
        ),
    }
    resolved_files = {}
    for key, (file_name, upstream_dag) in versioned_files.items():
        resolved_files[key] = _resolve_versioned_file(
            params[key],
            year_month,
            legacy_file_name=file_name,
            upstream_dag=upstream_dag,
        )

    fuel_path = (
        Path(params["fuel_price_path"])
        / f"year_month={year_month}"
        / "gas_ev_price.parquet"
    )
    if not fuel_path.is_file():
        raise FileNotFoundError(
            f"Silver 파일이 없습니다: {fuel_path}. "
            "eia_fuel_price_silver_pipeline 을 먼저 돌리세요."
        )
    resolved_files["fuel_price_path"] = str(fuel_path)

    resolved = {
        "year_month": year_month,
        "year": year_month.split("-")[0],
        "month": str(int(year_month.split("-")[1])),
        "monthly_taxi_trip_path": monthly_taxi_trip,
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
    dag_run = context.get("dag_run")
    partition_key = getattr(dag_run, "partition_key", None)
    job_env = os.getenv("SPARK_JOB_ENV", "local")
    if job_env == "prod" and not partition_key and not (
        params.get("year") and params.get("month")
    ):
        raise ValueError("운영 수동 실행은 year와 month를 함께 지정해야 합니다")
    year_month = resolve_target_year_month(
        logical_date,
        params,
        params["monthly_taxi_trip_path"],
        partition_key,
    )
    logger.info("Gold 대상 연월: %s", year_month)
    if job_env == "prod":
        return {
            "year_month": year_month,
            "year": year_month.split("-")[0],
            "month": str(int(year_month.split("-")[1])),
        }
    try:
        return resolve_input_paths(year_month, params)
    except FileNotFoundError as exc:
        if partition_key:
            raise AirflowSkipException(
                f"Silver 4종 준비 대기: year_month={year_month}; {exc}"
            ) from exc
        raise


@task(task_id="validate_gold")
def validate_gold_task(**context) -> None:
    resolved = context["task_instance"].xcom_pull(task_ids="validate_inputs")
    if context["params"].get("dry_run") is True:
        logger.info(
            "dry-run: Spark 내부 Gold 검증 완료, 적재 검증을 생략합니다: year_month=%s",
            resolved["year_month"],
        )
        return
    if os.getenv("SPARK_JOB_ENV", "local") == "prod":
        # 운영은 CSV가 아니라 RDS에 적재합니다 — 검증할 로컬 output_dir이 없습니다.
        logger.info(
            "운영 Gold 검증은 Spark의 RDS 적재 트랜잭션에서 완료했습니다: year_month=%s",
            resolved["year_month"],
        )
        return
    validate_gold_outputs(context["params"]["output_dir"], resolved["year_month"])
