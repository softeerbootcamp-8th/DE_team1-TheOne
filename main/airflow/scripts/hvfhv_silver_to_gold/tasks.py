"""Silver 4종 → Gold DAG의 월 파티션 경로와 산출물을 검증합니다."""

import logging
import os
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from urllib.parse import urlsplit

import pandas as pd
from airflow.sdk.exceptions import AirflowSkipException
from airflow.sdk import task

from main.airflow.common.monthly_bronze import TIMESTAMP_FILE_PATTERN
from shared.airflow.common.project_paths import PROJECT_ROOT
from shared.common.s3_reader import list_keys

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
S3_INPUT_DATASETS = {
    "hvfhv_path": "hvfhv",
    "driver_snapshot_path": "driver_vehicle_monthly_snapshot",
    "inventory_path": "lease_vehicle_inventory",
    "fuel_price_path": "gas_ev_price",
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


def _s3_root(root: str | Path) -> tuple[str, str] | None:
    parsed = urlsplit(str(root))
    if parsed.scheme != "s3":
        return None
    if not parsed.netloc:
        raise ValueError(f"S3 bucket 이 없습니다: {root}")
    return parsed.netloc, parsed.path.strip("/")


def _s3_partition_objects(
    root: str | Path, year_month: str
) -> tuple[str, str, list[str]] | None:
    location = _s3_root(root)
    if location is None:
        return None
    bucket, root_key = location
    prefix = f"{root_key + '/' if root_key else ''}year_month={year_month}/"
    return bucket, prefix, [
        key
        for key in list_keys(bucket, prefix)
        if PurePosixPath(key).parent.as_posix() == prefix.rstrip("/")
    ]


def _s3_uri(bucket: str, key: str) -> str:
    return f"s3://{bucket}/{key}"


def _dry_run_input_params(params: dict) -> dict:
    """배포 dry-run의 기본 로컬 입력만 같은 bucket의 S3 prefix로 바꿉니다."""
    if params.get("dry_run") is not True or os.getenv("RAW_STORAGE") != "s3":
        return params
    bucket = os.getenv("DATA_LAKE_S3_BUCKET")
    if not bucket:
        raise ValueError("S3 dry-run에는 DATA_LAKE_S3_BUCKET이 필요합니다")
    return {
        **params,
        **{
            key: f"s3://{bucket}/silver/{dataset}"
            for key, dataset in S3_INPUT_DATASETS.items()
            if params.get(key) == DEFAULT_PATHS[key]
        },
    }


def available_year_months(hvfhv_path: str | Path) -> list[str]:
    """HVFHV Silver에 실제로 있는 `year_month=` 파티션 목록입니다."""
    s3_root = _s3_root(hvfhv_path)
    if s3_root is not None:
        bucket, root_key = s3_root
        prefix = f"{root_key + '/' if root_key else ''}year_month="
        months = set()
        for key in list_keys(bucket, prefix):
            suffix = key.removeprefix(prefix)
            if "/" not in suffix:
                continue
            year_month, remainder = suffix.split("/", 1)
            if "/" in remainder:
                continue
            if TIMESTAMP_FILE_PATTERN.fullmatch(remainder) or (
                remainder.startswith("part-") and remainder.endswith(".parquet")
            ):
                months.add(year_month)
        return sorted(months)

    return sorted(
        partition.name.removeprefix("year_month=")
        for partition in Path(hvfhv_path).glob("year_month=*")
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
    s3_objects = _s3_partition_objects(root, year_month)
    if s3_objects is not None:
        bucket, prefix, keys = s3_objects
        versions = sorted(
            key
            for key in keys
            if TIMESTAMP_FILE_PATTERN.fullmatch(PurePosixPath(key).name)
        )
        if versions:
            return _s3_uri(bucket, versions[-1])
        legacy_key = f"{prefix}{legacy_file_name}"
        if legacy_key in keys:
            return _s3_uri(bucket, legacy_key)
        partition = _s3_uri(bucket, prefix.rstrip("/"))
        raise FileNotFoundError(
            f"Silver 버전이 없습니다: {partition}. {upstream_dag} 을 먼저 돌리세요."
        )

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
    hvfhv_path: str,
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

    hvfhv_s3 = _s3_partition_objects(params["hvfhv_path"], year_month)
    if hvfhv_s3 is not None:
        bucket, prefix, keys = hvfhv_s3
        versions = sorted(
            key
            for key in keys
            if TIMESTAMP_FILE_PATTERN.fullmatch(PurePosixPath(key).name)
        )
        if versions:
            hvfhv = _s3_uri(bucket, versions[-1])
        elif any(
            PurePosixPath(key).name.startswith("part-")
            and key.endswith(".parquet")
            for key in keys
        ):
            hvfhv = _s3_uri(bucket, f"{prefix}part-*.parquet")
        else:
            hvfhv = ""
        hvfhv_partition = _s3_uri(bucket, prefix.rstrip("/"))
    else:
        hvfhv_partition = Path(params["hvfhv_path"]) / f"year_month={year_month}"
        latest_hvfhv = _latest_version(hvfhv_partition)
        if latest_hvfhv is not None:
            hvfhv = str(latest_hvfhv)
        elif any(hvfhv_partition.glob("part-*.parquet")):
            # 구 레이아웃의 Spark part 파일만 읽습니다. 같은 디렉터리의 미완료
            # collected_at 파일이 섞이지 않도록 디렉터리 자체를 넘기지 않습니다.
            hvfhv = str(hvfhv_partition / "part-*.parquet")
        else:
            hvfhv = ""
    if not hvfhv:
        raise FileNotFoundError(
            f"HVFHV Silver 버전이 없습니다: {hvfhv_partition}. "
            "hvfhv_raw_to_silver_pipeline 을 먼저 돌리세요."
        )

    versioned_files = {
        "driver_snapshot_path": (
            "driver_vehicle_monthly_snapshot.parquet",
            "driver_vehicle_monthly_snapshot_raw_to_silver_pipeline",
        ),
        "inventory_path": (
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

    fuel_s3 = _s3_partition_objects(params["fuel_price_path"], year_month)
    if fuel_s3 is not None:
        bucket, prefix, keys = fuel_s3
        fuel_key = f"{prefix}gas_ev_price.parquet"
        fuel_path = _s3_uri(bucket, fuel_key)
        fuel_exists = fuel_key in keys
    else:
        fuel_path = (
            Path(params["fuel_price_path"])
            / f"year_month={year_month}"
            / "gas_ev_price.parquet"
        )
        fuel_exists = fuel_path.is_file()
    if not fuel_exists:
        raise FileNotFoundError(
            f"Silver 파일이 없습니다: {fuel_path}. "
            "eia_fuel_price_silver_pipeline 을 먼저 돌리세요."
        )
    resolved_files["fuel_price_path"] = str(fuel_path)

    resolved = {
        "year_month": year_month,
        "year": year_month.split("-")[0],
        "month": str(int(year_month.split("-")[1])),
        "hvfhv_path": hvfhv,
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
    params = _dry_run_input_params(context["params"])
    logical_date = context.get("logical_date") or datetime.now(timezone.utc)
    dag_run = context.get("dag_run")
    partition_key = getattr(dag_run, "partition_key", None)
    year_month = resolve_target_year_month(
        logical_date,
        params,
        params["hvfhv_path"],
        partition_key,
    )
    logger.info("Gold 대상 연월: %s", year_month)
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
    if context["params"].get("dry_run"):
        logger.info("dry-run: Spark job 내부 Gold 검증을 신뢰하고 적재 검증을 생략합니다")
        return

    resolved = context["task_instance"].xcom_pull(task_ids="validate_inputs")
    validate_gold_outputs(context["params"]["output_dir"], resolved["year_month"])
