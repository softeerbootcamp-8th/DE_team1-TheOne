"""Silver 4종 → Gold DAG의 월 파티션 경로와 산출물을 검증합니다."""

import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from airflow.sdk.exceptions import AirflowSkipException
from airflow.sdk import task

from shared.airflow.common.project_paths import PROJECT_ROOT
from shared.airflow.common.slack_failure_callback import (
    slack_skip_alert_callback,
)
from main.airflow.common import assets
from main.airflow.common.assets import (
    gold_csv_path,
    parse_partition_key,
    resolve_service_area,
    service_area_root,
)
from main.airflow.common.gold_staleness import record_success
from main.airflow.common.monthly_bronze import latest_local_silver_version
from main.common.eia_fuel_version import FUEL_FILE_NAME, fuel_source_tokens
from shared.common.s3_reader import list_keys
from shared.common.success_marker import marker_path

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
DATASETS = (
    "driver_aggregation",
    "driver_car_suggestion",
)
# 산출물마다 "이건 반드시 있어야 한다" 는 컬럼. 전체 스키마는 schema/gold/*.py 가
# 소유하고, 여기서는 조인 키와 판단에 쓰이는 값만 봅니다.
REQUIRED_COLUMNS = {
    "driver_aggregation": {
        "driver_id", "year_month", "monthly_net_profit", "monthly_lease_fee",
    },
    "driver_car_suggestion": {
        "driver_id", "year_month", "vehicle_model_id",
        "manufacturer", "model_name",
        "expected_net_profit_increase", "recommendation_reason",
    },
}
PROD_INPUT_DATASETS = (
    "monthly_taxi_trip",
    "driver_vehicle_monthly_snapshot",
    "lease_vehicle_inventory",
    "gas_ev_price",
)


def available_year_months(
    monthly_taxi_trip_path: str | Path, service_area: str
) -> list[str]:
    """월별 택시 운행 기록 Silver에 실제로 있는 `year_month=` 파티션 목록입니다.

    지역 계층이 들어간 뒤에도 찾을 수 있어야 합니다 — 한 레벨 glob 만 보면 조용히
    빈 목록이 되고, `resolve_target_year_month` 의 수동 실행 폴백이 "파티션이
    없습니다" 로 죽습니다(#851). 여러 후보에서 찾은 월은 합집합으로 모읍니다.
    """
    root = service_area_root(monthly_taxi_trip_path, service_area)
    months = {
        partition.name.removeprefix("year_month=")
        for partition in root.glob("year_month=*")
        if partition.is_dir()
        and _latest_version(partition) is not None
    }
    return sorted(months)


def _latest_version(partition: Path) -> Path | None:
    return latest_local_silver_version(partition)


def _resolve_versioned_input(
    root: str | Path,
    year_month: str,
    *,
    upstream_dag: str,
    service_area: str,
) -> str:
    """지역별 월 파티션에서 최신 Silver 버전을 찾습니다."""
    partition = service_area_root(root, service_area) / f"year_month={year_month}"
    latest = _latest_version(partition)
    if latest is not None:
        return str(latest)
    raise FileNotFoundError(
        f"Silver 버전이 없습니다: {partition}. {upstream_dag} 을 먼저 돌리세요."
    )


def _latest_fuel_price(partition: Path) -> Path | None:
    candidates = []
    for version in partition.glob("input_version=*"):
        source_tokens = fuel_source_tokens(version.name)
        path = version / FUEL_FILE_NAME
        if source_tokens and path.is_file() and marker_path(version).is_file():
            candidates.append((*source_tokens, path))
    return max(candidates, default=(None, None, None))[-1]


def _has_completed_fuel_s3(keys: set[str], prefix: str) -> bool:
    for key in keys:
        relative = key.removeprefix(prefix)
        parts = relative.split("/")
        if len(parts) != 2 or parts[1] != FUEL_FILE_NAME:
            continue
        if (
            fuel_source_tokens(parts[0])
            and f"{key.rsplit('/', 1)[0]}/_SUCCESS" in keys
        ):
            return True
    return False


def resolve_target_year_month(
    logical_date: datetime,
    params: dict,
    monthly_taxi_trip_path: str,
    service_area: str,
    partition_key: str | None = None,
) -> str:
    """대상 연월. 수동 파라미터, Asset 파티션 키, HVFHV 최신 월 순으로 고릅니다.

    최신 월 탐색은 파티션 키가 없는 수동 실행의 폴백입니다. 달력으로 직전 달을
    계산하면 안 됩니다 — 배정이 도는 시점은 TLC 공개 지연에 묶여 있어서
    (`monthly_taxi_trip_raw_to_silver_dag` 참고) 직전 달 파티션이 없는 것이 정상입니다. 있는 것 중
    최신을 고르되 **기준일을 넘지 않습니다.** 과거 날짜로 다시 돌렸을 때 그때 없던
    달이 섞이면 결과를 재현할 수 없습니다.
    """
    year = str(params.get("year") or "").strip()
    month = str(params.get("month") or "").strip()
    if year and month:
        return f"{year}-{month.zfill(2)}"

    if partition_key:
        # 키는 "{service_area}:{year_month}" 복합 문자열입니다(#674). 지역 성분이
        # 없으면 생산자가 아직 안 바뀐 것이라 parse_partition_key 가 요란하게
        # 실패합니다 — 조용히 기본 지역으로 넘기면 그 사실이 묻힙니다.
        _, year_month = parse_partition_key(partition_key)
        return year_month

    if logical_date.tzinfo is None:
        logical_date = logical_date.replace(tzinfo=timezone.utc)
    limit = f"{logical_date.year:04d}-{logical_date.month:02d}"
    candidates = [
        ym
        for ym in available_year_months(monthly_taxi_trip_path, service_area)
        if ym <= limit
    ]
    if not candidates:
        raise FileNotFoundError(
            f"기준일({limit}) 이하의 월별 택시 운행 기록 Silver 파티션이 없습니다: {monthly_taxi_trip_path}. "
            "monthly_taxi_trip_raw_to_silver_pipeline 을 먼저 돌리세요."
        )
    return candidates[-1]


def resolve_target_service_area(params: dict, partition_key: str | None = None) -> str:
    """대상 지역. 파티션 키가 있으면 그것이, 없으면 파라미터가 정합니다.

    **`resolve_target_year_month` 와 우선순위가 반대입니다.** 연월은 수동
    파라미터가 파티션 키를 덮어쓰는데, 지역은 그러면 안 됩니다 — `service_area`
    파라미터는 기본값(`NYC`)이 있어서 Asset 트리거 실행에서도 항상 값이 차 있고,
    파라미터를 우선하면 `"TX:2026-08"` 파티션의 Gold 를 **NYC 로 적재**하게 됩니다.
    연월 파라미터는 기본값이 None 이라 이 문제가 없습니다.
    """
    if partition_key:
        service_area, _ = parse_partition_key(partition_key)
        return service_area
    return resolve_service_area(params)


def resolve_input_paths(
    year_month: str, params: dict, service_area: str
) -> dict:
    """Spark 잡에 넘길 같은 지역·달의 Silver 4종 경로를 확인합니다."""
    datetime.strptime(year_month, "%Y-%m")

    partition = (
        service_area_root(params["monthly_taxi_trip_path"], service_area)
        / f"year_month={year_month}"
    )
    latest = _latest_version(partition)
    monthly_taxi_trip = str(latest) if latest is not None else None
    if monthly_taxi_trip is None:
        raise FileNotFoundError(
            "월별 택시 운행 기록 Silver 버전이 없습니다: "
            f"{partition}. "
            "monthly_taxi_trip_raw_to_silver_pipeline 을 먼저 돌리세요."
        )

    versioned_files = {
        "driver_vehicle_monthly_snapshot_path": "driver_vehicle_monthly_snapshot_raw_to_silver_pipeline",
        "lease_vehicle_inventory_path": "lease_vehicle_inventory_raw_to_silver_pipeline",
    }
    resolved_files = {}
    for key, upstream_dag in versioned_files.items():
        resolved_files[key] = _resolve_versioned_input(
            params[key],
            year_month,
            upstream_dag=upstream_dag,
            service_area=service_area,
        )

    fuel_partition = (
        service_area_root(params["fuel_price_path"], service_area)
        / f"year_month={year_month}"
    )
    fuel_path = _latest_fuel_price(fuel_partition)
    if fuel_path is None:
        raise FileNotFoundError(
            f"Silver 파일이 없습니다: {fuel_partition}. "
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


def validate_prod_input_partitions(
    bucket: str, year_month: str, service_area: str
) -> None:
    """운영 S3에 같은 지역·월의 완료된 Silver 4종이 있는지 확인합니다."""
    missing = []
    for dataset in PROD_INPUT_DATASETS:
        prefix = (
            f"silver/{dataset}/service_area={service_area}/"
            f"year_month={year_month}/"
        )
        keys = set(list_keys(bucket, prefix))
        completed = (
            _has_completed_fuel_s3(keys, prefix)
            if dataset == "gas_ev_price"
            else any(
                key.endswith(".parquet")
                and f"{key.rsplit('/', 1)[0]}/_SUCCESS" in keys
                for key in keys
            )
        )
        if not completed:
            missing.append(dataset)
    if missing:
        raise FileNotFoundError(
            f"Silver 완료본이 없습니다: s3://{bucket}, "
            f"service_area={service_area}, year_month={year_month}, datasets={missing}"
        )


def _notify_slack(callback, context: dict) -> None:
    """Slack 알림은 best-effort입니다 — 실패해도 파이프라인 판정을 막지 않습니다."""
    try:
        callback(context)
    except Exception:
        logger.warning("Slack 알림 전송에 실패했습니다", exc_info=True)


def validate_gold_outputs(
    output_dir: str, year_month: str, service_area: str
) -> None:
    """산출물 2종의 존재·행 수·필수 컬럼을 확인합니다.

    경로는 Spark 쓰기 쪽(`_csv_path`)과 같은 공용 함수로 만듭니다.
    """
    for dataset in DATASETS:
        path = gold_csv_path(output_dir, dataset, year_month, service_area)
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


def validate_triggering_asset_partitions(
    triggering_asset_events, service_area: str, year_month: str
) -> None:
    """소비된 Asset 이벤트가 Gold 실행 대상 지역·연월과 같은지 확인합니다."""
    expected = (service_area, year_month)
    if not triggering_asset_events:
        raise ValueError(
            f"Gold 입력 Asset 이벤트가 없습니다: expected={service_area}:{year_month}"
        )

    for asset, events in triggering_asset_events.items():
        asset_name = getattr(asset, "name", str(asset))
        if not events:
            raise ValueError(f"Gold 입력 Asset 이벤트가 없습니다: asset={asset_name}")
        for event in events:
            partition_key = getattr(event, "partition_key", None)
            try:
                actual = parse_partition_key(partition_key)
            except ValueError as exc:
                raise ValueError(
                    f"Gold 입력 Asset 파티션이 잘못됐습니다: "
                    f"asset={asset_name} partition_key={partition_key!r}"
                ) from exc
            if actual != expected:
                raise ValueError(
                    "Gold 입력 Asset 파티션 불일치: "
                    f"asset={asset_name} expected={service_area}:{year_month} "
                    f"actual={partition_key}"
                )


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
    service_area = resolve_target_service_area(params, partition_key)
    year_month = resolve_target_year_month(
        logical_date,
        params,
        params["monthly_taxi_trip_path"],
        service_area,
        partition_key,
    )
    if partition_key:
        validate_triggering_asset_partitions(
            context.get("triggering_asset_events"), service_area, year_month
        )
    logger.info("Gold 대상: service_area=%s year_month=%s", service_area, year_month)
    try:
        if job_env == "prod":
            validate_prod_input_partitions(
                os.environ["DATA_LAKE_S3_BUCKET"], year_month, service_area
            )
            resolved = {
                "service_area": service_area,
                "year_month": year_month,
                "year": year_month.split("-")[0],
                "month": str(int(year_month.split("-")[1])),
            }
        else:
            resolved = {
                **resolve_input_paths(year_month, params, service_area),
                "service_area": service_area,
            }
    except FileNotFoundError as exc:
        if partition_key:
            _notify_slack(slack_skip_alert_callback, {**context, "exception": exc})
            raise AirflowSkipException(
                f"Silver 4종 준비 대기: year_month={year_month}; {exc}"
            ) from exc
        raise

    return resolved


@task(task_id="validate_gold", outlets=[assets.GOLD_INPUTS_READY])
def validate_gold_task(**context) -> None:
    resolved = context["task_instance"].xcom_pull(task_ids="validate_inputs")
    if os.getenv("SPARK_JOB_ENV", "local") == "prod":
        # 운영은 CSV가 아니라 RDS에 적재합니다 — 검증할 로컬 output_dir이 없습니다.
        logger.info(
            "운영 Gold 검증은 Spark의 RDS 적재 트랜잭션에서 완료했습니다: year_month=%s",
            resolved["year_month"],
        )
    else:
        validate_gold_outputs(
            context["params"]["output_dir"],
            resolved["year_month"],
            resolved["service_area"],
        )
    record_success(
        resolved["service_area"],
        resolved["year_month"],
        datetime.now(timezone.utc),
    )
    assets.publish_month_partition(
        context.get("outlet_events"),
        assets.GOLD_INPUTS_READY,
        resolved["year_month"],
        resolved["service_area"],
    )
