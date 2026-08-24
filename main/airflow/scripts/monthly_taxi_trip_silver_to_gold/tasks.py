"""Silver 4종 → Gold DAG의 월 파티션 경로와 산출물을 검증합니다."""

import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from airflow.sdk.exceptions import AirflowSkipException
from airflow.sdk import Variable, task

from shared.airflow.common.project_paths import PROJECT_ROOT
from shared.airflow.common.slack_failure_callback import (
    slack_skip_alert_callback,
    slack_stale_alert_callback,
)
from main.airflow.common import assets
from main.airflow.common.assets import (
    gold_csv_path,
    parse_partition_key,
    resolve_service_area,
    service_area_root,
)
from main.airflow.common.monthly_bronze import latest_local_silver_version
from main.common.eia_fuel_version import FUEL_FILE_NAME, fuel_source_tokens
from shared.common.s3_reader import list_keys
from shared.common.success_marker import marker_path

logger = logging.getLogger(__name__)

ROOT = PROJECT_ROOT
SILVER = ROOT / "data" / "silver"
# 대상월 계산이 원천 API의 "latest" 해석에 달려 있어 절대 날짜로 SLA를 못 박을 수
# 없다 — 그래서 "직전 성공 이후 N일" 같은 상대 기준을 쓴다. Variable 미설정 시의
# 기본값이라 재배포 없이 운영 중 조정 가능해야 한다.
DEFAULT_STALE_SLA_DAYS = 31
STALE_SLA_DAYS_VARIABLE = "gold_stale_sla_days"
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


def resolve_stale_sla_days(params: dict) -> int:
    """SLA 기준일. Param이 비어 있으면 Variable(재배포 없이 조정), 없으면 기본값.

    Variable 조회가 실행 컨텍스트 밖(예: 단위 테스트)이라 실패해도 SLA 판정
    자체가 파이프라인을 막으면 안 되므로 기본값으로 내려갑니다.
    """
    configured = params.get("gold_stale_sla_days")
    if configured is not None:
        return int(configured)
    try:
        return int(
            Variable.get(STALE_SLA_DAYS_VARIABLE, default=DEFAULT_STALE_SLA_DAYS)
        )
    except Exception:
        logger.warning(
            "Variable(%s) 조회에 실패해 기본값 %s일을 씁니다",
            STALE_SLA_DAYS_VARIABLE,
            DEFAULT_STALE_SLA_DAYS,
            exc_info=True,
        )
        return DEFAULT_STALE_SLA_DAYS


def days_since_last_success(prev_end_date_success, now: datetime) -> int | None:
    """직전 성공 DagRun 종료 이후 지난 일수. 계산할 수 없으면 None.

    staleness 알림은 best-effort입니다 — 운영에서 prev_end_date_success/now 중
    하나가 예상과 달리 None이라 TypeError로 validate_inputs 전체가 죽는 사고가
    실제로 있었습니다. 원인 불문하고 여기서 막습니다.

    `prev_end_date_success`는 `is None`으로 못 거릅니다 — Airflow 3 TaskSDK가
    이전 성공 DagRun이 없을 때도 `None`을 감싼 `lazy_object_proxy.Proxy`를 주고,
    Proxy 객체 자체는 `None`이 아니라서 identity 비교가 항상 실패합니다.
    truthiness(`bool()`)는 Proxy가 감싼 값까지 확인하므로 이걸로 걸러냅니다.
    """
    if not prev_end_date_success or now is None:
        return None
    try:
        return (now - prev_end_date_success).days
    except TypeError:
        logger.warning(
            "days_since_last_success 계산 실패: prev_end_date_success=%r now=%r",
            prev_end_date_success,
            now,
        )
        return None


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


@task(task_id="validate_inputs", outlets=[assets.GOLD_INPUTS_READY])
def validate_inputs_task(**context) -> dict:
    params = context["params"]
    logical_date = context.get("logical_date") or datetime.now(timezone.utc)
    dag_run = context.get("dag_run")
    partition_key = getattr(dag_run, "partition_key", None)
    job_env = os.getenv("SPARK_JOB_ENV", "local")

    stale_days = resolve_stale_sla_days(params)
    days_since = days_since_last_success(
        context.get("prev_end_date_success"), datetime.now(timezone.utc)
    )
    if days_since is not None and days_since > stale_days:
        logger.warning("Gold staleness SLA 초과: %s일 (기준 %s일)", days_since, stale_days)
        _notify_slack(
            slack_stale_alert_callback,
            {**context, "days_since_success": days_since, "stale_days": stale_days},
        )

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

    assets.publish_month_partition(
        context.get("outlet_events"),
        assets.GOLD_INPUTS_READY,
        year_month,
        service_area,
    )
    return resolved


@task(task_id="validate_gold")
def validate_gold_task(**context) -> None:
    resolved = context["task_instance"].xcom_pull(task_ids="validate_inputs")
    if os.getenv("SPARK_JOB_ENV", "local") == "prod":
        # 운영은 CSV가 아니라 RDS에 적재합니다 — 검증할 로컬 output_dir이 없습니다.
        logger.info(
            "운영 Gold 검증은 Spark의 RDS 적재 트랜잭션에서 완료했습니다: year_month=%s",
            resolved["year_month"],
        )
        return
    validate_gold_outputs(
        context["params"]["output_dir"],
        resolved["year_month"],
        resolved["service_area"],
    )
