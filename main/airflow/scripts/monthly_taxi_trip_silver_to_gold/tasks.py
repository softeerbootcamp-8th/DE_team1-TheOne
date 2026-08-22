"""Silver 4종 → Gold DAG의 월 파티션 경로와 산출물을 검증합니다."""

import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import psycopg2
from airflow.sdk.exceptions import AirflowSkipException
from airflow.sdk import Variable, task

from main.airflow.common.monthly_bronze import TIMESTAMP_FILE_PATTERN
from shared.airflow.common.project_paths import PROJECT_ROOT
from shared.airflow.common.slack_failure_callback import (
    slack_skip_alert_callback,
    slack_stale_alert_callback,
)

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
        "year_month", "threshold_profit_increase", "is_rerun", "recommended_driver_count",
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
    (`monthly_taxi_trip_raw_to_silver_dag` 참고) 직전 달 파티션이 없는 것이 정상입니다. 있는 것 중
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
            "monthly_taxi_trip_raw_to_silver_pipeline 을 먼저 돌리세요."
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
            "monthly_taxi_trip_raw_to_silver_pipeline 을 먼저 돌리세요."
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


def _monthly_report_exists_in_postgres(year_month: str) -> bool:
    """운영 Gold DB에 이 대상월 `monthly_report` 행이 이미 있는지.

    관측용 판정이라 실패해도 파이프라인을 막지 않고 "최초완료"(False)로 내려갑니다 —
    첫 실행이라 테이블이 없는 경우도 이 경로로 자연스럽게 False가 됩니다.
    """
    dsn = os.getenv("GOLD_DATABASE_URL")
    if not dsn:
        return False
    try:
        conn = psycopg2.connect(dsn)
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT 1 FROM monthly_report WHERE year_month = %s LIMIT 1",
                    (year_month,),
                )
                return cursor.fetchone() is not None
        finally:
            conn.close()
    except Exception:
        logger.warning(
            "재트리거 판정용 Postgres 조회에 실패해 최초완료로 간주합니다", exc_info=True
        )
        return False


def resolve_is_rerun(job_env: str, year_month: str, params: dict) -> bool:
    """대상월 Gold가 이미 완료된 뒤의 재트리거인지. 기존 산출물 존재로 판정합니다."""
    if job_env == "prod":
        return _monthly_report_exists_in_postgres(year_month)
    path = (
        Path(params["output_dir"])
        / "monthly_report"
        / f"year_month={year_month}"
        / "monthly_report.csv"
    )
    return path.is_file()


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
            "is_rerun": resolve_is_rerun(job_env, year_month, params),
        }
    try:
        return {
            **resolve_input_paths(year_month, params),
            "is_rerun": resolve_is_rerun(job_env, year_month, params),
        }
    except FileNotFoundError as exc:
        if partition_key:
            _notify_slack(slack_skip_alert_callback, {**context, "exception": exc})
            raise AirflowSkipException(
                f"Silver 4종 준비 대기: year_month={year_month}; {exc}"
            ) from exc
        raise


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
    validate_gold_outputs(context["params"]["output_dir"], resolved["year_month"])
