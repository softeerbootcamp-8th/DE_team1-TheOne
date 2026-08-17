"""HVFHV 기사 배정 Silver DAG의 실행·검증 함수."""

import logging
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from airflow.sdk import task

from common.project_paths import PROJECT_ROOT

logger = logging.getLogger(__name__)

ROOT = PROJECT_ROOT
BRONZE = ROOT / "data" / "bronze"
SILVER = ROOT / "data" / "silver"
DEFAULT_PATHS = {
    "trips_path": str(SILVER / "hvfhv"),
    "preferences_path": str(BRONZE / "driver_preferences.parquet"),
    "company_path": str(ROOT / "data" / "source" / "company"),
    "travel_times_path": str(SILVER / "taxi_zone_travel_times"),
    "output_path": str(SILVER / "hvfhv_driver_trip"),
}
REQUIRED_COLUMNS = {
    "trip_key",
    "driver_id",
    "customer_id",
    "lease_id",
    "taxi_id",
    "pickup_datetime",
    "lease_started_on",
    "lease_ended_on",
    "year_month",
    "snapshot_date",
    "assignment_seed",
    "assignment_version",
    "trip_sequence",
    "deadhead_minutes",
    "preference_score",
    "make_key",
    "model_key",
    "model_year",
}


def available_year_months(trips_path: str | Path) -> list[str]:
    """`trips_path` 에 실제로 있는 `year_month=` 파티션 목록 (오름차순)."""
    return sorted(
        partition.name.removeprefix("year_month=")
        for partition in Path(trips_path).glob("year_month=*")
        if partition.is_dir()
    )


def resolve_target_year_month(
    logical_date: datetime,
    params: dict,
    trips_path: str | Path | None = None,
) -> str:
    """대상 연월을 정합니다. 파라미터가 있으면 그 값, 없으면 **데이터에서** 고릅니다.

    달력으로 직전 달을 계산하면 안 됩니다. TLC 는 두 달쯤 늦게 공개해서
    (2026-08 시점에 `fhvhv_tripdata_2026-07.parquet` 은 403, 2026-06 부터 200)
    직전 달 파티션은 존재한 적이 없고 매달 같은 자리에서 실패합니다. 지연 폭도
    일정하지 않아 "2개월 전" 같은 상수로 두면 그 상수가 다시 틀립니다.

    그래서 있는 것 중 최신을 고르되, **기준일의 직전 달을 넘지 않습니다.** 과거
    날짜로 백필할 때 그때 없던 달이 섞이면 결과를 재현할 수 없기 때문입니다.
    """
    if params.get("year") and params.get("month"):
        return (
            f"{str(params['year']).strip()}-"
            f"{str(params['month']).strip().zfill(2)}"
        )
    if logical_date.tzinfo is None:
        logical_date = logical_date.replace(tzinfo=timezone.utc)

    cap = (logical_date.replace(day=1) - timedelta(days=1)).strftime("%Y-%m")
    if trips_path is None:
        return cap

    available = available_year_months(trips_path)
    usable = [year_month for year_month in available if year_month <= cap]
    if not usable:
        raise FileNotFoundError(
            f"{cap} 이하의 HVFHV Silver 파티션이 없습니다: "
            f"trips_path={trips_path} available={available}"
        )
    return usable[-1]


def resolve_snapshot_date(company_path: str) -> str:
    """실제 존재하는 `snapshot_date=` 파티션 중 최신.

    전에는 지정이 없으면 `{year_month}-01` 을 썼습니다. 그런데 회사 원천은 대상 월과
    무관하게 생성되는 픽스처라(`scripts/synthetic_company_snapshot`), 2025-05 를
    처리하면 있지도 않은 `snapshot_date=2025-05-01` 을 찾아 매번 실패했습니다.
    돌리는 사람이 매번 손으로 넣어줘야 했고 그 사실이 어디에도 적혀 있지 않았습니다.

    존재하는 것 중 최신을 고르면 손으로 안 넣어도 되고, 나중에 스냅샷을 여러 개
    만들어도 자연스럽게 최신을 씁니다. vehicle_master 를 고르는 방식과 같습니다.
    """
    base = Path(company_path)
    partitions = sorted(
        item.name.removeprefix("snapshot_date=")
        for item in base.glob("snapshot_date=*")
        if item.is_dir()
    )
    if not partitions:
        raise FileNotFoundError(
            f"회사 원천 스냅샷이 없습니다: {base} — "
            "`make company-snapshot` 으로 만드세요."
        )
    return partitions[-1]


def validate_input_paths(year_month: str, snapshot_date: str, paths: dict) -> dict:
    date.fromisoformat(snapshot_date)
    datetime.strptime(year_month, "%Y-%m")
    for name in DEFAULT_PATHS:
        if name == "output_path":
            continue
        path = Path(paths[name])
        if name == "trips_path":
            path = path / f"year_month={year_month}"
        elif name == "company_path":
            path = path / f"snapshot_date={snapshot_date}"
        if not path.exists():
            raise FileNotFoundError(
                f"기사 배정 입력 경로가 없습니다: {name}={path}"
            )

    company = Path(paths["company_path"]) / f"snapshot_date={snapshot_date}"
    for filename in (
        "customer.parquet",
        "lease_contract.parquet",
        "taxi.parquet",
    ):
        if not (company / filename).is_file():
            raise FileNotFoundError(
                f"회사 스냅샷 파일이 없습니다: {company / filename}"
            )
    return {"year_month": year_month, "snapshot_date": snapshot_date}


def validate_silver_partition(
    output_dir: str | Path, year_month: str
) -> None:
    partition = Path(output_dir) / f"year_month={year_month}"
    files = sorted(partition.glob("*.parquet"))
    if not files:
        raise ValueError(f"기사 배정 Silver 파일이 없습니다: {partition}")
    tables = [pq.ParquetFile(path).read() for path in files]
    table = pa.concat_tables(tables)
    if table.num_rows == 0:
        raise ValueError(f"기사 배정 Silver 행 수가 0입니다: {partition}")

    if "year_month" not in table.column_names:
        table = table.append_column(
            "year_month", pa.array([year_month] * table.num_rows, pa.string())
        )
    missing = REQUIRED_COLUMNS - set(table.column_names)
    if missing:
        raise ValueError(f"기사 배정 Silver 필수 컬럼 누락: {sorted(missing)}")

    frame = table.to_pandas()
    keys = ["trip_key", "driver_id", "customer_id", "lease_id", "taxi_id"]
    if frame[keys].isna().any().any() or frame["trip_key"].duplicated().any():
        raise ValueError("기사 배정 Silver 키가 null이거나 trip_key가 중복됩니다")
    if set(frame["year_month"]) != {year_month}:
        raise ValueError("기사 배정 Silver 행의 year_month가 파티션과 다릅니다")

    pickup_date = frame["pickup_datetime"].dt.date
    started = frame["lease_started_on"]
    ended = frame["lease_ended_on"]
    if ((pickup_date < started) | (ended.notna() & (pickup_date >= ended))).any():
        raise ValueError("운행일이 대여 계약 기간 밖입니다")


@task(task_id="validate_inputs")
def validate_inputs_task(**context):
    params = context["params"]
    logical_date = context.get("logical_date") or datetime.now(timezone.utc)
    year_month = resolve_target_year_month(
        logical_date, params, params.get("trips_path")
    )
    logger.info("기사 배정 대상 연월: %s", year_month)
    snapshot_date = params.get("snapshot_date") or resolve_snapshot_date(
        params["company_path"]
    )
    logger.info("회사 원천 스냅샷 시점: %s", snapshot_date)
    return validate_input_paths(year_month, snapshot_date, params)


@task(task_id="validate_silver")
def validate_silver_task(**context):
    result = context["task_instance"].xcom_pull(task_ids="validate_inputs")
    validate_silver_partition(
        context["params"]["output_path"], result["year_month"]
    )
