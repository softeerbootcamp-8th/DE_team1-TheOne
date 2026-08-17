"""기사 운행 이력 Silver DAG의 실행·검증 함수."""

import logging
import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from airflow.sdk import task

from common.project_paths import PROJECT_ROOT
# 검증이 볼 컬럼은 생산자와 같은 곳에서 가져옵니다. 여기 다시 적으면 Spark job 이
# 컬럼을 바꿔도 이 목록만 옛 표를 보고 통과시킵니다 (#466).
from schema.silver.hvfhv_driver_trip import KEY_COLUMNS, REQUIRED_COLUMNS

logger = logging.getLogger(__name__)

ROOT = PROJECT_ROOT
SILVER = ROOT / "data" / "silver"
DEFAULT_PATHS = {
    "trips_path": os.getenv("SILVER_DIR", str(SILVER / "hvfhv")),
    # 생산자(`driver_master_raw_to_silver`)가 같은 env 로 쓰기 경로를 바꿉니다.
    # 여기만 하드코딩하면 그 env 를 켜는 순간 소비자가 빈 자리를 봅니다.
    "leases_path": os.getenv(
        "DRIVER_MASTER_SILVER_DIR", str(SILVER / "driver_vehicle_leases")
    ),
    "output_path": str(SILVER / "hvfhv_driver_trip"),
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


def validate_input_paths(year_month: str, snapshot_date: str, paths: dict) -> dict:
    """두 Clean Silver 의 대상 월 파티션이 모두 있어야 합니다.

    한쪽만 있으면 Spark 가 조인 단계까지 가서야 죽습니다. 그때는 이미 운행 파티션을
    통째로 읽은 뒤라, 없는 경로 하나를 알려주는 데 몇 분이 듭니다.
    """
    date.fromisoformat(snapshot_date)
    datetime.strptime(year_month, "%Y-%m")
    for name in ("trips_path", "leases_path"):
        partition = Path(paths[name]) / f"year_month={year_month}"
        if not partition.exists():
            raise FileNotFoundError(
                f"기사 운행 이력 입력 파티션이 없습니다: {name}={partition}"
            )
    return {"year_month": year_month, "snapshot_date": snapshot_date}


def validate_silver_partition(
    output_dir: str | Path, year_month: str
) -> None:
    partition = Path(output_dir) / f"year_month={year_month}"
    files = sorted(partition.glob("*.parquet"))
    if not files:
        raise ValueError(f"기사 운행 이력 Silver 파일이 없습니다: {partition}")
    tables = [pq.ParquetFile(path).read() for path in files]
    table = pa.concat_tables(tables)
    if table.num_rows == 0:
        raise ValueError(f"기사 운행 이력 Silver 행 수가 0입니다: {partition}")

    if "year_month" not in table.column_names:
        table = table.append_column(
            "year_month", pa.array([year_month] * table.num_rows, pa.string())
        )
    missing = REQUIRED_COLUMNS - set(table.column_names)
    if missing:
        raise ValueError(f"기사 운행 이력 Silver 필수 컬럼 누락: {sorted(missing)}")

    frame = table.to_pandas()
    keys = list(KEY_COLUMNS)
    if frame[keys].isna().any().any() or frame["trip_key"].duplicated().any():
        raise ValueError("기사 운행 이력 Silver 키가 null이거나 trip_key가 중복됩니다")
    if set(frame["year_month"]) != {year_month}:
        raise ValueError("기사 운행 이력 Silver 행의 year_month가 파티션과 다릅니다")

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
    logger.info("기사 운행 이력 대상 연월: %s", year_month)
    # 리스 Clean Silver 는 `year_month` 파티션 하나가 그 달 1일 스냅샷입니다
    # (`driver_assignment/source_job.py` 가 그렇게 만듭니다). 파라미터로 받으면
    # 아무 경로도 고르지 않으면서 계보 컬럼만 틀리게 찍힙니다 — 실패 없이 통과합니다.
    #
    # #478 이 여기에 `resolve_snapshot_date(company_path)` 를 넣었는데, 그건 이 DAG 이
    # 회사 스냅샷을 직접 읽던 시절의 처방입니다. 지금은 회사 원천을 안 봅니다 —
    # 그 픽스처는 가짜 데이터 API 쪽(`synthetic_driver_trip_source`)에서만 쓰이고,
    # 이 DAG 의 입력은 월 파티션이 보장된 두 Clean Silver 뿐입니다. #478 이 고치려던
    # "없는 snapshot_date= 파티션을 찾아 실패" 자체가 성립하지 않습니다.
    return validate_input_paths(year_month, f"{year_month}-01", params)


@task(task_id="validate_silver")
def validate_silver_task(**context):
    result = context["task_instance"].xcom_pull(task_ids="validate_inputs")
    validate_silver_partition(
        context["params"]["output_path"], result["year_month"]
    )
