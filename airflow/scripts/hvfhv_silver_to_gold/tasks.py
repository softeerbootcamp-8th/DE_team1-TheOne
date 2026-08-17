"""Silver → Gold DAG의 실행·검증 함수.

입력 세 개의 경로 규칙이 서로 다릅니다.

    hvfhv_driver_trip   <silver>/hvfhv_driver_trip/year_month=YYYY-MM/
    vehicle_master      <silver>/vehicle_master/collected_date=YYYY-MM-DD/city=<도시>/vehicle_master.parquet
    gas_ev_price        <silver>/gas_ev_price/year_month=YYYY-MM/gas_ev_price.parquet
"""

import logging
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
from airflow.sdk import task

from common.project_paths import PROJECT_ROOT

logger = logging.getLogger(__name__)

ROOT = PROJECT_ROOT
SILVER = ROOT / "data" / "silver"
DEFAULT_PATHS = {
    "trips_path": str(SILVER / "hvfhv_driver_trip"),
    "vehicle_master_path": str(SILVER / "vehicle_master"),
    "gas_ev_price_path": str(SILVER / "gas_ev_price"),
    "output_dir": str(ROOT / "data" / "gold"),
}
DATASETS = ("driver_aggregation", "driver_car_suggestion", "monthly_report")
# 산출물마다 "이건 반드시 있어야 한다" 는 컬럼. 전체 스키마는 schema/gold/*.py 가
# 소유하고, 여기서는 조인 키와 판단에 쓰이는 값만 봅니다.
REQUIRED_COLUMNS = {
    "driver_aggregation": {
        "driver_id", "year_month", "monthly_net_profit", "monthly_rental_fee",
    },
    "driver_car_suggestion": {
        "driver_id", "year_month", "recommended_make_key", "recommended_model_key",
        "expected_net_profit_increase", "recommendation_reason",
    },
    "monthly_report": {
        "year_month", "threshold_profit_increase", "recommended_driver_count",
        "avg_net_profit_increase_per_driver",
    },
}


def available_year_months(trips_path: str | Path) -> list[str]:
    """`trips_path` 에 실제로 있는 `year_month=` 파티션 목록 (오름차순)."""
    return sorted(
        partition.name.removeprefix("year_month=")
        for partition in Path(trips_path).glob("year_month=*")
        if partition.is_dir()
    )


def resolve_target_year_month(logical_date: datetime, params: dict, trips_path: str) -> str:
    """대상 연월. 파라미터가 있으면 그 값, 없으면 **배정 결과에서** 고릅니다.

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
    candidates = [ym for ym in available_year_months(trips_path) if ym <= limit]
    if not candidates:
        raise FileNotFoundError(
            f"기준일({limit}) 이하의 기사 배정 파티션이 없습니다: {trips_path}. "
            "hvfhv_driver_trip_silver_pipeline 을 먼저 돌리세요."
        )
    return candidates[-1]


def resolve_vehicle_master_file(dataset_dir: str | Path, as_of: date) -> Path:
    """대상 월에 쓸 vehicle_master 파일. `as_of` 이하 최신을 우선합니다.

    `as_of` 이후 수집분을 우선 배제하는 이유는, 그때 없던 차량이 추천 후보로 섞이면
    과거 달을 다시 돌렸을 때 결과가 달라지기 때문입니다.

    그런데 **없으면 실패시키지 않고 가장 오래된 것으로 물러섭니다.** 마스터 수집은
    2026-08 에 시작했고 TLC 는 두 달쯤 늦게 공개해서, "대상 월 >= 마스터 수집일" 인
    조합이 당분간 생기지 않습니다. 엄격히 막으면 만들 수 있는 달이 하나도 없습니다.
    대신 그 경우를 경고로 남깁니다 — 렌탈 카탈로그는 시점별 보유 현황이 아니라 스펙
    목록이라 시점 차이의 영향이 작지만, 결과에 다른 시점이 섞였다는 사실은 남아야 합니다.

    도시가 여러 개면 어느 쪽을 쓸지 정할 근거가 없으므로 조용히 고르지 않고 실패시킵니다.
    """
    dataset_dir = Path(dataset_dir)
    partitions = []
    for partition in dataset_dir.glob("collected_date=*"):
        if not partition.is_dir():
            continue
        try:
            partition_date = date.fromisoformat(partition.name.removeprefix("collected_date="))
        except ValueError:
            continue
        partitions.append((partition_date, partition))
    if not partitions:
        raise FileNotFoundError(
            f"vehicle_master 파티션이 없습니다: {dataset_dir}. "
            "vehicle_master_silver_pipeline 을 먼저 돌리세요."
        )

    within = [item for item in partitions if item[0] <= as_of]
    if within:
        latest = max(within, key=lambda item: item[0])[1]
    else:
        chosen_date, latest = min(partitions, key=lambda item: item[0])
        logger.warning(
            "대상 월(%s) 이하의 vehicle_master 가 없어 %s 수집분을 씁니다. "
            "그 시점에 없던 차량이 추천 후보에 섞일 수 있습니다.",
            as_of.isoformat(), chosen_date.isoformat(),
        )

    files = sorted(latest.glob("city=*/vehicle_master.parquet"))
    if not files:
        raise FileNotFoundError(f"vehicle_master 도시 파티션이 비어 있습니다: {latest}")
    if len(files) > 1:
        raise ValueError(
            "vehicle_master 도시가 여러 개라 하나를 고를 수 없습니다. "
            f"vehicle_master_path 로 파일을 직접 지정하세요: {[str(f) for f in files]}"
        )
    return files[0]


def resolve_input_paths(year_month: str, params: dict) -> dict:
    """Spark 잡에 넘길 입력 경로 3개를 확정하고 존재를 확인합니다."""
    datetime.strptime(year_month, "%Y-%m")

    trips = Path(params["trips_path"]) / f"year_month={year_month}"
    if not trips.is_dir():
        raise FileNotFoundError(
            f"기사 배정 Silver 파티션이 없습니다: {trips}. "
            "hvfhv_driver_trip_silver_pipeline 을 먼저 돌리세요."
        )

    # 대상 월 말일 기준. 그 달 이후에 수집된 마스터를 끌어오면 그때 없던 차량이 섞입니다.
    year, month = (int(part) for part in year_month.split("-"))
    as_of = date(year + month // 12, month % 12 + 1, 1) - timedelta(days=1)
    vehicle_master = Path(params["vehicle_master_path"])
    if vehicle_master.is_dir():
        vehicle_master = resolve_vehicle_master_file(vehicle_master, as_of)
    if not vehicle_master.is_file():
        raise FileNotFoundError(f"vehicle_master Silver 파일이 없습니다: {vehicle_master}")

    gas_ev_price = Path(params["gas_ev_price_path"])
    if gas_ev_price.is_dir():
        gas_ev_price = gas_ev_price / f"year_month={year_month}" / "gas_ev_price.parquet"
    if not gas_ev_price.is_file():
        raise FileNotFoundError(
            f"연료비 Silver 파일이 없습니다: {gas_ev_price}. "
            "eia_fuel_price_bronze_to_silver_pipeline 을 대상 월로 먼저 돌리세요."
        )

    resolved = {
        "year_month": year_month,
        "year": year_month.split("-")[0],
        "month": str(int(year_month.split("-")[1])),
        "trips_path": str(trips),
        "vehicle_master_path": str(vehicle_master),
        "gas_ev_price_path": str(gas_ev_price),
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
    year_month = resolve_target_year_month(logical_date, params, params["trips_path"])
    logger.info("Gold 대상 연월: %s", year_month)
    return resolve_input_paths(year_month, params)


@task(task_id="validate_gold")
def validate_gold_task(**context) -> None:
    resolved = context["task_instance"].xcom_pull(task_ids="validate_inputs")
    validate_gold_outputs(context["params"]["output_dir"], resolved["year_month"])
