"""차량 마스터 Silver DAG의 실행·검증 함수."""

import importlib
import logging
import os
from datetime import timedelta

from airflow.sdk import task

from schema.source import VEHICLE_MASTER_REQUIRED_NON_NULL
from sub.airflow.common import assets
from shared.airflow.common.lambda_runtime import lambda_handler_for
from shared.airflow.common.project_paths import PROJECT_ROOT
from shared.airflow.common.slack_failure_callback import slack_failure_callback
from shared.airflow.common.validation import parse_handler_result, parse_iso_date, read_parquet


logger = logging.getLogger(__name__)

DEFAULT_SILVER_DIR = os.getenv("SILVER_DIR", str(PROJECT_ROOT / "data" / "silver"))

MAX_SOURCE_AGE_DAYS = {
    "vehicle_catalog": 14,
    "uber_eligible_vehicles": 14,
    "lyft_eligible_vehicles": 14,
    "fueleconomy_vehicle_specs": 45,
}


@task(task_id="build_vehicle_master")
def build_vehicle_master_task(**context) -> dict:
    """원천 4개의 최신 파티션을 읽어 차량 마스터를 조립합니다."""
    params = context.get("params", {})
    event = {"silver_dir": params.get("silver_dir") or DEFAULT_SILVER_DIR}
    collected_date = (params.get("collected_date") or "").strip()
    if collected_date:
        event["collected_date"] = collected_date

    result = lambda_handler_for("vehicle_master_silver", package="sub.aws_lambda.functions")(event=event)
    logger.info("차량 마스터 조립 완료: %s", result)
    return result


@task(
    task_id="validate_silver",
    retries=1,
    retry_delay=timedelta(minutes=10),
    on_failure_callback=slack_failure_callback,
    outlets=[assets.VEHICLE_MASTER_SILVER],
)
def validate_silver_task(result: dict, **context) -> None:
    """도시별 파일이 layout 규칙·스키마와 맞는지, 원천이 낡지 않았는지 봅니다."""
    params = context.get("params", {})
    silver_dir = params.get("silver_dir") or DEFAULT_SILVER_DIR
    layout = importlib.import_module("sub.aws_lambda.common.vehicle_master_layout")
    loader = importlib.import_module("sub.aws_lambda.functions.vehicle_master_silver.loader")

    parsed = parse_handler_result(result)
    collected_date = parse_iso_date(result.get("collected_date"))

    total_rows = 0
    seen_cities: set[str] = set()
    for path in parsed.locations:
        city = layout.city_from_partition(path.parent)
        if city in seen_cities:
            raise ValueError(f"같은 도시가 두 번 적재됐습니다: {city}")
        seen_cities.add(city)

        expected = layout.silver_file(silver_dir, collected_date, city)
        if path.resolve() != expected.resolve():
            raise ValueError(
                f"적재 경로가 layout 규칙과 다릅니다: {path} != {expected}"
            )

        table = read_parquet(path)
        if table.schema != loader.SCHEMA:
            raise ValueError(f"Silver 스키마가 loader.SCHEMA 와 다릅니다: {path}")
        if table.num_rows == 0:
            raise ValueError(f"도시 파일에 행이 없습니다: {city}")
        # 스키마 검사는 이름과 타입만 봅니다. nullable 컬럼이 전 행 NULL 이어도
        # 통과하므로 값이 있어야 할 컬럼은 따로 셉니다 (#567).
        for column in sorted(VEHICLE_MASTER_REQUIRED_NON_NULL):
            nulls = table[column].null_count
            if nulls:
                raise ValueError(
                    f"{city}: {column} 이 {nulls}/{table.num_rows} 행에서 비었습니다"
                )
        total_rows += table.num_rows

    if total_rows != parsed.row_count:
        raise ValueError(
            f"Silver 행 수 합계가 row_count 와 다릅니다: "
            f"{total_rows} != {parsed.row_count}"
        )

    _require_fresh_sources(result.get("source_collected_dates"), collected_date)
    logger.info("Silver 검증 통과: cities=%d rows=%d", len(seen_cities), total_rows)


def _require_fresh_sources(source_collected_dates: object, as_of) -> None:
    """원천이 언제 수집된 것인지 확인합니다.

    Extractor 는 기준일 이하의 최신 파티션을 쓰기 때문에, 상류가 몇 주 멈춰 있어도
    **성공합니다.** 그 상태로 만든 마스터가 Gold 로 흘러가면 지난달 렌트료로 추천이
    나가고, 결과만 봐서는 구분할 수 없습니다. 여기서 끊습니다.
    """
    if not isinstance(source_collected_dates, dict) or not source_collected_dates:
        raise ValueError("source_collected_dates 가 비어 있습니다.")

    missing = set(MAX_SOURCE_AGE_DAYS) - set(source_collected_dates)
    if missing:
        raise ValueError(f"원천 수집일이 빠졌습니다: {sorted(missing)}")

    stale: list[str] = []
    for dataset, max_age_days in MAX_SOURCE_AGE_DAYS.items():
        collected = parse_iso_date(
            source_collected_dates[dataset], field=f"{dataset} 수집일"
        )
        age_days = (as_of - collected).days
        if age_days > max_age_days:
            stale.append(f"{dataset}={age_days}일(한도 {max_age_days}일)")

    if stale:
        raise ValueError("원천 스냅샷이 너무 오래됐습니다: " + ", ".join(stale))
