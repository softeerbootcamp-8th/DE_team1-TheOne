"""차량 대장 · 제원 · Uber/Lyft 자격 4개 Silver 를 합쳐 `vehicle_master` Silver 를 만듭니다.

다른 DAG 와 달리 **Bronze 를 읽지 않습니다.** 입력도 출력도 Silver 인 파생 Silver 라
수집 태스크가 없고, `lambda/functions/vehicle_master_silver` 핸들러 하나를 부릅니다.

스케줄이 시간이 아니라 Asset 인 이유
------------------------------------
상류가 갱신되지 않았는데 시간만 보고 돌면 **조용히 틀립니다.** 이 파이프라인의
Extractor 는 기준일 이하의 최신 파티션을 쓰므로, 상류 크롤링이 실패해도 지난주
데이터로 멀쩡히 성공합니다. 아무도 모릅니다. Asset 스케줄은 "갱신됐을 때만" 돌아서
그 경우를 원천적으로 없앱니다.

제원을 AND 에 넣지 않는 이유
----------------------------
"4개가 다 모이면" 을 그대로 쓰면 안 됩니다. 상류 주기가 다릅니다.

    대장    매주 월 03:00      Uber   매주 월 05:00
    Lyft    매주 월 04:00      제원   매월 1일 04:00   <- 여기

Asset 스케줄은 "마지막 실행 이후 **모든** Asset 이 갱신되면" 실행입니다. 제원을 AND
에 넣으면 한 달에 한 번만 돌고, 그동안 주간 갱신된 렌트료가 반영되지 않습니다.
렌트료는 Gold 의 객단가 상승액을 직접 만드는 값이라 3주 묵으면 없는 가격을
제안하게 됩니다.

그래서 **주간 3종은 AND, 제원은 OR** 입니다. 제원이 갱신되면 즉시 한 번 더 조립하되,
평소에는 주간 3종만으로 돕니다. 제원은 Extractor 가 알아서 최신 파티션을 씁니다.

OR 형태는 상류가 깨졌을 때도 버팁니다 — 제원 크롤링이 한 번 실패해도 대장·자격이
멀쩡하면 조립은 계속됩니다.

첫 실행
-------
Asset 스케줄 DAG 는 모든 Asset 이 최소 한 번 발행돼야 처음 돕니다. 배포 직후에는
상류 3개를 한 번씩 돌리거나 이 DAG 를 수동 트리거하세요.
"""

import importlib
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.sdk import Param, dag, task

from common import assets
from common.validation import parse_handler_result, parse_iso_date, read_parquet

logger = logging.getLogger(__name__)

try:
    from common.slack_failure_callback import (
        slack_failure_callback,
        slack_retry_alert_callback,
    )
except Exception as exc:  # pragma: no cover - 콜백이 없어도 DAG 는 떠야 합니다
    logger.warning("Slack 실패 콜백을 불러오지 못했습니다: %s", exc)

    def slack_retry_alert_callback(context):
        task_instance = context.get("task_instance")
        logger.warning(
            "Task 재시도 예정: %s",
            task_instance.task_id if task_instance else "unknown",
        )

    def slack_failure_callback(context):
        task_instance = context.get("task_instance")
        logger.error(
            "Task 실패: %s", task_instance.task_id if task_instance else "unknown"
        )

CURRENT_DIR = Path(__file__).resolve().parent
AIRFLOW_DIR = CURRENT_DIR.parent
CONTAINER_ROOT = Path("/opt/airflow/project-root")
PROJECT_ROOT = CONTAINER_ROOT if CONTAINER_ROOT.exists() else AIRFLOW_DIR.parent

# Airflow 이미지에는 pipeline-core 가 설치돼 있지 않아 경로로 참조(이후 변경 필요)
for path in (PROJECT_ROOT, PROJECT_ROOT / "libs" / "pipeline_core"):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

DEFAULT_SILVER_DIR = os.getenv("SILVER_DIR", str(PROJECT_ROOT / "data" / "silver"))

# 원천이 얼마나 낡았으면 실패시킬지. Asset 스케줄이라 평소에는 갓 갱신된 것만
# 들어오지만, 수동 트리거·백필에서는 오래된 스냅샷으로 돌 수 있습니다.
# 주간 3종은 주 1회라 2주, 제원은 월 1회라 45일을 넘으면 수집이 멈춘 것으로 봅니다.
MAX_SOURCE_AGE_DAYS = {
    "vehicle_catalog": 14,
    "uber_eligible_vehicles": 14,
    "lyft_eligible_vehicles": 14,
    "fueleconomy_vehicle_specs": 45,
}


def lambda_handler_for(function_name: str):
    """`lambda`가 파이썬 예약어라 정적 import 가 안 돼 동적으로 불러옵니다."""
    module = importlib.import_module(f"lambda.functions.{function_name}.handler")
    return module.lambda_handler


default_args = {
    "owner": "DE_team1",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=15),
    "execution_timeout": timedelta(minutes=15),
    "on_retry_callback": slack_retry_alert_callback,
    "on_failure_callback": slack_failure_callback,
}


@dag(
    dag_id="vehicle_master_silver_pipeline",
    default_args=default_args,
    description="차량 대장·제원·배차 자격 Silver 를 합쳐 차량 마스터 Silver 생성",
    # 주간 3종이 다 갱신되면 실행. 연·월 단위인 제원이 갱신돼도 즉시 재조립.
    schedule=(
        assets.VEHICLE_CATALOG_SILVER
        & assets.UBER_ELIGIBLE_VEHICLES_SILVER
        & assets.LYFT_ELIGIBLE_VEHICLES_SILVER
    )
    | assets.FUELECONOMY_VEHICLE_SPECS_SILVER,
    start_date=datetime(2026, 8, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["vehicle_master", "silver", "lambda"],
    params={
        "collected_date": Param(
            None,
            type=["null", "string"],
            pattern=r"^\d{4}-\d{2}-\d{2}$",
            description=(
                "이 테이블을 만드는 날짜 (예: '2026-08-13'). 비우면 실행 시각의 "
                "UTC 날짜. 원천은 이 날짜 이하의 최신 파티션에서 각각 읽습니다."
            ),
        ),
        "silver_dir": Param(
            DEFAULT_SILVER_DIR,
            type="string",
            description="Silver 데이터 기본 경로 (읽기·쓰기 모두 여기)",
        ),
    },
)
def vehicle_master_silver_pipeline():
    @task(task_id="build_vehicle_master")
    def build_vehicle_master_task(**context) -> dict:
        """원천 4개의 최신 파티션을 읽어 차량 마스터를 조립합니다."""
        params = context.get("params", {})
        event = {"silver_dir": params.get("silver_dir") or DEFAULT_SILVER_DIR}
        collected_date = (params.get("collected_date") or "").strip()
        if collected_date:
            event["collected_date"] = collected_date

        result = lambda_handler_for("vehicle_master_silver")(event=event)
        logger.info("차량 마스터 조립 완료: %s", result)
        return result

    @task(
        task_id="validate_silver",
        retries=1,
        retry_delay=timedelta(minutes=10),
        on_failure_callback=slack_failure_callback,
        # 검증을 통과했을 때만 Asset 이벤트를 냅니다. Gold 가 이걸 구독합니다.
        outlets=[assets.VEHICLE_MASTER_SILVER],
    )
    def validate_silver_task(result: dict, **context) -> None:
        """도시별 파일이 layout 규칙·스키마와 맞는지, 원천이 낡지 않았는지 봅니다."""
        params = context.get("params", {})
        silver_dir = params.get("silver_dir") or DEFAULT_SILVER_DIR
        layout = importlib.import_module("lambda.functions.common.vehicle_master_layout")
        loader = importlib.import_module(
            "lambda.functions.vehicle_master_silver.loader"
        )

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
            # 도시 하나가 통째로 비면 합계만 봐서는 못 잡습니다.
            if table.num_rows == 0:
                raise ValueError(f"도시 파일에 행이 없습니다: {city}")
            total_rows += table.num_rows

        if total_rows != parsed.row_count:
            raise ValueError(
                f"Silver 행 수 합계가 row_count 와 다릅니다: "
                f"{total_rows} != {parsed.row_count}"
            )

        _require_fresh_sources(result.get("source_collected_dates"), collected_date)
        logger.info(
            "Silver 검증 통과: cities=%d rows=%d", len(seen_cities), total_rows
        )

    silver_result = build_vehicle_master_task()
    validate_silver_task(silver_result)


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


vehicle_master_dag = vehicle_master_silver_pipeline()
