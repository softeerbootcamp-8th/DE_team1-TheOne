"""리스 업체 보유 차량 대장 Raw -> Bronze -> Silver 파이프라인 DAG.

렌탈 업체 사이트에서 차량 대장(차종/주간 렌트료)을 수집해 Bronze 에 적재하고,
조인 키를 정규화해 Silver 로 변환합니다. 두 단계 모두 lambda/functions 의
핸들러를 그대로 호출합니다.

수집 대상이 12대뿐이고 업체가 카드 이미지를 새로 올릴 때만 값이 바뀌므로
주 1회로 잡았습니다. 실제 변경 빈도가 관측되면 조정하세요.

이미 적재된 Bronze 를 다시 변환하려면 수동 트리거하면서 `collected_date`
파라미터에 대상 수집일(예: "2026-08-09")을 넣으세요.

각 적재 뒤에 검증 태스크가 붙습니다. 핸들러는 쓰기에 성공하기만 하면 되돌아오므로
**적재 결과가 비어 있어도 DAG 는 성공으로 끝납니다.** 특히 Silver 는 업체별로 파일을
여러 개 쓰는데, 그중 하나가 비어도 `row_count` 합계만 맞으면 아무도 모릅니다.
그래서 경로 규칙 · 파일 존재 · 행 수 합계 · 스키마를 여기서 직접 확인합니다.
"""

import importlib
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow.parquet as pq
from airflow.sdk import Param, dag, task

logger = logging.getLogger(__name__)

try:
    from common.slack_failure_callback import slack_failure_callback
except Exception as exc:
    logger.warning("Slack 실패 콜백을 불러오지 못했습니다: %s", exc)

    def slack_failure_callback(context):
        task_instance = context.get("task_instance")
        logger.error(
            "Task 실패: %s",
            task_instance.task_id if task_instance else "unknown",
        )

CURRENT_DIR = Path(__file__).resolve().parent
AIRFLOW_DIR = CURRENT_DIR.parent
CONTAINER_ROOT = Path("/opt/airflow/project-root")
PROJECT_ROOT = CONTAINER_ROOT if CONTAINER_ROOT.exists() else AIRFLOW_DIR.parent

# Airflow 이미지에는 pipeline-core가 설치돼 있지 않아 경로로 참조(이후 변경 필요)
for path in (PROJECT_ROOT, PROJECT_ROOT / "libs" / "pipeline_core"):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

DEFAULT_BRONZE_DIR = os.getenv("BRONZE_DIR", str(PROJECT_ROOT / "data" / "bronze"))
DEFAULT_SILVER_DIR = os.getenv("SILVER_DIR", str(PROJECT_ROOT / "data" / "silver"))


def lambda_handler_for(function_name: str):
    """`lambda`가 파이썬 예약어라 정적 import가 안 돼 동적으로 불러옵니다."""
    module = importlib.import_module(f"lambda.functions.{function_name}.handler")
    return module.lambda_handler


def _unpack(result: dict) -> tuple[int, list[str], str]:
    """핸들러 응답에서 검증에 쓰는 세 값을 꺼내며 모양을 확인합니다."""
    if not isinstance(result, dict):
        raise TypeError(f"핸들러 결과가 dict 가 아닙니다: {type(result).__name__}")

    row_count = result.get("row_count")
    # bool 은 int 의 하위 타입이라 따로 막습니다 (True 가 1 로 통과합니다).
    if isinstance(row_count, bool) or not isinstance(row_count, int):
        raise ValueError(f"row_count 가 정수가 아닙니다: {row_count!r}")
    if row_count <= 0:
        raise ValueError(f"row_count 는 1 이상이어야 합니다: {row_count}")

    locations = result.get("locations")
    if not isinstance(locations, list) or not locations:
        raise ValueError(f"locations 가 비어 있습니다: {locations!r}")
    if not all(isinstance(p, str) and p for p in locations):
        raise ValueError(f"locations 에 빈 경로가 있습니다: {locations!r}")

    collected_date = result.get("collected_date")
    if not isinstance(collected_date, str):
        raise ValueError(f"collected_date 가 문자열이 아닙니다: {collected_date!r}")
    try:
        parsed = datetime.strptime(collected_date, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(
            f"collected_date 가 YYYY-MM-DD 가 아닙니다: {collected_date}"
        ) from exc
    if parsed.isoformat() != collected_date:
        raise ValueError(f"collected_date 가 YYYY-MM-DD 가 아닙니다: {collected_date}")

    return row_count, locations, collected_date


def _require_nonempty_file(path: Path) -> None:
    """파일이 있고 0바이트가 아닌지 봅니다. 업체 하나가 통째로 빠지는 걸 여기서 잡습니다."""
    if not path.is_file():
        raise FileNotFoundError(f"적재 파일이 없습니다: {path}")
    if path.stat().st_size == 0:
        raise ValueError(f"적재 파일이 0바이트입니다: {path}")


def _read_parquet(path: Path):
    if not path.name.endswith(".parquet"):
        raise ValueError(f"Parquet 파일이 아닙니다: {path}")
    try:
        return pq.ParquetFile(path).read()
    except Exception as exc:
        raise RuntimeError(f"Parquet 을 읽지 못했습니다: {path}") from exc


default_args = {
    "owner": "DE_team1",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=30),
    "on_failure_callback": slack_failure_callback,
}


@dag(
    dag_id="vehicle_catalog_raw_to_silver_pipeline",
    default_args=default_args,
    description="리스 업체 보유 차량 대장 Raw -> Bronze -> Silver 수집 및 정제 파이프라인",
    schedule="0 3 * * 1",  # 매주 월요일 03:00 UTC
    start_date=datetime(2026, 8, 1),
    catchup=False,
    max_active_runs=1,
    tags=["vehicle_catalog", "raw", "bronze", "silver", "lambda"],
    params={
        "collected_date": Param(
            None,
            type=["null", "string"],
            pattern=r"^\d{4}-\d{2}-\d{2}$",
            description=(
                "이미 적재된 Bronze 를 다시 변환할 때만 지정 (예: '2026-08-09'). "
                "비워두면 이번 실행이 적재한 수집일을 그대로 씁니다."
            ),
        ),
        "bronze_dir": Param(
            DEFAULT_BRONZE_DIR,
            type="string",
            description="Bronze 데이터 저장 기본 경로",
        ),
        "silver_dir": Param(
            DEFAULT_SILVER_DIR,
            type="string",
            description="Silver 데이터 저장 기본 경로",
        ),
    },
)
def vehicle_catalog_raw_to_silver_pipeline():
    @task(task_id="raw_to_bronze")
    def raw_to_bronze_task(**context) -> dict:
        """렌탈 업체 사이트를 수집해 Bronze 에 적재합니다."""
        params = context.get("params", {})
        result = lambda_handler_for("vehicle_catalog_raw_to_bronze")(
            event={"base_dir": params.get("bronze_dir") or DEFAULT_BRONZE_DIR}
        )
        logger.info("Raw -> Bronze 완료: %s", result)
        return result

    @task(task_id="bronze_to_silver")
    def bronze_to_silver_task(raw_result: dict, **context) -> dict:
        """Bronze 차량 대장의 조인 키를 정규화해 Silver 로 적재합니다."""
        params = context.get("params", {})
        # Bronze 핸들러는 실행 시각으로 파티션을 정하므로 DAG 가 수집일을 따로
        # 계산하면 자정 근처에서 어긋납니다. Bronze 가 알려준 값을 그대로 씁니다.
        collected_date = (params.get("collected_date") or "").strip() or raw_result[
            "collected_date"
        ]

        result = lambda_handler_for("vehicle_catalog_bronze_to_silver")(
            event={
                "collected_date": collected_date,
                "bronze_dir": params.get("bronze_dir") or DEFAULT_BRONZE_DIR,
                "silver_dir": params.get("silver_dir") or DEFAULT_SILVER_DIR,
            }
        )
        logger.info("Bronze -> Silver 완료: %s", result)
        return result

    # 검증 코드는 이 파일 안에 둡니다. 공통 모듈로 빼는 건 다른 DAG 들과 함께
    # 정리할 별도 작업입니다 — 지금 추상화하면 데이터셋마다 다른 규칙이 섞입니다.
    @task(
        task_id="validate_bronze",
        retries=1,
        retry_delay=timedelta(minutes=10),
        on_failure_callback=slack_failure_callback,
    )
    def validate_bronze_task(result: dict, **context) -> None:
        """Bronze 적재 결과가 layout 규칙과 맞는지 확인합니다."""
        params = context.get("params", {})
        bronze_dir = params.get("bronze_dir") or DEFAULT_BRONZE_DIR
        layout = importlib.import_module(
            "lambda.functions.common.vehicle_catalog_layout"
        )

        row_count, locations, collected_date = _unpack(result)
        if len(locations) != 1:
            raise ValueError(f"Bronze locations 는 하나여야 합니다: {locations}")

        path = Path(locations[0])
        _require_nonempty_file(path)

        # 파일명이 수집 시각입니다. 여기서 되짚어 layout 이 정한 자리와 맞춰봅니다.
        try:
            collected_at = datetime.strptime(path.stem, "%Y%m%dT%H%M%SZ").replace(
                tzinfo=timezone.utc
            )
        except ValueError as exc:
            raise ValueError(
                f"Bronze 파일명이 수집시각 형식이 아닙니다: {path.name}"
            ) from exc

        vendor = layout.vendor_from_partition(path.parent)
        expected = layout.bronze_file(bronze_dir, vendor, collected_at)
        if path.resolve() != expected.resolve():
            raise ValueError(f"적재 경로가 layout 규칙과 다릅니다: {path} != {expected}")
        if f"{collected_at:%Y-%m-%d}" != collected_date:
            raise ValueError(
                f"파일명의 수집일과 collected_date 가 다릅니다: "
                f"{path.name} != {collected_date}"
            )

        table = _read_parquet(path)
        if table.num_rows != row_count:
            raise ValueError(
                f"Bronze 행 수가 row_count 와 다릅니다: {table.num_rows} != {row_count}"
            )
        logger.info("Bronze 검증 통과: vendor=%s rows=%d", vendor, row_count)

    @task(
        task_id="validate_silver",
        retries=1,
        retry_delay=timedelta(minutes=10),
        on_failure_callback=slack_failure_callback,
    )
    def validate_silver_task(result: dict, **context) -> None:
        """Silver 는 업체별로 파일을 씁니다. 그중 하나가 비어도 잡아냅니다."""
        params = context.get("params", {})
        silver_dir = params.get("silver_dir") or DEFAULT_SILVER_DIR
        layout = importlib.import_module(
            "lambda.functions.common.vehicle_catalog_layout"
        )
        loader = importlib.import_module(
            "lambda.functions.vehicle_catalog_bronze_to_silver.loader"
        )

        row_count, locations, collected_date = _unpack(result)
        target_date = datetime.strptime(collected_date, "%Y-%m-%d").date()

        total_rows = 0
        seen_vendors: set[str] = set()
        for location in locations:
            path = Path(location)
            _require_nonempty_file(path)

            vendor = layout.vendor_from_partition(path.parent)
            if vendor in seen_vendors:
                raise ValueError(f"같은 업체가 두 번 적재됐습니다: {vendor}")
            seen_vendors.add(vendor)

            expected = layout.silver_file(silver_dir, target_date, vendor)
            if path.resolve() != expected.resolve():
                raise ValueError(
                    f"적재 경로가 layout 규칙과 다릅니다: {path} != {expected}"
                )

            table = _read_parquet(path)
            if table.schema != loader.SCHEMA:
                raise ValueError(f"Silver 스키마가 loader.SCHEMA 와 다릅니다: {path}")
            # 업체 하나가 통째로 비면 여기서 걸립니다. 합계만 보면 못 잡습니다.
            if table.num_rows == 0:
                raise ValueError(f"업체 파일에 행이 없습니다: {vendor}")
            total_rows += table.num_rows

        if total_rows != row_count:
            raise ValueError(
                f"Silver 행 수 합계가 row_count 와 다릅니다: {total_rows} != {row_count}"
            )
        logger.info(
            "Silver 검증 통과: vendors=%d rows=%d", len(seen_vendors), total_rows
        )

    raw_result = raw_to_bronze_task()
    bronze_checked = validate_bronze_task(raw_result)
    silver_result = bronze_to_silver_task(raw_result)
    # Bronze 가 검증을 통과한 뒤에 변환합니다. 안 걸어두면 깨진 Bronze 를 그대로 읽습니다.
    bronze_checked >> silver_result
    validate_silver_task(silver_result)


vehicle_catalog_dag = vehicle_catalog_raw_to_silver_pipeline()
