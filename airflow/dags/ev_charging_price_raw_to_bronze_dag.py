"""뉴욕주 전기차 충전소 원문을 매일 Bronze JSON으로 적재합니다.

NLR API 키 설정:
1. https://developer.nlr.gov/signup/ 에서 API 키를 발급받습니다.
2. Airflow UI의 Admin > Variables에서 다음 Variable을 등록합니다.
   - Key: NLR_API_KEY
   - Value: 발급받은 API 키
"""

import importlib
import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.sdk import Variable, dag, task

logger = logging.getLogger(__name__)

try:
    from common.slack_failure_callback import slack_failure_callback
except Exception as exc:
    logger.warning("Slack 실패 콜백을 불러오지 못했습니다: %s", exc)

    def slack_failure_callback(context):
        task = context.get("task_instance")
        logger.error("Task 실패: %s", task.task_id if task else "unknown")


CURRENT_DIR = Path(__file__).resolve().parent
AIRFLOW_DIR = CURRENT_DIR.parent
CONTAINER_ROOT = Path("/opt/airflow/project-root")
PROJECT_ROOT = CONTAINER_ROOT if CONTAINER_ROOT.exists() else AIRFLOW_DIR.parent

for path in (PROJECT_ROOT, PROJECT_ROOT / "libs" / "pipeline_core"):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

BRONZE_DIR = str(PROJECT_ROOT / "data" / "bronze")


def lambda_handler_for(function_name: str):
    module = importlib.import_module(f"lambda.functions.{function_name}.handler")
    return module.lambda_handler


def run_gx_bronze_validation(stations: list[dict], total_results: int) -> None:
    """EV 충전소 원문의 데이터 품질 규칙을 GX로 검증합니다."""
    logging.getLogger("great_expectations").setLevel(logging.WARNING)

    # DAG 파싱과 실제 검증 실행을 분리하기 위해 Task 실행 시점에 import합니다.
    import great_expectations as gx
    import pandas as pd

    dataframe = pd.DataFrame(stations)

    # Suite를 코드로 관리하므로 디스크에 설정을 남기지 않는 Context를 사용합니다.
    context = gx.get_context(mode="ephemeral")
    context.variables.progress_bars = {"globally": False}

    # fuel_stations 전체를 이번 실행의 단일 Batch로 등록합니다.
    batch = (
        context.data_sources.add_pandas(name="ev_charging_bronze_source")
        .add_dataframe_asset(name="ev_charging_bronze_asset")
        .add_batch_definition_whole_dataframe("ev_charging_bronze_batch")
        .get_batch(batch_parameters={"dataframe": dataframe})
    )

    # 경로·파일 검사는 DAG 경계에서 하고, Suite는 JSON 내용만 검증합니다.
    suite = gx.ExpectationSuite(
        name="ev_charging_bronze_suite",
        expectations=[
            gx.expectations.ExpectTableRowCountToBeBetween(min_value=1),
            gx.expectations.ExpectTableRowCountToEqual(value=total_results),
            *(
                gx.expectations.ExpectColumnToExist(column=column)
                for column in ("state", "fuel_type_code", "ev_pricing")
            ),
            *(
                gx.expectations.ExpectColumnValuesToNotBeNull(column=column)
                for column in ("state", "fuel_type_code")
            ),
            gx.expectations.ExpectColumnValuesToBeInSet(
                column="state", value_set=["NY"]
            ),
            gx.expectations.ExpectColumnValuesToBeInSet(
                column="fuel_type_code", value_set=["ELEC"]
            ),
            # NULL과 "Free"는 Silver 변환 단계에서 별도로 분류하므로 Bronze에서 허용합니다.
            gx.expectations.ExpectColumnValuesToBeOfType(
                column="ev_pricing", type_="str"
            ),
        ],
    )

    # SUMMARY로 원문 전체 대신 실패 건수와 일부 예시만 받습니다.
    validation = batch.validate(suite, result_format="SUMMARY")

    failures = [result for result in validation.results if not result.success]
    for failure in failures:
        result = dict(failure.result)
        kwargs = failure.expectation_config.kwargs
        column = kwargs.get("column") or "/".join(
            filter(None, (kwargs.get("column_A"), kwargs.get("column_B")))
        )
        logger.error(
            "gx_validation failed layer=bronze expectation=%s column=%s "
            "unexpected_count=%s observed_value=%s",
            failure.expectation_config.type,
            column or "table",
            result.get("unexpected_count"),
            result.get("observed_value"),
        )

    # 실패를 예외로 전파해야 Airflow 재시도와 Slack 콜백이 동작합니다.
    if failures:
        rules = ", ".join(
            f"{failure.expectation_config.type}"
            f"[{failure.expectation_config.kwargs.get('column') or 'table'}]"
            for failure in failures
        )
        raise ValueError(f"EV Charging Bronze GX 검증 실패: {rules}")

    logger.info(
        "gx_validation passed layer=bronze expectations=%s",
        validation.statistics["evaluated_expectations"],
    )


default_args = {
    "owner": "DE_team1",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=15),
    "on_failure_callback": slack_failure_callback,
}


@dag(
    dag_id="ev_charging_price_raw_to_bronze_pipeline",
    default_args=default_args,
    description="뉴욕주 전기차 충전소 일별 Raw -> Bronze 파이프라인",
    schedule="0 9 * * *",
    start_date=datetime(2026, 8, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["ev_charging", "raw", "bronze", "lambda"],
)
def ev_charging_price_raw_to_bronze_pipeline():
    @task(task_id="raw_to_bronze")
    def raw_to_bronze_task() -> dict:
        api_key = os.getenv("NLR_API_KEY") or Variable.get(
            "NLR_API_KEY", default=None
        )
        if not api_key:
            raise ValueError("Airflow Variable 또는 환경변수 NLR_API_KEY가 필요합니다.")
        os.environ["NLR_API_KEY"] = api_key

        result = lambda_handler_for("ev_charging_stations_raw_to_bronze")(
            event={"base_dir": BRONZE_DIR}
        )
        logger.info("Raw -> Bronze 완료: %s", result)
        return result

    @task(
        task_id="validate_bronze",
        retries=1,
        retry_delay=timedelta(minutes=10),
        on_failure_callback=slack_failure_callback,
    )
    def validate_bronze_task(result: dict) -> None:
        if not isinstance(result, dict):
            raise TypeError("Handler 결과가 dict가 아닙니다.")

        row_count = result.get("row_count")
        locations = result.get("locations")
        collected_date = result.get("collected_date")
        if isinstance(row_count, bool) or row_count != 1:
            raise ValueError("Bronze row_count는 1이어야 합니다.")
        if (
            not isinstance(locations, list)
            or len(locations) != 1
            or not isinstance(locations[0], str)
            or not locations[0]
        ):
            raise ValueError("locations에는 파일 경로가 하나 있어야 합니다.")
        if not isinstance(collected_date, str):
            raise ValueError("collected_date가 문자열이 아닙니다.")
        try:
            target_date = datetime.strptime(collected_date, "%Y-%m-%d").date()
        except ValueError as exc:
            raise ValueError("collected_date는 YYYY-MM-DD 형식이어야 합니다.") from exc
        if target_date.isoformat() != collected_date:
            raise ValueError("collected_date는 YYYY-MM-DD 형식이어야 합니다.")
        if result.get("state") != "NY" or result.get("fuel_type_code") != "ELEC":
            raise ValueError("Handler의 state 또는 fuel_type_code가 올바르지 않습니다.")

        path = Path(locations[0])
        if not path.is_file():
            raise FileNotFoundError(f"적재 파일이 없습니다: {path}")
        try:
            collected_at = datetime.strptime(path.stem, "%Y%m%dT%H%M%SZ").replace(
                tzinfo=timezone.utc
            )
        except ValueError as exc:
            raise ValueError("Bronze 파일명의 수집시각 형식이 올바르지 않습니다.") from exc

        layout = importlib.import_module(
            "lambda.functions.common.ev_charging_layout"
        )
        expected = layout.bronze_file(BRONZE_DIR, collected_at)
        if path.resolve() != expected.resolve():
            raise ValueError(f"적재 경로가 예상과 다릅니다: {path}")
        if collected_at.date() != target_date:
            raise ValueError("Bronze 파일명과 collected_date가 다릅니다.")

        try:
            payload = json.loads(path.read_bytes())
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError(f"Bronze JSON을 읽지 못했습니다: {path}") from exc
        if not isinstance(payload, dict):
            raise ValueError("Bronze JSON이 객체 형식이 아닙니다.")

        stations = payload.get("fuel_stations")
        total_results = payload.get("total_results")
        if not isinstance(stations, list):
            raise ValueError("Bronze fuel_stations가 목록 형식이 아닙니다.")
        if isinstance(total_results, bool) or not isinstance(total_results, int):
            raise ValueError("Bronze total_results가 정수가 아닙니다.")
        if any(not isinstance(station, dict) for station in stations):
            raise ValueError("Bronze 충전소 데이터가 객체 형식이 아닙니다.")

        # 위에서는 Handler 응답·경로·파일 형식을 확인했고,
        # 여기서는 파일 안의 충전소 데이터 품질 규칙을 GX로 검증합니다.
        run_gx_bronze_validation(stations, total_results)

    validate_bronze_task(raw_to_bronze_task())


ev_charging_price_raw_to_bronze_dag = ev_charging_price_raw_to_bronze_pipeline()
