"""EV Charging Raw -> Bronze DAG 경계와 GX Bronze Suite를 확인합니다.

CI 의 `check_dags.py` 는 DAG 가 import 되는지만 봅니다. 여기서는 그 다음,
`validate_bronze` 가 경계 오류와 데이터 품질 오류를 실제로 걸러내는지를 봅니다.
"""

import importlib
import json
from datetime import datetime, timezone

import pytest

from dags import ev_charging_price_raw_to_bronze_dag as dag_module
from scripts.ev_charging_price_raw_to_bronze import tasks as task_module

# `lambda` 는 예약어라 일반 import 문 대신 동적으로 불러옵니다.
layout = importlib.import_module("lambda.functions.common.ev_charging_layout")

DAG = dag_module.ev_charging_price_raw_to_bronze_dag
validate_bronze = DAG.get_task("validate_bronze").python_callable

COLLECTED_AT = datetime(2026, 8, 9, 12, 0, tzinfo=timezone.utc)
COLLECTED_DATE = "2026-08-09"


def station(
    state: object = "NY",
    fuel_type_code: object = "ELEC",
    ev_pricing: object = "$0.30/kWh",
) -> dict:
    return {
        "id": 1,
        "state": state,
        "fuel_type_code": fuel_type_code,
        "zip": "10001",
        "ev_pricing": ev_pricing,
    }


def payload_of(stations: list[dict], total_results: int | None = None) -> dict:
    return {
        "total_results": len(stations) if total_results is None else total_results,
        "fuel_stations": stations,
    }


@pytest.fixture
def bronze_dir(tmp_path, monkeypatch):
    """검증 태스크가 보는 Bronze 루트를 임시 디렉터리로 돌립니다."""
    monkeypatch.setattr(task_module, "BRONZE_DIR", str(tmp_path))
    return str(tmp_path)


def write_bronze(bronze_dir: str, body, collected_at: datetime = COLLECTED_AT):
    """레이아웃 규칙에 맞는 자리에 Bronze 파일을 씁니다. body 가 str 이면 그대로 씁니다."""
    path = layout.bronze_file(bronze_dir, collected_at)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body if isinstance(body, str) else json.dumps(body))
    return path


def result_of(path, **overrides) -> dict:
    result = {
        "row_count": 1,
        "locations": [str(path)],
        "collected_date": COLLECTED_DATE,
        "state": "NY",
        "fuel_type_code": "ELEC",
    }
    result.update(overrides)
    return result


# --- DAG 구조 -----------------------------------------------------------------


def test_dag_id_and_tasks():
    assert DAG.dag_id == "ev_charging_price_raw_to_bronze_pipeline"
    assert {task.task_id for task in DAG.tasks} == {"raw_to_bronze", "validate_bronze"}


def test_validate_runs_after_load():
    assert DAG.get_task("raw_to_bronze").downstream_task_ids == {"validate_bronze"}


# --- 정상 케이스 ---------------------------------------------------------------


def test_valid_bronze_passes(bronze_dir):
    path = write_bronze(
        bronze_dir,
        payload_of([station(), station(ev_pricing="Free"), station(ev_pricing=None)]),
    )

    validate_bronze(result_of(path))


# --- Handler 응답이 잘못된 경우 -------------------------------------------------


def test_result_must_be_dict():
    with pytest.raises(TypeError):
        validate_bronze(["not", "a", "dict"])


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param({"row_count": 2}, id="row_count가 1이 아님"),
        pytest.param({"row_count": True}, id="row_count가 bool"),
        pytest.param({"locations": []}, id="locations가 비어 있음"),
        pytest.param({"locations": ["a", "b"]}, id="locations가 2개"),
        pytest.param({"locations": [""]}, id="locations가 빈 문자열"),
        pytest.param({"collected_date": 20260809}, id="collected_date가 문자열이 아님"),
        pytest.param({"collected_date": "2026/08/09"}, id="collected_date 구분자 오류"),
        pytest.param({"collected_date": "2026-8-9"}, id="collected_date 0패딩 누락"),
        pytest.param({"state": "CA"}, id="state가 NY가 아님"),
        pytest.param({"fuel_type_code": "LPG"}, id="fuel_type_code가 ELEC이 아님"),
    ],
)
def test_invalid_result_is_rejected(bronze_dir, overrides):
    path = write_bronze(bronze_dir, payload_of([station()]))

    with pytest.raises(ValueError):
        validate_bronze(result_of(path, **overrides))


# --- 적재된 파일이 잘못된 경우 --------------------------------------------------


def test_missing_file_is_rejected(bronze_dir):
    path = layout.bronze_file(bronze_dir, COLLECTED_AT)

    with pytest.raises(FileNotFoundError):
        validate_bronze(result_of(path))


def test_file_name_must_be_collection_time(bronze_dir):
    path = layout.bronze_file(bronze_dir, COLLECTED_AT).with_name("bronze.json")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload_of([station()])))

    with pytest.raises(ValueError, match="수집시각"):
        validate_bronze(result_of(path))


def test_file_outside_layout_is_rejected(bronze_dir, tmp_path):
    # 파일명 규칙은 맞지만 레이아웃이 정한 파티션 밖에 있는 경우
    path = write_bronze(str(tmp_path / "elsewhere"), payload_of([station()]))

    with pytest.raises(ValueError, match="적재 경로"):
        validate_bronze(result_of(path))


def test_file_date_must_match_collected_date(bronze_dir):
    other_day = COLLECTED_AT.replace(day=8)
    path = write_bronze(bronze_dir, payload_of([station()]), collected_at=other_day)

    with pytest.raises(ValueError, match="collected_date"):
        validate_bronze(result_of(path))


@pytest.mark.parametrize(
    "body",
    [
        pytest.param("{not json", id="JSON 파싱 실패"),
        pytest.param([{"fuel_stations": []}], id="최상위가 객체가 아님"),
        pytest.param({"total_results": 1}, id="fuel_stations 키 없음"),
        pytest.param(payload_of([station()], total_results=True), id="total_results가 bool"),
        pytest.param({"total_results": 1, "fuel_stations": ["문자열"]}, id="충전소가 객체가 아님"),
    ],
)
def test_invalid_bronze_payload_is_rejected(bronze_dir, body):
    path = write_bronze(bronze_dir, body)

    with pytest.raises(ValueError):
        validate_bronze(result_of(path))


@pytest.mark.parametrize(
    ("body", "failed_rule"),
    [
        pytest.param(
            payload_of([]),
            "expect_table_row_count_to_be_between[table]",
            id="충전소 목록이 비어 있음",
        ),
        pytest.param(
            payload_of([station()], total_results=2),
            "expect_table_row_count_to_equal[table]",
            id="total_results와 실제 건수 불일치",
        ),
        pytest.param(
            payload_of([station(state="CA")]),
            "expect_column_values_to_be_in_set[state]",
            id="NY 이외의 주",
        ),
        pytest.param(
            payload_of([station(fuel_type_code="LPG")]),
            "expect_column_values_to_be_in_set[fuel_type_code]",
            id="ELEC 이외의 연료",
        ),
        pytest.param(
            payload_of([station(state=None)]),
            "expect_column_values_to_not_be_null[state]",
            id="state가 NULL",
        ),
        pytest.param(
            payload_of([station(fuel_type_code=None)]),
            "expect_column_values_to_not_be_null[fuel_type_code]",
            id="fuel_type_code가 NULL",
        ),
        pytest.param(
            payload_of([{key: value for key, value in station().items() if key != "ev_pricing"}]),
            "expect_column_to_exist[ev_pricing]",
            id="ev_pricing 컬럼 누락",
        ),
        pytest.param(
            payload_of([station(ev_pricing=0.3)]),
            "expect_column_values_to_be_of_type[ev_pricing]",
            id="ev_pricing이 문자열이 아님",
        ),
    ],
)
def test_gx_bronze_expectation_failure_is_rejected_and_logged(
    bronze_dir, body, failed_rule, caplog
):
    path = write_bronze(bronze_dir, body)

    with pytest.raises(ValueError, match=failed_rule.replace("[", r"\[")):
        validate_bronze(result_of(path))

    expectation = failed_rule.split("[", 1)[0]
    assert f"expectation={expectation}" in caplog.text
