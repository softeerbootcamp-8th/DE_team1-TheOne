"""Uber 자격 차량 DAG 가 핸들러에 넘기는 event 계약을 고정합니다.

두 태스크는 event 딕셔너리로만 핸들러와 이야기합니다. 키 이름이 어긋나도 핸들러는
`event.get(...) or 기본값` 으로 받으므로 **예외 없이 기본 경로에 씁니다.** DAG 는
초록불로 끝나고 데이터만 엉뚱한 곳에 쌓입니다. 그래서 계약을 여기서 못 박습니다.

Lyft DAG 와 event 계약이 같습니다(#228 에서 `base_dir` 로 통일). 그래도 테스트를
합치지 않습니다 — 두 DAG 는 서로 다른 사이트를 긁고 따로 바뀌므로, 한쪽이 깨졌을 때
어느 쪽인지 이름만 보고 알 수 있어야 합니다.

시나리오:

1. DAG 구조 — dag_id, 태스크 2개, raw_to_bronze -> bronze_to_silver 의존 순서
2. [필수] Bronze event = base_dir + city_slug 두 키
3. [필수] Silver event 에 city_slug 가 없음
4. [필수] collected_date 가 비면 raw_result 값을 씀
5. [필수] collected_date 가 공백 문자열이어도 raw_result 값으로 떨어짐
6. collected_date 를 주면 그 값이 Silver 로 감
7. city_slug 를 주면 그 값이 Bronze event 로 감
8. 파라미터가 비면 DAG 기본값(경로 / 도시)을 씀

핸들러는 부르지 않습니다. `lambda_handler_for` 를 가짜로 바꿔 event 만 받아 적습니다.
네트워크도 파일 쓰기도 없습니다.
"""

import pytest

from dags import uber_eligible_vehicles_raw_to_silver_dag as dag_module
from sub.airflow.scripts.uber_eligible_vehicles_raw_to_silver import tasks as task_module

DAG = dag_module.uber_eligible_vehicles_dag
DAG_ID = "uber_eligible_vehicles_raw_to_silver_pipeline"
BRONZE_FUNCTION = "uber_eligible_vehicles_raw_to_bronze"
SILVER_FUNCTION = "uber_eligible_vehicles_bronze_to_silver"

# Bronze 가 돌려줬다고 가정할 수집일. PARAM_DATE 와 반드시 달라야
# "어느 쪽 값이 갔는지" 를 구분할 수 있습니다.
BRONZE_DATE = "2026-08-11"
PARAM_DATE = "2026-08-01"
RAW_RESULT = {"collected_date": BRONZE_DATE}


@pytest.fixture
def events(monkeypatch):
    """핸들러를 부르지 않고 (함수명, event) 만 받아 적습니다."""
    captured: list[tuple[str, dict]] = []

    def fake_lambda_handler_for(function_name: str, **_kwargs):
        def handler(event: dict) -> dict:
            captured.append((function_name, event))
            return dict(RAW_RESULT)

        return handler

    monkeypatch.setattr(task_module, "lambda_handler_for", fake_lambda_handler_for)
    return captured


def call_task(task_id: str, **kwargs) -> dict:
    """태스크의 실제 함수를 Airflow 없이 직접 부릅니다."""
    return DAG.get_task(task_id).python_callable(**kwargs)


def only_event(events: list[tuple[str, dict]], function_name: str) -> dict:
    """받아 적은 것 중 해당 핸들러로 간 event 하나를 꺼냅니다."""
    matched = [event for name, event in events if name == function_name]
    assert len(matched) == 1, f"{function_name} 호출이 1건이 아닙니다: {events}"
    return matched[0]


def test_적재와_검증이_번갈아_이어진다():
    """raw_to_bronze -> validate_bronze -> bronze_to_silver -> validate_silver.

    검증이 변환 앞에 있어야 깨진 Bronze 를 읽지 않습니다.
    """
    assert DAG.dag_id == DAG_ID
    assert set(DAG.task_ids) == {
        "raw_to_bronze",
        "validate_bronze",
        "bronze_to_silver",
        "validate_silver",
    }
    assert DAG.get_task("raw_to_bronze").upstream_task_ids == set()
    assert DAG.get_task("validate_bronze").upstream_task_ids == {"raw_to_bronze"}
    # Bronze 결과(collected_date)가 필요해서 raw_to_bronze 에도 붙어 있습니다.
    assert DAG.get_task("bronze_to_silver").upstream_task_ids == {
        "raw_to_bronze",
        "validate_bronze",
    }
    assert DAG.get_task("validate_silver").upstream_task_ids == {"bronze_to_silver"}


def test_Bronze_event_는_base_dir_city_slug_collected_date_를_넘긴다(events):
    """[필수] 이 핸들러는 경로를 `base_dir` 로만 받습니다. `bronze_dir` 로 보내면 기본 경로에 씁니다."""
    call_task(
        "raw_to_bronze",
        params={"bronze_dir": "/tmp/bronze", "city_slug": "chicago"},
    )

    assert only_event(events, BRONZE_FUNCTION) == {
        "base_dir": "/tmp/bronze",
        "city_slug": "chicago",
        "collected_date": None,
    }


def test_Silver_event_에는_city_slug_가_들어가지_않는다(events):
    """[필수] Silver 핸들러는 수집일 아래 도시 디렉터리를 전부 훑으므로 도시를 받지 않습니다."""
    call_task(
        "bronze_to_silver",
        raw_result=RAW_RESULT,
        params={"city_slug": "chicago", "bronze_dir": "/tmp/b", "silver_dir": "/tmp/s"},
    )

    event = only_event(events, SILVER_FUNCTION)
    assert "city_slug" not in event
    assert event == {
        "collected_date": BRONZE_DATE,
        "bronze_dir": "/tmp/b",
        "silver_dir": "/tmp/s",
    }


def test_collected_date_가_없으면_Bronze_가_알려준_값을_쓴다(events):
    """[필수] 자정 근처 어긋남 방지 — DAG 가 날짜를 따로 계산하면 안 됩니다."""
    call_task("bronze_to_silver", raw_result=RAW_RESULT, params={"collected_date": None})

    assert only_event(events, SILVER_FUNCTION)["collected_date"] == BRONZE_DATE


def test_collected_date_가_공백_문자열이어도_Bronze_값으로_떨어진다(events):
    """[필수] Airflow UI 에서 비워두면 None 이 아니라 공백이 들어올 수 있습니다."""
    call_task("bronze_to_silver", raw_result=RAW_RESULT, params={"collected_date": "   "})

    assert only_event(events, SILVER_FUNCTION)["collected_date"] == BRONZE_DATE


def test_collected_date_를_주면_그_값이_Silver_로_간다(events):
    """이미 적재된 Bronze 를 다시 변환하는 경로입니다."""
    call_task(
        "bronze_to_silver", raw_result=RAW_RESULT, params={"collected_date": PARAM_DATE}
    )

    assert only_event(events, SILVER_FUNCTION)["collected_date"] == PARAM_DATE


def test_city_slug_를_주면_그_값이_Bronze_로_간다(events):
    """자격 페이지가 도시마다 달라, 무시되면 항상 같은 도시만 긁습니다."""
    call_task("raw_to_bronze", params={"city_slug": "chicago"})

    assert only_event(events, BRONZE_FUNCTION)["city_slug"] == "chicago"


def test_파라미터가_비면_DAG_기본값을_쓴다(events):
    """params 가 통째로 비어도 None 이 핸들러로 새어 나가면 안 됩니다."""
    call_task("raw_to_bronze", params={})
    call_task("bronze_to_silver", raw_result=RAW_RESULT, params={})

    bronze = only_event(events, BRONZE_FUNCTION)
    assert bronze["base_dir"] == dag_module.DEFAULT_BRONZE_DIR
    assert bronze["city_slug"] == dag_module.DEFAULT_CITY_SLUG

    silver = only_event(events, SILVER_FUNCTION)
    assert silver["bronze_dir"] == dag_module.DEFAULT_BRONZE_DIR
    assert silver["silver_dir"] == dag_module.DEFAULT_SILVER_DIR
