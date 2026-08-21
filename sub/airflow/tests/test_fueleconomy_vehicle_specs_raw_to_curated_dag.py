"""차종별 제원 DAG 의 월 1회 스케줄 계약과 event 계약을 고정합니다.

이 DAG 는 **매월 1일에만** 돕니다. 그래서 다른 DAG 보다 테스트가 더 필요합니다 —
스케줄이나 event 조립이 리팩터로 깨져도 다음 실행까지 한 달이라, 그때까지 아무도
모릅니다. 매일 도는 DAG 라면 다음 날 아침에 드러날 사고가 여기서는 한 달을 갑니다.

스케줄 세 값이 한 묶음입니다.

    schedule="0 4 1 * *"   매월 1일 04:00 UTC
    catchup=False          배포 시점 이전 달을 소급 실행하지 않음
    max_active_runs=1      수동 트리거가 겹쳐 같은 파티션을 동시에 쓰는 것을 막음

event 쪽은 다른 DAG 와 같은 규칙입니다. `collected_date` 는 Raw 가 돌려준 값을
그대로 씁니다 — DAG 가 날짜를 따로 계산하면 자정 근처에서 어긋나 없는 파티션을
읽습니다.

핸들러는 부르지 않습니다. `lambda_handler_for` 를 가짜로 바꿔 event 만 받아 적습니다.
네트워크(20MB CSV 내려받기)도 파일 쓰기도 없습니다.
"""

import pytest

from dags import fueleconomy_vehicle_specs_raw_to_curated_dag as dag_module
from sub.airflow.scripts.fueleconomy_vehicle_specs_raw_to_curated import tasks as task_module

DAG = dag_module.fueleconomy_vehicle_specs_dag
DAG_ID = "fueleconomy_vehicle_specs_raw_to_curated_pipeline"
RAW_FUNCTION = "fueleconomy_vehicle_specs_source_to_raw"
CURATED_FUNCTION = "fueleconomy_vehicle_specs_raw_to_curated"

# Raw 가 돌려줬다고 가정할 수집일. PARAM_DATE 와 반드시 달라야
# "어느 쪽 값이 갔는지" 를 구분할 수 있습니다.
RAW_DATE = "2026-01-01"
PARAM_DATE = "2025-01-01"
RAW_RESULT = {"collected_date": RAW_DATE}


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

    검증이 변환 앞에 있어야 깨진 Raw 를 읽지 않습니다.
    """
    assert DAG.dag_id == DAG_ID
    assert set(DAG.task_ids) == {
        "source_to_raw",
        "validate_raw",
        "raw_to_curated",
        "validate_curated",
    }
    assert DAG.get_task("source_to_raw").upstream_task_ids == set()
    assert DAG.get_task("validate_raw").upstream_task_ids == {"source_to_raw"}
    # Raw 결과(collected_date)가 필요해서 source_to_raw 에도 붙어 있습니다.
    assert DAG.get_task("raw_to_curated").upstream_task_ids == {
        "source_to_raw",
        "validate_raw",
    }
    assert DAG.get_task("validate_curated").upstream_task_ids == {"raw_to_curated"}


def test_월_1회_스케줄_계약을_지킨다():
    """[필수] 깨져도 다음 실행이 한 달 뒤라 운영 중에는 드러나지 않습니다.

    셋을 한 테스트에 묶은 이유: 따로 보면 각각 그럴듯해 보이지만, 셋이 함께여야
    "매월 1일에 한 번만, 소급 없이" 라는 의도가 됩니다.
    """
    assert DAG.schedule == "0 4 1 * *"  # 매월 1일 04:00 UTC
    assert DAG.catchup is False  # 배포 시점 이전 달을 소급 실행하지 않음
    assert DAG.max_active_runs == 1  # 수동 트리거가 겹쳐 같은 파티션을 동시에 쓰는 것 방지


def test_Bronze_event_는_base_dir_와_collected_date_를_넘긴다(events):
    """핸들러가 받는 인자명이 `base_dir` 입니다 — `bronze_dir` 로 보내면 기본 경로에 씁니다."""
    call_task("source_to_raw", params={"raw_dir": "/tmp/raw"})

    assert only_event(events, RAW_FUNCTION) == {
        "base_dir": "/tmp/raw",
        "collected_date": None,
    }


def test_Silver_event_는_세_키를_넘긴다(events):
    """키가 빠지면 핸들러가 환경변수 기본값으로 조용히 넘어갑니다."""
    call_task(
        "raw_to_curated",
        raw_result=RAW_RESULT,
        params={"raw_dir": "/tmp/b", "curated_dir": "/tmp/s"},
    )

    assert only_event(events, CURATED_FUNCTION) == {
        "collected_date": RAW_DATE,
        "raw_dir": "/tmp/b",
        "curated_dir": "/tmp/s",
    }


def test_collected_date_가_없으면_Bronze_가_알려준_값을_쓴다(events):
    """[필수] 자정 근처 어긋남 방지 — DAG 가 날짜를 따로 계산하면 안 됩니다."""
    call_task("raw_to_curated", raw_result=RAW_RESULT, params={"collected_date": None})

    assert only_event(events, CURATED_FUNCTION)["collected_date"] == RAW_DATE


def test_collected_date_가_공백_문자열이어도_Bronze_값으로_떨어진다(events):
    """[필수] Airflow UI 에서 비워두면 None 이 아니라 공백이 들어올 수 있습니다."""
    call_task("raw_to_curated", raw_result=RAW_RESULT, params={"collected_date": "   "})

    assert only_event(events, CURATED_FUNCTION)["collected_date"] == RAW_DATE


def test_collected_date_를_주면_그_값이_Silver_로_간다(events):
    """지난달 스냅샷을 다시 변환하는 경로입니다. 월 1회라 이 경로를 쓸 일이 실제로 있습니다."""
    call_task(
        "raw_to_curated", raw_result=RAW_RESULT, params={"collected_date": PARAM_DATE}
    )

    assert only_event(events, CURATED_FUNCTION)["collected_date"] == PARAM_DATE


def test_경로_파라미터가_비면_DAG_기본값을_쓴다(events):
    """params 가 통째로 비어도 None 이 핸들러로 새어 나가면 안 됩니다."""
    call_task("source_to_raw", params={})
    call_task("raw_to_curated", raw_result=RAW_RESULT, params={})

    assert only_event(events, RAW_FUNCTION)["base_dir"] == dag_module.DEFAULT_RAW_DIR

    silver = only_event(events, CURATED_FUNCTION)
    assert silver["raw_dir"] == dag_module.DEFAULT_RAW_DIR
    assert silver["curated_dir"] == dag_module.DEFAULT_CURATED_DIR
