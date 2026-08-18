"""차량 대장 DAG 가 핸들러에 넘기는 event 계약을 고정합니다.

이 DAG 는 두 태스크가 event 딕셔너리로만 핸들러와 이야기합니다. 키 이름이나
값을 고르는 규칙이 바뀌어도 **DAG 는 그대로 성공하고** 엉뚱한 파티션을 읽거나
쓰기만 합니다. 그래서 계약을 여기서 못 박습니다.

가장 중요한 건 `collected_date` 를 고르는 규칙입니다. Bronze 핸들러는 자기
실행 시각으로 파티션을 정하는데, Silver 쪽이 날짜를 따로 계산하면 자정 근처
실행에서 둘이 어긋나 **없는 파티션을 읽습니다.** 그래서 Bronze 가 돌려준 값을
그대로 쓰는 게 규약인데, 지금까지 주석에만 있고 깨져도 아무도 몰랐습니다.

핸들러는 부르지 않습니다. `lambda_handler_for` 를 가짜로 바꿔 event 만 받아
적습니다. 네트워크도 파일 쓰기도 없습니다.
"""

import importlib.util
import sys
from pathlib import Path

import pytest

from sub.airflow.scripts.vehicle_catalog_raw_to_silver import tasks as task_module

DAG_FILE = (
    Path(__file__).resolve().parents[1] / "dags" / "vehicle_catalog_raw_to_silver_dag.py"
)
DAG_ID = "vehicle_catalog_raw_to_silver_pipeline"
BRONZE_FUNCTION = "vehicle_catalog_raw_to_bronze"
SILVER_FUNCTION = "vehicle_catalog_bronze_to_silver"

# Bronze 가 돌려줬다고 가정할 수집일. 아래 PARAM_DATE 와 반드시 달라야
# "어느 쪽 값이 갔는지" 를 구분할 수 있습니다.
BRONZE_DATE = "2026-08-09"
PARAM_DATE = "2026-08-01"


@pytest.fixture(scope="module")
def dag_module():
    """DAG 파일을 모듈로 읽어옵니다.

    실제 Airflow 와 같이 dags 폴더를 경로에 넣습니다. 빼면 dags/common 을 못 찾는데도
    DAG 쪽이 예외를 잡고 경고만 내서 조용히 다른 코드 경로를 타게 됩니다.
    """
    dags_dir = str(DAG_FILE.parent)
    if dags_dir not in sys.path:
        sys.path.insert(0, dags_dir)

    spec = importlib.util.spec_from_file_location(DAG_FILE.stem, DAG_FILE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def events(dag_module, monkeypatch):
    """핸들러를 부르지 않고 (함수명, event) 만 받아 적습니다."""
    captured: list[tuple[str, dict]] = []

    def fake_lambda_handler_for(function_name: str, **_kwargs):
        def handler(event: dict) -> dict:
            captured.append((function_name, event))
            # Bronze 태스크의 반환값 모양만 맞춰줍니다. Silver 태스크는 이걸 안 씁니다.
            return {"collected_date": BRONZE_DATE}

        return handler

    monkeypatch.setattr(task_module, "lambda_handler_for", fake_lambda_handler_for)
    return captured


def call_task(dag_module, task_id: str, **kwargs) -> dict:
    """태스크의 실제 함수를 Airflow 없이 직접 부릅니다."""
    return dag_module.vehicle_catalog_dag.get_task(task_id).python_callable(**kwargs)


def silver_event(events: list[tuple[str, dict]]) -> dict:
    """받아 적은 것 중 Silver 핸들러로 간 event 하나를 꺼냅니다."""
    matched = [event for name, event in events if name == SILVER_FUNCTION]
    assert len(matched) == 1, f"Silver 핸들러 호출이 1건이 아닙니다: {events}"
    return matched[0]


def test_적재와_검증이_번갈아_이어진다(dag_module):
    """raw_to_bronze -> validate_bronze -> bronze_to_silver -> validate_silver.

    검증이 변환 앞에 있어야 깨진 Bronze 를 읽지 않습니다. 이 순서가 뒤집히면
    Silver 가 먼저 돌아 검증이 사후 통보가 됩니다.
    """
    dag = dag_module.vehicle_catalog_dag

    assert dag.dag_id == DAG_ID
    assert set(dag.task_ids) == {
        "raw_to_bronze",
        "validate_bronze",
        "bronze_to_silver",
        "validate_silver",
    }
    assert dag.get_task("raw_to_bronze").upstream_task_ids == set()
    assert dag.get_task("validate_bronze").upstream_task_ids == {"raw_to_bronze"}
    # Bronze 결과(collected_date)가 필요해서 raw_to_bronze 에도 붙어 있습니다.
    assert dag.get_task("bronze_to_silver").upstream_task_ids == {
        "raw_to_bronze",
        "validate_bronze",
    }
    assert dag.get_task("validate_silver").upstream_task_ids == {"bronze_to_silver"}


def test_Bronze_event_는_base_dir_하나만_넘긴다(dag_module, events):
    """핸들러가 받는 인자명이 `base_dir` 입니다 — `bronze_dir` 로 바뀌면 조용히 기본 경로에 씁니다."""
    call_task(dag_module, "raw_to_bronze", params={"bronze_dir": "/tmp/bronze"})

    name, event = events[0]
    assert name == BRONZE_FUNCTION
    assert event == {"base_dir": "/tmp/bronze"}


def test_collected_date_가_없으면_Bronze_가_알려준_값을_쓴다(dag_module, events):
    """[필수] 자정 근처 어긋남 방지 — DAG 가 날짜를 따로 계산하면 안 됩니다."""
    call_task(
        dag_module,
        "bronze_to_silver",
        raw_result={"collected_date": BRONZE_DATE},
        params={"collected_date": None},
    )

    assert silver_event(events)["collected_date"] == BRONZE_DATE


def test_collected_date_가_공백_문자열이어도_Bronze_값으로_떨어진다(dag_module, events):
    """[필수] Airflow UI 에서 비워두면 None 이 아니라 공백이 들어올 수 있습니다."""
    call_task(
        dag_module,
        "bronze_to_silver",
        raw_result={"collected_date": BRONZE_DATE},
        params={"collected_date": "   "},
    )

    assert silver_event(events)["collected_date"] == BRONZE_DATE


def test_collected_date_를_주면_그_값이_Silver_로_간다(dag_module, events):
    """이미 적재된 Bronze 를 다시 변환하는 경로입니다."""
    call_task(
        dag_module,
        "bronze_to_silver",
        raw_result={"collected_date": BRONZE_DATE},
        params={"collected_date": PARAM_DATE},
    )

    assert silver_event(events)["collected_date"] == PARAM_DATE


def test_Silver_event_는_세_키를_넘긴다(dag_module, events):
    """키가 빠지면 핸들러가 환경변수 기본값으로 조용히 넘어갑니다."""
    call_task(
        dag_module,
        "bronze_to_silver",
        raw_result={"collected_date": BRONZE_DATE},
        params={
            "collected_date": None,
            "bronze_dir": "/tmp/bronze",
            "silver_dir": "/tmp/silver",
        },
    )

    assert silver_event(events) == {
        "collected_date": BRONZE_DATE,
        "bronze_dir": "/tmp/bronze",
        "silver_dir": "/tmp/silver",
    }


def test_경로_파라미터가_비면_DAG_기본값을_쓴다(dag_module, events):
    """params 가 통째로 비어도 None 이 핸들러로 새어 나가면 안 됩니다."""
    call_task(dag_module, "raw_to_bronze", params={})
    call_task(
        dag_module,
        "bronze_to_silver",
        raw_result={"collected_date": BRONZE_DATE},
        params={},
    )

    assert events[0][1]["base_dir"] == dag_module.DEFAULT_BRONZE_DIR
    silver = silver_event(events)
    assert silver["bronze_dir"] == dag_module.DEFAULT_BRONZE_DIR
    assert silver["silver_dir"] == dag_module.DEFAULT_SILVER_DIR
