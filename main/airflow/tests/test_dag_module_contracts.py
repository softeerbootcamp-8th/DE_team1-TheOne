"""메인 데이터 프로덕트 DAG의 공개 실행 계약."""

import ast
import importlib
from datetime import timedelta
from pathlib import Path

import pytest

from main.airflow.common.assets import MAX_ACTIVE_SERVICE_AREA_RUNS


DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"


DAG_VARIABLES = {
    "driver_vehicle_monthly_snapshot_raw_to_silver_dag": "driver_vehicle_monthly_snapshot_raw_to_silver_dag",
    "eia_electricity_price_raw_to_silver_dag": "eia_electricity_price_raw_to_silver_dag",
    "eia_gas_price_raw_to_silver_dag": "eia_gas_price_raw_to_silver_dag",
    "eia_fuel_price_silver_dag": "eia_fuel_price_silver_dag",
    "lease_vehicle_inventory_raw_to_silver_dag": "lease_vehicle_inventory_raw_to_silver_dag",
    "monthly_taxi_trip_silver_to_gold_dag": "monthly_taxi_trip_silver_to_gold_dag",
    "monthly_taxi_trip_raw_to_silver_dag": "monthly_taxi_trip_dag",
}

# sub 에서 이동한 DAG 만 등록합니다 — 셸 글로브가 확장된 cron 이 그대로 통과한 적이
# 있어(sub 쪽 계약 참고) 스케줄은 셀 때마다 명시적으로 검사합니다.
SCHEDULES = {
    "eia_electricity_price_raw_to_silver_dag": "0 6 1 * *",
}

RETRY_CONTRACTS = {
    "eia_gas_price_raw_to_silver_pipeline": {
        "collection": {"raw_to_bronze"},
        "transform": {"bronze_to_silver": 10},
        "validation": {"validate_bronze", "validate_silver"},
    },
    "eia_electricity_price_raw_to_silver_pipeline": {
        "collection": {"raw_to_bronze"},
        "transform": {"bronze_to_silver": 10},
        "validation": {"validate_bronze", "validate_silver"},
    },
}


@pytest.mark.parametrize(("module_name", "dag_variable"), DAG_VARIABLES.items())
def test_DAG_동시실행과_catchup_계약을_유지한다(module_name, dag_variable):
    dag = getattr(importlib.import_module(f"dags.{module_name}"), dag_variable)

    assert dag.catchup is False
    assert dag.max_active_runs == MAX_ACTIVE_SERVICE_AREA_RUNS


@pytest.mark.parametrize(
    ("module_name", "dag_variable", "expected_schedule"),
    [
        (module_name, DAG_VARIABLES[module_name], schedule)
        for module_name, schedule in SCHEDULES.items()
    ],
)
def test_DAG_schedule을_유지한다(module_name, dag_variable, expected_schedule):
    module = importlib.import_module(f"dags.{module_name}")
    assert getattr(module, dag_variable).schedule == expected_schedule


@pytest.mark.parametrize(("dag_id", "contract"), RETRY_CONTRACTS.items())
def test_모든_task는_장애유형에_맞는_retry_정책을_쓴다(dag_id, contract):
    dags = {
        dag.dag_id: dag
        for module_name, dag_variable in DAG_VARIABLES.items()
        for dag in [getattr(importlib.import_module(f"dags.{module_name}"), dag_variable)]
    }

    dag = dags[dag_id]
    expected_task_ids = (
        contract["collection"] | set(contract["transform"]) | contract["validation"]
    )
    assert {task.task_id for task in dag.tasks} == expected_task_ids

    for task_id in contract["collection"]:
        task = dag.get_task(task_id)
        assert task.retries == 2
        assert task.retry_delay == timedelta(minutes=5)
        assert task.retry_exponential_backoff is True

    for task_id, expected_delay in contract["transform"].items():
        task = dag.get_task(task_id)
        assert task.retries == 1
        assert task.retry_delay.total_seconds() == expected_delay * 60

    for task_id in contract["validation"]:
        assert dag.get_task(task_id).retries == 0
        
def test_차량_교체_추천_기준선_기본값은_서비스_조건인_600이다():
    """콜 리스트에 오르는 기준입니다. 낮추면 성사 못 할 기사까지 담당자에게 넘어가고,
    올리면 제안할 수 있었던 기사가 빠집니다. 값을 바꾸려면 이 테스트를 함께 고치면서
    docs/METRICS.md 의 근거도 같이 바꾸라는 뜻으로 못박습니다 (#492)."""
    dag = getattr(
        importlib.import_module("dags.monthly_taxi_trip_silver_to_gold_dag"),
        "monthly_taxi_trip_silver_to_gold_dag",
    )

    assert dag.params["threshold_profit_increase"] == 600.0


def test_EIA_전력_충전단가_markup_기본값은_Variable_미설정시_2다():
    """#743 — Param 기본값을 Variable(eia_electricity_markup)에서 가져오도록 바꿨다.
    Variable을 설정하지 않은 상태(테스트 DB는 매번 비어 있음)에서는 이전 리터럴
    기본값 2.0과 동일하게 동작해야 한다(회귀 없음)."""
    dag = getattr(
        importlib.import_module("dags.eia_electricity_price_raw_to_silver_dag"),
        "eia_electricity_price_raw_to_silver_dag",
    )

    assert dag.params["markup"] == 2.0


def test_HVFHV_불합격_허용비율_기본값은_Variable_미설정시_0_05다():
    """#743 — MONTHLY_TAXI_TRIP_ERROR_THRESHOLD 모듈 상수를 error_threshold Param으로
    승격하고 기본값을 Variable(hvfhv_error_threshold)에서 가져오도록 바꿨다. Variable을
    설정하지 않은 상태에서는 이전 리터럴 기본값 0.05와 동일해야 한다(회귀 없음)."""
    dag = getattr(
        importlib.import_module("dags.monthly_taxi_trip_raw_to_silver_dag"),
        "monthly_taxi_trip_dag",
    )

    assert dag.params["error_threshold"] == 0.05


def _reload_with_fresh_params(module_path: str, probe_name: str):
    """DAG 모듈을 별도 이름으로 다시 실행해 top-level Param()을 재평가시킵니다.

    일반 재import는 캐시된 모듈을 그대로 돌려줘 Variable.set() 이후에도 예전
    Param 기본값을 봅니다. 다른 테스트가 붙들고 있는 정본 DAG 객체와 동일성
    비교가 어긋나지 않도록, 정본은 그대로 두고 별도 이름으로만 다시 읽습니다.
    """
    import importlib.util

    canonical = importlib.import_module(module_path)
    spec = importlib.util.spec_from_file_location(probe_name, canonical.__file__)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_차량_교체_추천_기준선은_Variable_설정시_그값을_따른다():
    """#743 — 키 이름이 잘못돼도 Variable.get이 조용히 기본값만 계속 반환해
    "미설정 시 회귀 없음" 테스트만으로는 못 잡는다. Variable을 실제로 설정해
    그 값이 그대로 반영되는지까지 확인한다."""
    from airflow.models import Variable

    Variable.set("gold_profit_threshold", "777.0")
    try:
        module = _reload_with_fresh_params(
            "dags.monthly_taxi_trip_silver_to_gold_dag", "_gold_dag_variable_probe"
        )
        assert module.monthly_taxi_trip_silver_to_gold_dag.params[
            "threshold_profit_increase"
        ] == 777.0
    finally:
        Variable.delete("gold_profit_threshold")


def test_EIA_전력_충전단가_markup은_Variable_설정시_그값을_따른다():
    from airflow.models import Variable

    Variable.set("eia_electricity_markup", "3.5")
    try:
        module = _reload_with_fresh_params(
            "dags.eia_electricity_price_raw_to_silver_dag",
            "_eia_electricity_dag_variable_probe",
        )
        assert module.eia_electricity_price_raw_to_silver_dag.params["markup"] == 3.5
    finally:
        Variable.delete("eia_electricity_markup")


def test_HVFHV_불합격_허용비율은_Variable_설정시_그값을_따른다():
    from airflow.models import Variable

    Variable.set("hvfhv_error_threshold", "0.2")
    try:
        module = _reload_with_fresh_params(
            "dags.monthly_taxi_trip_raw_to_silver_dag",
            "_monthly_taxi_trip_raw_to_silver_dag_variable_probe",
        )
        assert module.monthly_taxi_trip_dag.params["error_threshold"] == 0.2
    finally:
        Variable.delete("hvfhv_error_threshold")


def _decorator_name(decorator: ast.expr) -> str | None:
    target = decorator.func if isinstance(decorator, ast.Call) else decorator
    return target.id if isinstance(target, ast.Name) else None


RUNTIME_TAGS = {"main", "sub"}


def _dag_tags(dag_path: Path) -> list[str]:
    tree = ast.parse(dag_path.read_text(encoding="utf-8"), filename=str(dag_path))
    for definition in (n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)):
        for decorator in definition.decorator_list:
            if not isinstance(decorator, ast.Call) or _decorator_name(decorator) != "dag":
                continue
            for keyword in decorator.keywords:
                if keyword.arg == "tags":
                    return [ast.literal_eval(element) for element in keyword.value.elts]
    raise AssertionError(f"{dag_path.name} 에 @dag(tags=[...]) 가 없습니다")


@pytest.mark.parametrize(
    "dag_path",
    sorted(DAGS_DIR.glob("*_dag.py")),
    ids=lambda path: path.name,
)
def test_DAG_태그_첫_원소는_런타임_구분자_main_이다(dag_path):
    """main·sub 가 한 DAGS_FOLDER 에 마운트돼 웹 UI 목록에서 섞입니다. 도메인 태그로는
    못 가릅니다 — `hvfhv` 는 양쪽 DAG 에 다 붙어 있습니다. 그래서 런타임 구분자를 첫
    원소로 못박아 태그 필터 한 번으로 갈라 보게 합니다 (#632)."""
    tags = _dag_tags(dag_path)

    assert tags[0] == "main"
    assert RUNTIME_TAGS & set(tags[1:]) == set()
