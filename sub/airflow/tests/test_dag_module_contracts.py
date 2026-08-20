"""DAG 선언과 실행 모듈 분리 계약.

1. DAG 파일에는 단 하나의 ``@dag`` 팩토리만 정의
2. cron schedule 유지 — 여기 등록되지 않은 DAG 는 스케줄이 검사되지 않습니다.
   실제로 EIA DAG 를 빠뜨렸을 때 셸 글로브가 확장된 cron 이 그대로 통과했습니다.
3. 모든 task의 수집·변환·검증별 retry 정책 유지
4. Vehicle Master와 EIA 통합의 핵심 task 의존성 유지
"""

import ast
import importlib
from datetime import timedelta
from pathlib import Path

import pytest


DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"

DAG_VARIABLES = {
    "driver_master_raw_to_silver_dag": "driver_master_raw_to_silver_dag",
    "fueleconomy_vehicle_specs_raw_to_silver_dag": "fueleconomy_vehicle_specs_dag",
    "hvfhv_driver_trip_silver_dag": "hvfhv_driver_trip_silver_dag",
    "hvfhv_raw_to_silver_dag": "hvfhv_dag",
    "hvfhv_silver_to_gold_dag": "hvfhv_silver_to_gold_dag",
    "lyft_eligible_vehicles_raw_to_silver_dag": "lyft_eligible_vehicles_dag",
    "uber_eligible_vehicles_raw_to_silver_dag": "uber_eligible_vehicles_dag",
    "vehicle_catalog_raw_to_silver_dag": "vehicle_catalog_dag",
    "vehicle_master_silver_dag": "vehicle_master_dag",
}
DAG_VARIABLES = {
    module_name: variable_name
    for module_name, variable_name in DAG_VARIABLES.items()
    if (DAGS_DIR / f"{module_name}.py").exists()
}

SCHEDULES = {
    # EIA 파일에는 이력이 통째로 들어 있어 매일 받을 이유가 없습니다. 월 1회 갱신은
    # 과거 값 개정분을 확보하기 위한 것입니다.
    "driver_master_raw_to_silver_dag": "0 0 10 * *",
    "hvfhv_raw_to_silver_dag": "0 0 10 * *",
    "vehicle_catalog_raw_to_silver_dag": "0 3 * * 1",
    "lyft_eligible_vehicles_raw_to_silver_dag": "0 4 * * 1",
    "uber_eligible_vehicles_raw_to_silver_dag": "0 5 * * 1",
}

RETRY_CONTRACTS = {
    "driver_master_raw_to_silver_pipeline": {
        "collection": {"raw_to_bronze"},
        "transform": {"bronze_to_silver": 15},
        "validation": {"validate_bronze", "validate_silver"},
    },
    "fueleconomy_vehicle_specs_raw_to_silver_pipeline": {
        "collection": {"raw_to_bronze"},
        "transform": {"bronze_to_silver": 15},
        "validation": {"validate_bronze", "validate_silver"},
    },
    "hvfhv_driver_trip_silver_pipeline": {
        "collection": set(),
        "transform": {"build_driver_trip_silver": 30},
        "validation": {"validate_inputs", "validate_silver"},
    },
    "hvfhv_raw_to_silver_pipeline": {
        "collection": {"raw_to_bronze"},
        "transform": {"bronze_to_silver": 30},
        "validation": {"validate_bronze", "validate_silver"},
    },
    "hvfhv_silver_to_gold_pipeline": {
        "collection": set(),
        "transform": {"build_gold": 10},
        "validation": {"validate_inputs", "validate_gold"},
    },
    "lyft_eligible_vehicles_raw_to_silver_pipeline": {
        "collection": {"raw_to_bronze"},
        "transform": {"bronze_to_silver": 15},
        "validation": {"validate_bronze", "validate_silver"},
    },
    "uber_eligible_vehicles_raw_to_silver_pipeline": {
        "collection": {"raw_to_bronze"},
        "transform": {"bronze_to_silver": 15},
        "validation": {"validate_bronze", "validate_silver"},
    },
    "vehicle_catalog_raw_to_silver_pipeline": {
        "collection": {"raw_to_bronze"},
        "transform": {"bronze_to_silver": 30},
        "validation": {"validate_bronze", "validate_silver"},
    },
    "vehicle_master_silver_pipeline": {
        "collection": set(),
        "transform": {"build_vehicle_master": 15},
        "validation": {"validate_silver"},
    },
}
RETRY_CONTRACTS = {
    dag_id: contract
    for dag_id, contract in RETRY_CONTRACTS.items()
    if dag_id
    in {
        "fueleconomy_vehicle_specs_raw_to_silver_pipeline",
        "lyft_eligible_vehicles_raw_to_silver_pipeline",
        "uber_eligible_vehicles_raw_to_silver_pipeline",
        "vehicle_catalog_raw_to_silver_pipeline",
        "vehicle_master_silver_pipeline",
    }
}


def _decorator_name(decorator: ast.expr) -> str | None:
    target = decorator.func if isinstance(decorator, ast.Call) else decorator
    return target.id if isinstance(target, ast.Name) else None


@pytest.mark.parametrize(
    "dag_path",
    sorted(DAGS_DIR.glob("*_dag.py")),
    ids=lambda path: path.name,
)
def test_DAG_파일은_dag_팩토리만_정의한다(dag_path):
    tree = ast.parse(dag_path.read_text(encoding="utf-8"), filename=str(dag_path))
    definitions = [node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)]

    assert len(definitions) == 1, [node.name for node in definitions]
    assert any(
        _decorator_name(decorator) == "dag"
        for decorator in definitions[0].decorator_list
    )


@pytest.mark.parametrize(
    ("module_name", "dag_variable", "expected_schedule"),
    [
        (module_name, DAG_VARIABLES[module_name], schedule)
        for module_name, schedule in SCHEDULES.items()
        if module_name in DAG_VARIABLES
    ],
)
def test_DAG_schedule은_분리_전과_같다(
    module_name, dag_variable, expected_schedule
):
    module = importlib.import_module(f"dags.{module_name}")
    assert getattr(module, dag_variable).schedule == expected_schedule


@pytest.mark.parametrize(("module_name", "dag_variable"), DAG_VARIABLES.items())
def test_DAG_동시실행과_catchup_계약은_분리_전과_같다(
    module_name, dag_variable
):
    dag = getattr(importlib.import_module(f"dags.{module_name}"), dag_variable)
    assert dag.catchup is False
    assert dag.max_active_runs == 1


@pytest.mark.parametrize(
    ("dag_id", "contract"),
    RETRY_CONTRACTS.items(),
)
def test_모든_task는_장애유형에_맞는_retry_정책을_쓴다(dag_id, contract):
    dags = {
        dag.dag_id: dag
        for module_name, dag_variable in DAG_VARIABLES.items()
        for dag in [
            getattr(importlib.import_module(f"dags.{module_name}"), dag_variable)
        ]
    }

    dag = dags[dag_id]
    expected_task_ids = (
        contract["collection"]
        | set(contract["transform"])
        | contract["validation"]
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


def test_Vehicle_Master_DAG의_공개_계약을_유지한다():
    module = importlib.import_module("dags.vehicle_master_silver_dag")
    dag = module.vehicle_master_dag

    assert dag.dag_id == "vehicle_master_silver_pipeline"
    assert {task.task_id for task in dag.tasks} == {
        "build_vehicle_master",
        "validate_silver",
    }
    assert dag.get_task("validate_silver").upstream_task_ids == {
        "build_vehicle_master"
    }


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
def test_DAG_태그_첫_원소는_런타임_구분자_sub_이다(dag_path):
    """main·sub 가 한 DAGS_FOLDER 에 마운트돼 웹 UI 목록에서 섞입니다. 도메인 태그로는
    못 가릅니다 — `hvfhv` 는 양쪽 DAG 에 다 붙어 있습니다. 그래서 런타임 구분자를 첫
    원소로 못박아 태그 필터 한 번으로 갈라 보게 합니다 (#632)."""
    tags = _dag_tags(dag_path)

    assert tags[0] == "sub"
    assert RUNTIME_TAGS & set(tags[1:]) == set()
