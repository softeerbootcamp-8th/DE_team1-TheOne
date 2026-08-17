"""DAG 선언과 실행 모듈 분리 계약.

1. DAG 파일에는 단 하나의 ``@dag`` 팩토리만 정의
2. 누락됐던 7개 cron schedule 유지
3. 모든 task의 수집·변환·검증별 retry 정책 유지
4. Vehicle Master와 Gas/EV의 핵심 task 의존성 유지
"""

import ast
import importlib
from datetime import timedelta
from pathlib import Path

import pytest


DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"

DAG_VARIABLES = {
    "ev_charging_price_raw_to_bronze_dag": "ev_charging_price_raw_to_bronze_dag",
    "eia_gas_price_raw_to_bronze_dag": "eia_gas_price_raw_to_bronze_dag",
    "eia_electricity_price_raw_to_bronze_dag": "eia_electricity_price_raw_to_bronze_dag",
    "eia_fuel_price_bronze_to_silver_dag": "eia_fuel_price_bronze_to_silver_dag",
    "fueleconomy_vehicle_specs_raw_to_silver_dag": "fueleconomy_vehicle_specs_dag",
    "gas_ev_price_bronze_to_silver_dag": "gas_ev_price_bronze_to_silver_dag",
    "gas_price_raw_to_bronze_dag": "gas_price_raw_to_bronze_dag",
    "hvfhv_driver_trip_silver_dag": "hvfhv_driver_trip_silver_dag",
    "hvfhv_raw_to_silver_dag": "hvfhv_dag",
    "hvfhv_silver_to_gold_dag": "hvfhv_silver_to_gold_dag",
    "lyft_eligible_vehicles_raw_to_silver_dag": "lyft_eligible_vehicles_dag",
    "uber_eligible_vehicles_raw_to_silver_dag": "uber_eligible_vehicles_dag",
    "vehicle_catalog_raw_to_silver_dag": "vehicle_catalog_dag",
    "vehicle_master_silver_dag": "vehicle_master_dag",
}

SCHEDULES = {
    # EIA 파일에는 이력이 통째로 들어 있어 매일 받을 이유가 없습니다. 월 1회 갱신은
    # 과거 값 개정분을 확보하기 위한 것입니다.
    "eia_gas_price_raw_to_bronze_dag": "0 5 1 * *",
    "eia_electricity_price_raw_to_bronze_dag": "0 6 1 * *",
    "ev_charging_price_raw_to_bronze_dag": "0 9 * * *",
    "gas_price_raw_to_bronze_dag": "0 9 * * *",
    "gas_ev_price_bronze_to_silver_dag": "0 10 1 * *",
    "hvfhv_raw_to_silver_dag": "0 0 10 * *",
    "vehicle_catalog_raw_to_silver_dag": "0 3 * * 1",
    "lyft_eligible_vehicles_raw_to_silver_dag": "0 4 * * 1",
    "uber_eligible_vehicles_raw_to_silver_dag": "0 5 * * 1",
}

RETRY_CONTRACTS = {
    "eia_gas_price_raw_to_bronze_pipeline": {
        "collection": {"raw_to_bronze"},
        "transform": {},
        "validation": {"validate_bronze"},
    },
    "eia_electricity_price_raw_to_bronze_pipeline": {
        "collection": {"raw_to_bronze"},
        "transform": {},
        "validation": {"validate_bronze"},
    },
    "eia_fuel_price_bronze_to_silver_pipeline": {
        "collection": set(),
        "transform": {"bronze_to_silver": 10},
        "validation": {"check_bronze", "validate_silver"},
    },
    "ev_charging_price_raw_to_bronze_pipeline": {
        "collection": {"raw_to_bronze"},
        "transform": {},
        "validation": {"validate_bronze"},
    },
    "fueleconomy_vehicle_specs_raw_to_silver_pipeline": {
        "collection": {"raw_to_bronze"},
        "transform": {"bronze_to_silver": 15},
        "validation": {"validate_bronze", "validate_silver"},
    },
    "gas_ev_price_bronze_to_silver_pipeline": {
        "collection": set(),
        "transform": {
            "gas_bronze_to_silver": 10,
            "ev_bronze_to_silver": 10,
            "integrate_silver": 10,
        },
        "validation": {
            "check_month_completeness",
            "validate_gas_silver",
            "validate_ev_silver",
            "validate_integrated_silver",
        },
    },
    "gas_price_raw_to_bronze_pipeline": {
        "collection": {"raw_to_bronze"},
        "transform": {},
        "validation": {"validate_bronze"},
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


def test_Gas_EV_DAG의_개별_Silver_검증_의존성을_유지한다():
    module = importlib.import_module("dags.gas_ev_price_bronze_to_silver_dag")
    dag = module.gas_ev_price_bronze_to_silver_dag

    assert "gas_bronze_to_silver" in dag.get_task(
        "validate_gas_silver"
    ).upstream_task_ids
    assert "ev_bronze_to_silver" in dag.get_task(
        "validate_ev_silver"
    ).upstream_task_ids
