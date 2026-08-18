"""메인 데이터 프로덕트 DAG의 공개 실행 계약."""

import importlib
from datetime import timedelta

import pytest


DAG_VARIABLES = {
    "driver_master_raw_to_silver_dag": "driver_master_raw_to_silver_dag",
    "eia_electricity_price_raw_to_bronze_dag": "eia_electricity_price_raw_to_bronze_dag",
    "eia_electricity_price_bronze_to_silver_dag": "eia_electricity_price_bronze_to_silver_dag",
    "eia_gas_price_raw_to_bronze_dag": "eia_gas_price_raw_to_bronze_dag",
    "eia_gas_price_bronze_to_silver_dag": "eia_gas_price_bronze_to_silver_dag",
    "hvfhv_driver_trip_silver_dag": "hvfhv_driver_trip_silver_dag",
    "lease_vehicle_inventory_raw_to_silver_dag": "lease_vehicle_inventory_raw_to_silver_dag",
    "hvfhv_raw_to_silver_dag": "hvfhv_dag",
    "hvfhv_silver_to_gold_dag": "hvfhv_silver_to_gold_dag",
}

# sub 에서 이동한 DAG 만 등록합니다 — 셸 글로브가 확장된 cron 이 그대로 통과한 적이
# 있어(sub 쪽 계약 참고) 스케줄은 셀 때마다 명시적으로 검사합니다.
SCHEDULES = {
    "eia_electricity_price_raw_to_bronze_dag": "0 6 1 * *",
}

RETRY_CONTRACTS = {
    "eia_electricity_price_raw_to_bronze_pipeline": {
        "collection": {"raw_to_bronze"},
        "transform": {},
        "validation": {"validate_bronze"},
    },
}


@pytest.mark.parametrize(("module_name", "dag_variable"), DAG_VARIABLES.items())
def test_DAG_동시실행과_catchup_계약을_유지한다(module_name, dag_variable):
    dag = getattr(importlib.import_module(f"dags.{module_name}"), dag_variable)

    assert dag.catchup is False
    assert dag.max_active_runs == 1


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
        importlib.import_module("dags.hvfhv_silver_to_gold_dag"),
        "hvfhv_silver_to_gold_dag",
    )

    assert dag.params["threshold_profit_increase"] == 600.0
