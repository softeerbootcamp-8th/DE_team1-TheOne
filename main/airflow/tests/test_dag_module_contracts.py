"""메인 데이터 프로덕트 DAG의 공개 실행 계약."""

import importlib

import pytest


DAG_VARIABLES = {
    "driver_master_raw_to_silver_dag": "driver_master_raw_to_silver_dag",
    "hvfhv_driver_trip_silver_dag": "hvfhv_driver_trip_silver_dag",
    "hvfhv_raw_to_silver_dag": "hvfhv_dag",
    "hvfhv_silver_to_gold_dag": "hvfhv_silver_to_gold_dag",
}


@pytest.mark.parametrize(("module_name", "dag_variable"), DAG_VARIABLES.items())
def test_DAG_동시실행과_catchup_계약을_유지한다(module_name, dag_variable):
    dag = getattr(importlib.import_module(f"dags.{module_name}"), dag_variable)

    assert dag.catchup is False
    assert dag.max_active_runs == 1
