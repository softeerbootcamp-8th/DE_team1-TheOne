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


def test_차량_교체_추천_기준선_기본값은_서비스_조건인_600이다():
    """콜 리스트에 오르는 기준입니다. 낮추면 성사 못 할 기사까지 담당자에게 넘어가고,
    올리면 제안할 수 있었던 기사가 빠집니다. 값을 바꾸려면 이 테스트를 함께 고치면서
    docs/METRICS.md 의 근거도 같이 바꾸라는 뜻으로 못박습니다 (#492)."""
    dag = getattr(
        importlib.import_module("dags.hvfhv_silver_to_gold_dag"),
        "hvfhv_silver_to_gold_dag",
    )

    assert dag.params["threshold_profit_increase"] == 600.0
