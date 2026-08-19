"""Airflow Slack 알림 단계와 DAG 연결 시나리오.

1. Retry Alert와 Final Fail은 서로 다른 상태와 공통 실행 정보를 표시
2. 모든 DAG Task는 Retry Alert와 Final Fail 콜백을 상속
3. Slack provider가 없어도 로깅 fallback으로 DAG import 유지
"""

import importlib
from pathlib import Path

import pytest

from shared.airflow.common.slack_failure_callback import (
    SLACK_FAILURE_TEXT,
    SLACK_RETRY_ALERT_TEXT,
    SLACK_WEBHOOK_CONN_ID,
    slack_failure_callback,
    slack_retry_alert_callback,
)


DAG_MODULES = {
    "dags.driver_vehicle_monthly_snapshot_raw_to_silver_dag": "driver_vehicle_monthly_snapshot_raw_to_silver_dag",
    "dags.eia_electricity_price_raw_to_bronze_dag": (
        "eia_electricity_price_raw_to_bronze_dag"
    ),
    "dags.eia_electricity_price_bronze_to_silver_dag": (
        "eia_electricity_price_bronze_to_silver_dag"
    ),
    "dags.eia_fuel_price_silver_dag": "eia_fuel_price_silver_dag",
    "dags.eia_gas_price_raw_to_bronze_dag": "eia_gas_price_raw_to_bronze_dag",
    "dags.eia_gas_price_bronze_to_silver_dag": (
        "eia_gas_price_bronze_to_silver_dag"
    ),
    "dags.fueleconomy_vehicle_specs_raw_to_silver_dag": (
        "fueleconomy_vehicle_specs_dag"
    ),
    "dags.hvfhv_raw_to_silver_dag": "hvfhv_dag",
    "dags.lyft_eligible_vehicles_raw_to_silver_dag": (
        "lyft_eligible_vehicles_dag"
    ),
    "dags.uber_eligible_vehicles_raw_to_silver_dag": (
        "uber_eligible_vehicles_dag"
    ),
    "dags.vehicle_catalog_raw_to_silver_dag": "vehicle_catalog_dag",
    "dags.vehicle_master_silver_dag": "vehicle_master_dag",
}
DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"
DAG_MODULES = {
    module_name: dag_variable
    for module_name, dag_variable in DAG_MODULES.items()
    if (DAGS_DIR / f"{module_name.rsplit('.', 1)[-1]}.py").exists()
}


@pytest.mark.parametrize(
    ("text", "heading"),
    [
        (SLACK_RETRY_ALERT_TEXT, ":warning: *Airflow Task Alert*"),
        (SLACK_FAILURE_TEXT, ":red_circle: *Airflow Task Fail*"),
    ],
)
def test_Slack_알림은_상태와_실행_정보를_표시한다(text, heading):
    assert heading in text
    for expected in (
        "{{ dag.dag_id }}",
        "{{ ti.task_id }}",
        "{{ run_id }}",
        "{{ ti.try_number }}",
        "{{ task.retries + 1 }}",
        "{{ ti.log_url }}",
    ):
        assert expected in text


def test_Retry_Alert와_Final_Fail은_서로_다른_콜백이다():
    assert SLACK_WEBHOOK_CONN_ID == "slack_webhook"
    assert slack_retry_alert_callback is not slack_failure_callback


@pytest.mark.parametrize("module_name,dag_variable", DAG_MODULES.items())
def test_모든_DAG_Task에_Retry와_Fail_콜백이_연결된다(
    module_name, dag_variable
):
    module = importlib.import_module(module_name)
    dag = getattr(module, dag_variable)

    assert len(dag.tasks) > 0
    for airflow_task in dag.tasks:
        assert module.slack_retry_alert_callback in airflow_task.on_retry_callback
        assert module.slack_failure_callback in airflow_task.on_failure_callback


def test_Slack_provider가_없어도_fallback_콜백은_호출된다(caplog):
    if not getattr(slack_retry_alert_callback, "is_fallback", False):
        pytest.skip("Slack provider가 설치된 환경")

    context = {"task_instance": type("TI", (), {"task_id": "smoke"})()}
    slack_retry_alert_callback(context)
    slack_failure_callback(context)

    assert "Task 재시도 예정: smoke" in caplog.text
    assert "Task 최종 실패: smoke" in caplog.text
