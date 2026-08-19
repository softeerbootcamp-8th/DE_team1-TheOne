"""Airflow Slack 알림 단계와 DAG 연결 시나리오.

1. Retry Alert와 Final Fail은 서로 다른 상태와 공통 실행 정보를 표시
2. 모든 DAG Task는 Retry Alert와 Final Fail 콜백을 상속
3. Slack provider가 없어도 로깅 fallback으로 DAG import 유지
4. 그 fallback 이 지금 쓰이고 있으면 실패 — 알림이 죽은 채로 초록불이면
   모든 검증 가드의 실패가 아무한테도 안 감 (#546)
"""

import importlib
import sys
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
    "dags.driver_master_raw_to_silver_dag": "driver_master_raw_to_silver_dag",
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
    "dags.hvfhv_driver_trip_silver_dag": "hvfhv_driver_trip_silver_dag",
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


def test_지금_쓰는_콜백은_로깅_fallback_이_아니다():
    """폴백이면 실패가 Airflow 로그에만 남고 아무한테도 안 갑니다.

    예전에는 이 자리에서 `pytest.skip` 을 했습니다. 그러면 provider 가 있어도
    없어도 초록불이라, 실제로는 DAG 16개의 알림이 죽어 있는데 CI 는 통과했습니다
    (#546). `apache-airflow-providers-slack` 은 이제 선언된 의존성이므로 폴백은
    "설치가 빠졌다" 는 신호입니다.
    """
    for callback in (slack_retry_alert_callback, slack_failure_callback):
        assert not getattr(callback, "is_fallback", False), (
            "Slack provider 가 없어 로깅 폴백을 쓰고 있습니다. "
            "main/airflow 에서 `uv sync --frozen` 을 돌리세요."
        )


def test_provider가_없으면_fallback_콜백이_로그로_대체한다(caplog, monkeypatch):
    """폴백 자체는 살려 둡니다 — provider 가 빠져도 DAG import 는 살아야 합니다.

    설치 여부에 좌우되지 않도록 import 를 막고 **별도 이름으로** 다시 읽습니다.
    정규 모듈을 reload 하면 콜백 객체가 새로 만들어져, 이미 그 객체를 붙들고 있는
    DAG 들과 동일성 비교가 어긋납니다(위 연결 테스트).
    """
    import shared.airflow.common.slack_failure_callback as canonical

    for name in [n for n in sys.modules if n.startswith("airflow.providers.slack")]:
        monkeypatch.delitem(sys.modules, name, raising=False)
    monkeypatch.setitem(sys.modules, "airflow.providers.slack", None)

    spec = importlib.util.spec_from_file_location("_slack_no_provider", canonical.__file__)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    assert module.slack_failure_callback.is_fallback

    context = {"task_instance": type("TI", (), {"task_id": "smoke"})()}
    module.slack_retry_alert_callback(context)
    module.slack_failure_callback(context)

    assert "Task 재시도 예정: smoke" in caplog.text
    assert "Task 최종 실패: smoke" in caplog.text
