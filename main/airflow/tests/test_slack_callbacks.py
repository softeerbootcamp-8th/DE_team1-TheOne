"""Airflow Slack 알림 단계와 DAG 연결 시나리오.

1. Retry Alert와 Final Fail은 서로 다른 상태와 공통 실행 정보를 표시
2. 모든 DAG Task는 Retry Alert와 Final Fail 콜백을 상속
3. Slack provider가 없어도 로깅 fallback으로 DAG import 유지
4. 그 fallback 이 지금 쓰이고 있으면 실패 — 알림이 죽은 채로 초록불이면
   모든 검증 가드의 실패가 아무한테도 안 감 (#546)
5. 렌더한 결과의 시도 표기가 분자·분모 같은 축을 씀 — clear 후 재실행에서
   `2 / 1` 이 나가던 문제 (#550)
6. 실패 사유를 싣되 길면 자름. 없으면 `None` 이 아니라 `(사유 없음)`
"""

import importlib
import sys
from pathlib import Path

import pytest

from shared.airflow.common.slack_failure_callback import (
    REASON_MAX_CHARS,
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
    "dags.eia_gas_price_raw_to_silver_dag": "eia_gas_price_raw_to_silver_dag",
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
        "{{ ti.max_tries + 1 }}",
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


# --- 실제 렌더 결과 (#550) ----------------------------------------------------
# 위 자리표시자 테스트는 이름이 있는지만 봅니다. `task.retries` 를 쓰던 시절에도
# 통과했고, 실제로는 `2 / 1` 이 나갔습니다. 여기서는 값을 넣어 렌더한 문자열을 봅니다.

ALERT_TEXTS = [SLACK_RETRY_ALERT_TEXT, SLACK_FAILURE_TEXT]


def _ti(try_number: int = 1, max_tries: int = 0):
    return type(
        "TI", (),
        {"task_id": "validate_inputs", "try_number": try_number,
         "max_tries": max_tries, "log_url": "http://airflow/log"},
    )


def _render(text: str, **overrides) -> str:
    from jinja2 import Template

    context = {
        "dag": type("DAG", (), {"dag_id": "hvfhv_driver_trip_silver_pipeline"}),
        "ti": _ti(),
        "run_id": "manual__2026-08-19T05:17:03",
        "exception": None,
    }
    context.update(overrides)
    return Template(text).render(**context)


@pytest.mark.parametrize("text", ALERT_TEXTS)
def test_clear_후_재실행해도_시도_표기가_어긋나지_않는다(text):
    """`retries=0` 태스크를 clear 하고 다시 돌린 상황.

    `task.retries` 는 정적 0 이라 분모가 1 로 고정되는데 분자만 2 로 올라가
    `2 / 1` 이 나갔습니다. `ti.max_tries` 는 clear 때 `try_number + retries` 로
    누적되므로 분자와 같은 축에 있습니다.
    """
    rendered = _render(text, ti=_ti(try_number=2, max_tries=1))

    assert "`2 / 2`" in rendered


@pytest.mark.parametrize("text", ALERT_TEXTS)
def test_실패_사유가_본문에_실린다(text):
    rendered = _render(text, exception=ValueError("배정 행 수가 보존되지 않았습니다"))

    assert "배정 행 수가 보존되지 않았습니다" in rendered


@pytest.mark.parametrize("text", ALERT_TEXTS)
def test_긴_예외는_잘라서_싣는다(text):
    """Spark `Py4JJavaError` 는 수백 줄입니다. 그대로 실으면 Slack 이 거부합니다."""
    rendered = _render(text, exception=RuntimeError("스택" * 1000))

    reason = next(line for line in rendered.splitlines() if line.startswith("*사유*"))
    assert len(reason) < REASON_MAX_CHARS + 50
    assert reason.endswith("...`")


@pytest.mark.parametrize("text", ALERT_TEXTS)
def test_예외가_없으면_None_이_아니라_사유_없음으로_찍는다(text):
    """`exception` 은 None 일 수 있고, `| string` 을 먼저 태우면 "None" 이 찍힙니다."""
    rendered = _render(text, exception=None)

    assert "(사유 없음)" in rendered
    assert "None" not in rendered
