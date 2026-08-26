"""Airflow Slack 알림 단계와 DAG 연결 시나리오.

1. Retry Alert·Final Fail·Gold Success는 서로 다른 상태와 실행 정보를 표시
2. 모든 DAG Task는 Retry Alert와 Final Fail 콜백을 상속
3. Slack provider가 없어도 로깅 fallback으로 DAG import 유지
4. 그 fallback 이 지금 쓰이고 있으면 실패 — 알림이 죽은 채로 초록불이면
   모든 검증 가드의 실패가 아무한테도 안 감 (#546)
5. 렌더한 결과의 시도 표기가 분자·분모 같은 축을 씀 — clear 후 재실행에서
   `2 / 1` 이 나가던 문제 (#550)
6. 실패 사유를 싣되 길면 자름. 없으면 `None` 이 아니라 `(사유 없음)`.
   여러 줄이면 한 줄로 접음 — Slack 인라인 코드는 줄을 못 넘어서 EMR 실패
   사유처럼 줄바꿈이 있으면 백틱이 글자 그대로 찍힘
7. Block Kit은 상태·실행 유형·원인·조치를 먼저, 기술 식별자를 나중에 표시
8. Gold 성공은 대상 연월과 Asset 실행 유형을 표시
9. 모든 알림(텍스트·Block)이 파티션 키를 표시 — 지역 축(#674)이 들어가면 이걸로
   어느 지역이 죽었는지 가린다
10. 파티션이 없는 DAG(키 부재·None)에서도 StrictUndefined 렌더링이 죽지 않고 `-` 로 찍힘
11. 배포가 웹훅 커넥션을 환경변수로 넣음 — DB 에만 있으면 볼륨과 함께 조용히
    사라지고, 4번과 같은 '알림 죽은 초록불' 이 됩니다
"""

import importlib
import sys
from pathlib import Path

import pytest

from shared.airflow.common.slack_failure_callback import (
    REASON_MAX_CHARS,
    SLACK_FAILURE_BLOCKS,
    SLACK_FAILURE_TEXT,
    SLACK_RETRY_ALERT_BLOCKS,
    SLACK_RETRY_ALERT_TEXT,
    SLACK_SKIP_BLOCKS,
    SLACK_SKIP_TEXT,
    SLACK_STALE_BLOCKS,
    SLACK_STALE_TEXT,
    SLACK_SUCCESS_BLOCKS,
    SLACK_SUCCESS_TEXT,
    SLACK_WEBHOOK_CONN_ID,
    slack_failure_callback,
    slack_retry_alert_callback,
    slack_skip_alert_callback,
    slack_stale_alert_callback,
    slack_success_callback,
)


DAG_MODULES = {
    "dags.driver_vehicle_monthly_snapshot_raw_to_silver_dag": "driver_vehicle_monthly_snapshot_raw_to_silver_dag",
    "dags.eia_electricity_price_raw_to_silver_dag": (
        "eia_electricity_price_raw_to_silver_dag"
    ),
    "dags.eia_fuel_price_silver_dag": "eia_fuel_price_silver_dag",
    "dags.eia_gas_price_raw_to_silver_dag": "eia_gas_price_raw_to_silver_dag",
    "dags.fueleconomy_vehicle_specs_raw_to_silver_dag": (
        "fueleconomy_vehicle_specs_dag"
    ),
    "dags.monthly_taxi_trip_raw_to_silver_dag": "monthly_taxi_trip_dag",
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
ROOT = Path(__file__).resolve().parents[3]
DAG_MODULES = {
    module_name: dag_variable
    for module_name, dag_variable in DAG_MODULES.items()
    if (DAGS_DIR / f"{module_name.rsplit('.', 1)[-1]}.py").exists()
}


@pytest.mark.parametrize(
    ("text", "heading"),
    [
        (SLACK_RETRY_ALERT_TEXT, "⏳ *Airflow 태스크 재시도 예정*"),
        (SLACK_FAILURE_TEXT, "🚨 *Airflow 파이프라인 최종 실패*"),
    ],
)
def test_Slack_알림은_상태와_실행_정보를_표시한다(text, heading):
    assert heading in text
    for expected in (
        "{{ dag.dag_id }}",
        "{{ ti.task_id }}",
        "run_id",
        "{{ ti.try_number }}",
        "{{ ti.max_tries + 1 }}",
        "{{ ti.log_url }}",
    ):
        assert expected in text


def test_Retry_Fail_GoldSuccess는_서로_다른_콜백이다():
    assert SLACK_WEBHOOK_CONN_ID == "slack_webhook"
    assert len(
        {slack_retry_alert_callback, slack_failure_callback, slack_success_callback}
    ) == 3


def test_Gold_Success_알림은_실행정보를_표시한다():
    assert "✅ *Gold 생성 완료*" in SLACK_SUCCESS_TEXT
    for expected in (
        "{{ dag.dag_id }}",
        "run_id",
        "{{ ti.log_url }}",
    ):
        assert expected in SLACK_SUCCESS_TEXT


def test_Gold_Skip_알림은_원인과_실행정보를_표시한다():
    assert "⚠️ *Gold 파이프라인 입력 대기 (skip)*" in SLACK_SKIP_TEXT
    for expected in (
        "{{ dag.dag_id }}",
        "{{ ti.task_id }}",
        "run_id",
        "{{ ti.log_url }}",
    ):
        assert expected in SLACK_SKIP_TEXT


def test_Gold_Staleness_알림은_경과일과_SLA기준을_표시한다():
    assert "⏰ *Gold 파이프라인 staleness 경고*" in SLACK_STALE_TEXT
    for expected in (
        "{{ dag.dag_id }}",
        "{{ days_since_success }}",
        "{{ stale_days }}",
        "{{ ti.log_url }}",
    ):
        assert expected in SLACK_STALE_TEXT


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
    for callback in (
        slack_retry_alert_callback,
        slack_failure_callback,
        slack_success_callback,
        slack_skip_alert_callback,
        slack_stale_alert_callback,
    ):
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
    assert module.slack_success_callback.is_fallback
    assert module.slack_skip_alert_callback.is_fallback
    assert module.slack_stale_alert_callback.is_fallback

    context = {"task_instance": type("TI", (), {"task_id": "smoke"})()}
    module.slack_retry_alert_callback(context)
    module.slack_failure_callback(context)
    module.slack_success_callback(context)
    module.slack_skip_alert_callback(context)
    module.slack_stale_alert_callback({"days_since_success": 40, "stale_days": 31})

    assert "Task 재시도 예정: smoke" in caplog.text
    assert "Task 최종 실패: smoke" in caplog.text
    assert "Task 성공: smoke" in caplog.text
    assert "Task skip: smoke" in caplog.text
    assert "40일" in caplog.text and "31일" in caplog.text


# --- 실제 렌더 결과 (#550) ----------------------------------------------------
# 위 자리표시자 테스트는 이름이 있는지만 봅니다. `task.retries` 를 쓰던 시절에도
# 통과했고, 실제로는 `2 / 1` 이 나갔습니다. 여기서는 값을 넣어 렌더한 문자열을 봅니다.

ALERT_TEXTS = [SLACK_RETRY_ALERT_TEXT, SLACK_FAILURE_TEXT]


def _ti(try_number: int = 1, max_tries: int = 0):
    task_instance = type(
        "TI", (),
        {"task_id": "validate_inputs", "try_number": try_number,
         "max_tries": max_tries, "log_url": "http://airflow/log"},
    )()
    task_instance.xcom_pull = lambda **kwargs: {"year_month": "2026-08"}
    return task_instance


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


def _render_blocks(blocks, **overrides) -> str:
    def text_values(value):
        if isinstance(value, str):
            yield _render(value, **overrides)
        elif isinstance(value, dict):
            for nested in value.values():
                yield from text_values(nested)
        elif isinstance(value, list):
            for nested in value:
                yield from text_values(nested)

    return "\n".join(text_values(blocks))


def test_최종실패_Block은_원인과_조치를_먼저_보여준다():
    rendered = _render_blocks(
        SLACK_FAILURE_BLOCKS,
        ti=_ti(try_number=3, max_tries=2),
        exception=ValueError("입력 파티션이 없습니다: year_month=2150-05"),
    )

    for expected in (
        "🚨 파이프라인 최종 실패",
        "입력 파티션이 없습니다: year_month=2150-05",
        "입력·파라미터 확인 후 재실행",
        "수동 실행",
        "3 / 3",
        "hvfhv_driver_trip_silver_pipeline",
        "http://airflow/log",
    ):
        assert expected in rendered
    assert ":red_circle:" not in rendered


def test_Gold성공_Block은_대상연월과_Asset실행을_보여준다():
    rendered = _render_blocks(
        SLACK_SUCCESS_BLOCKS,
        run_id="asset_triggered__2026-08-20T09:22:58",
    )

    assert "✅ Gold 생성 완료" in rendered
    assert "2026-08" in rendered
    assert "Asset 트리거" in rendered
    assert "http://airflow/log" in rendered


def test_Skip_Block은_원인과_실행정보를_보여준다():
    rendered = _render_blocks(
        SLACK_SKIP_BLOCKS,
        run_id="asset_triggered__2026-08-20T09:22:58",
        exception=FileNotFoundError("Silver 4종 준비 대기: year_month=2026-08"),
    )

    for expected in (
        "⚠️ Gold 파이프라인 입력 대기 (skip)",
        "Silver 4종 준비 대기: year_month=2026-08",
        "Asset 트리거",
        "http://airflow/log",
    ):
        assert expected in rendered


def test_Staleness_Block은_경과일과_SLA기준을_보여준다():
    rendered = _render_blocks(
        SLACK_STALE_BLOCKS,
        days_since_success=40,
        stale_days=31,
    )

    assert "⏰ Gold 파이프라인 staleness 경고" in rendered
    assert "40일" in rendered
    assert "31일" in rendered


def test_notifier에_각상태_Block이_연결된다():
    assert slack_retry_alert_callback.blocks == SLACK_RETRY_ALERT_BLOCKS
    assert slack_failure_callback.blocks == SLACK_FAILURE_BLOCKS
    assert slack_success_callback.blocks == SLACK_SUCCESS_BLOCKS
    assert slack_skip_alert_callback.blocks == SLACK_SKIP_BLOCKS
    assert slack_stale_alert_callback.blocks == SLACK_STALE_BLOCKS


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

    reason = next(line for line in rendered.splitlines() if line.startswith("*원인*"))
    assert len(reason) < REASON_MAX_CHARS + 50
    assert reason.endswith("...`")


# EMR Serverless 실패 사유는 "Last few exceptions:" 뒤에 예외를 줄 단위로 붙입니다.
MULTILINE_EXCEPTION = RuntimeError(
    "EMR Serverless job 실패: Serverless Job failed: FAILED - Job failed, please check "
    "complete logs in configured logging destination. ExitCode: 1. Last few exceptions:\n"
    "psycopg2.errors.UniqueViolation: duplicate key value violates unique constraint\n"
    '"driver_aggregation_pkey"'
)


@pytest.mark.parametrize("text", ALERT_TEXTS)
def test_여러_줄_사유도_한_줄로_접어_싣는다(text):
    """Slack 의 인라인 코드(백틱 1개)는 줄을 넘지 못합니다. 여러 줄 사유를 그대로
    실으면 여는 백틱과 닫는 백틱이 서로 다른 줄에 놓여 양쪽 다 짝이 없어지고,
    Slack 이 코드로 렌더하지 않고 백틱을 글자 그대로 찍습니다 — 실제 EMR 실패
    알림이 이렇게 깨져서 왔습니다. 다른 항목은 값이 한 줄이라 멀쩡했습니다.
    """
    rendered = _render(text, exception=MULTILINE_EXCEPTION)

    reason = next(line for line in rendered.splitlines() if line.startswith("*원인*"))
    assert reason.count("`") == 2, "백틱이 짝이 맞아야 코드로 렌더됩니다"
    assert "driver_aggregation_pkey" in reason, "접느라 뒷부분을 잃으면 안 됩니다"


@pytest.mark.parametrize("text", ALERT_TEXTS)
def test_사유_안의_백틱이_코드_스팬을_깨지_않는다(text):
    """검증 실패 메시지가 경로를 백틱으로 감싸는 경우가 있습니다."""
    rendered = _render(text, exception=RuntimeError("`s3://de-theone/x` 가 없습니다"))

    reason = next(line for line in rendered.splitlines() if line.startswith("*원인*"))
    assert reason.count("`") == 2


@pytest.mark.parametrize("text", ALERT_TEXTS)
def test_예외가_없으면_None_이_아니라_사유_없음으로_찍는다(text):
    """`exception` 은 None 일 수 있고, `| string` 을 먼저 태우면 "None" 이 찍힙니다."""
    rendered = _render(text, exception=None)

    assert "(사유 없음)" in rendered
    assert "None" not in rendered


ALL_ALERT_TEXTS = [
    SLACK_RETRY_ALERT_TEXT,
    SLACK_FAILURE_TEXT,
    SLACK_SUCCESS_TEXT,
    SLACK_SKIP_TEXT,
    SLACK_STALE_TEXT,
]
ALL_ALERT_BLOCKS = [
    SLACK_RETRY_ALERT_BLOCKS,
    SLACK_FAILURE_BLOCKS,
    SLACK_SUCCESS_BLOCKS,
    SLACK_SKIP_BLOCKS,
    SLACK_STALE_BLOCKS,
]


@pytest.mark.parametrize("text", ALL_ALERT_TEXTS)
def test_모든_알림이_파티션을_표시한다(text):
    """지역 축(#674)이 들어가면 파티션 키가 "{service_area}:{year_month}" 가 되어
    알림만 보고 어느 지역이 죽었는지 가릴 수 있습니다. 지역마다 DAG 를 새로 만들지
    않는 설계라, 이 항목이 빠지면 N 개 지역이 한 DAG 로 들어올 때 온콜이 지역을
    구분할 방법이 없습니다."""
    assert "*파티션*" in text

    rendered = _render(text, partition_key="NYC:2026-08")

    assert "NYC:2026-08" in rendered


@pytest.mark.parametrize("blocks", ALL_ALERT_BLOCKS)
def test_모든_Block에도_파티션이_실린다(blocks):
    rendered = _render_blocks(blocks, partition_key="NYC:2026-08")

    assert "*파티션*" in rendered
    assert "NYC:2026-08" in rendered


@pytest.mark.parametrize("text", ALL_ALERT_TEXTS)
@pytest.mark.parametrize(
    ("context", "expected"),
    [
        ({}, "-"),
        ({"partition_key": None}, "-"),
        ({"partition_key": "NYC:2026-08"}, "NYC:2026-08"),
    ],
    ids=["키_부재", "None_값", "정상_값"],
)
def test_파티션이_없는_DAG에서도_알림이_렌더된다(text, context, expected):
    """`partition_key` 는 `NotRequired[str | None]` 이라 비파티션 DAG 에서는 컨텍스트에
    키가 아예 없거나 None 입니다. 그리고 Airflow 의 기본 렌더링은
    `StrictUndefined`(`airflow/sdk/definitions/dag.py`) 라, 방어 없이 쓰면 알림 자체가
    UndefinedError 로 죽습니다 — 실패를 알리려는 알림이 실패로 죽는 것이 가장 나쁩니다.
    `default(..., true)` 가 세 경우를 모두 처리하는지 실제 StrictUndefined 로 확인합니다.
    """
    from jinja2 import StrictUndefined, Template

    base = {
        "dag": type("DAG", (), {"dag_id": "monthly_taxi_trip_silver_to_gold_pipeline"}),
        "ti": _ti(),
        "run_id": "asset_triggered__2026-08-23T05:17:03",
        "exception": None,
        "days_since_success": 40,
        "stale_days": 31,
    }
    rendered = Template(text, undefined=StrictUndefined).render(**base, **context)

    partition_line = next(
        line for line in rendered.splitlines() if line.startswith("*파티션*")
    )
    assert partition_line == f"*파티션*: `{expected}`"


def test_배포는_웹훅_커넥션을_환경변수로_넣는다():
    """`airflow connections add` 로 손수 넣으면 값이 Postgres 볼륨에만 남습니다.
    볼륨 삭제나 인스턴스 교체로 사라져도 DAG 는 그대로 돌기 때문에, 알림만 끊긴 것을
    아무도 모릅니다. 배포마다 다시 써지는 환경변수라야 그 상태가 생기지 않습니다.

    조회 순서가 EnvironmentVariablesBackend -> MetastoreBackend 이므로 이 값이
    남아 있는 DB 행보다 우선합니다.
    """
    from shared.airflow.common.slack_failure_callback import SLACK_WEBHOOK_CONN_ID

    compose = (ROOT / "docker-compose.ec2.yml").read_text()
    workflow = (ROOT / ".github/workflows/deploy-airflow.yml").read_text()

    # 커넥션 id 를 바꾸면 환경변수 이름도 함께 바뀌어야 합니다
    assert f"AIRFLOW_CONN_{SLACK_WEBHOOK_CONN_ID.upper()}:" in compose

    # 값이 비면 알림이 조용히 죽으니 필수 변수로 둡니다
    assert "${SLACK_WEBHOOK_URL:?" in compose

    # URL 은 password 에 둡니다 — host 는 암호화 대상이 아닙니다
    assert '"password": "${SLACK_WEBHOOK_URL' in compose

    # CD 가 .env 에 써주지 않으면 위 필수 변수 때문에 컨테이너가 아예 안 뜹니다
    assert "SLACK_WEBHOOK_URL: ${{ secrets.SLACK_WEBHOOK_URL }}" in workflow
    assert "echo SLACK_WEBHOOK_URL=" in workflow

    # 웹훅 URL 이 저장소에 박히지 않았는지
    assert "hooks.slack.com/services/T" not in compose + workflow


@pytest.mark.parametrize("blocks", ALL_ALERT_BLOCKS)
def test_Block에서도_여러_줄_사유가_코드_스팬을_깨지_않는다(blocks):
    """텍스트 폴백과 Block Kit 이 같은 REASON_TEXT 를 씁니다. 한쪽만 고치면
    실제로 나가는 Block 쪽이 깨진 채로 남습니다.
    """
    for line in _render_blocks(blocks, exception=MULTILINE_EXCEPTION).splitlines():
        assert line.count("`") % 2 == 0, f"짝 없는 백틱: {line!r}"
