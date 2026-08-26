"""Gold 지역별 freshness watchdog 시나리오.

1. Gold 검증 성공 → 지역·월 복합 파티션과 UTC 성공 시각 저장
2. 성공 이력 없음 → 감시 기준점만 만들고 즉시 알리지 않음
3. 마지막 성공이 SLA 초과 → 저장된 파티션으로 Slack 콜백 호출
4. 마지막 성공이 SLA 이내 → 알리지 않음
5. NYC 성공과 TX 감시 상태 분리 → 한 지역 성공이 다른 지역 지연을 가리지 않음
6. 과거 월 백필 성공 → 최신 완료 파티션과 freshness 시각 유지
7. 같은 월 재성공 → freshness 시각 갱신
8. 더 최신 월 성공 → 최신 완료 파티션과 freshness 시각 갱신

실제 Slack 네트워크는 호출하지 않고 콜백 경계와 전달 컨텍스트를 검증합니다.
"""

from datetime import datetime, timedelta, timezone

from main.airflow.common import gold_staleness
from main.airflow.scripts.monthly_taxi_trip_silver_to_gold import tasks as gold_tasks
from main.airflow.scripts.source_api_refresh import tasks as refresh_tasks


NOW = datetime(2026, 8, 24, 3, 0, tzinfo=timezone.utc)


def _variable_store(monkeypatch) -> dict:
    store = {}

    def get(key, default=None, deserialize_json=False):
        return store.get(key, default)

    def set_(key, value, serialize_json=False):
        store[key] = value

    monkeypatch.setattr(gold_staleness.Variable, "get", get)
    monkeypatch.setattr(gold_staleness.Variable, "set", set_)
    return store


def _run_watchdog(service_area: str, stale_days: int = 31) -> None:
    refresh_tasks.check_gold_staleness_task.function(
        params={
            "service_area": service_area,
            "gold_stale_sla_days": stale_days,
        }
    )


def test_Gold검증성공시_지역월과_UTC성공시각을_기록한다(monkeypatch):
    store = _variable_store(monkeypatch)
    monkeypatch.setattr(gold_tasks, "validate_gold_outputs", lambda *args: None)
    task_instance = type(
        "TaskInstance",
        (),
        {
            "xcom_pull": lambda self, task_ids: {
                "service_area": "NYC",
                "year_month": "2026-08",
            }
        },
    )()
    before = datetime.now(timezone.utc)

    gold_tasks.validate_gold_task.function(
        params={"output_dir": "/gold"}, task_instance=task_instance
    )

    state = store[gold_staleness.state_key("NYC")]
    completed_at = datetime.fromisoformat(state["last_success_at"])
    assert state["partition_key"] == "NYC:2026-08"
    assert before <= completed_at <= datetime.now(timezone.utc)


def test_과거월_백필은_최신_Gold_freshness를_갱신하지_않는다(monkeypatch):
    _variable_store(monkeypatch)
    latest_completed_at = NOW - timedelta(days=20)
    gold_staleness.record_success("NYC", "2026-08", latest_completed_at)

    state = gold_staleness.record_success("NYC", "2024-01", NOW)

    assert state["partition_key"] == "NYC:2026-08"
    assert state["last_success_at"] == latest_completed_at.isoformat()


def test_같은월_재성공은_freshness시각을_갱신한다(monkeypatch):
    _variable_store(monkeypatch)
    gold_staleness.record_success("NYC", "2026-08", NOW - timedelta(days=1))

    state = gold_staleness.record_success("NYC", "2026-08", NOW)

    assert state["partition_key"] == "NYC:2026-08"
    assert state["last_success_at"] == NOW.isoformat()


def test_더_최신월_성공은_Gold_freshness를_갱신한다(monkeypatch):
    _variable_store(monkeypatch)
    gold_staleness.record_success("NYC", "2026-07", NOW - timedelta(days=20))

    state = gold_staleness.record_success("NYC", "2026-08", NOW)

    assert state["partition_key"] == "NYC:2026-08"
    assert state["last_success_at"] == NOW.isoformat()


def test_성공이력이_없으면_기준점만_만들고_즉시_알리지_않는다(monkeypatch):
    store = _variable_store(monkeypatch)
    calls = []
    monkeypatch.setattr(refresh_tasks, "slack_stale_alert_callback", calls.append)

    _run_watchdog("NYC", stale_days=10)

    state = store[gold_staleness.state_key("NYC")]
    assert state["last_success_at"] is None
    assert state["watch_started_at"]
    assert calls == []


def test_SLA초과시_저장된_파티션으로_Slack알림을_보낸다(monkeypatch):
    _variable_store(monkeypatch)
    calls = []
    monkeypatch.setattr(refresh_tasks, "slack_stale_alert_callback", calls.append)
    gold_staleness.record_success("NYC", "2026-07", NOW - timedelta(days=40))
    monkeypatch.setattr(
        refresh_tasks,
        "datetime",
        type("FixedDateTime", (), {"now": staticmethod(lambda tz=None: NOW)}),
    )

    _run_watchdog("NYC", stale_days=10)

    assert len(calls) == 1
    assert calls[0]["partition_key"] == "NYC:2026-07"
    assert calls[0]["days_since_success"] == 40
    assert calls[0]["stale_days"] == 10


def test_SLA이내면_알리지_않는다(monkeypatch):
    _variable_store(monkeypatch)
    calls = []
    monkeypatch.setattr(refresh_tasks, "slack_stale_alert_callback", calls.append)
    gold_staleness.record_success("NYC", "2026-08", NOW - timedelta(days=1))
    monkeypatch.setattr(
        refresh_tasks,
        "datetime",
        type("FixedDateTime", (), {"now": staticmethod(lambda tz=None: NOW)}),
    )

    _run_watchdog("NYC", stale_days=31)

    assert calls == []


def test_NYC성공이_TX지연을_가리지_않는다(monkeypatch):
    _variable_store(monkeypatch)
    calls = []
    monkeypatch.setattr(refresh_tasks, "slack_stale_alert_callback", calls.append)
    gold_staleness.save_state(
        "TX",
        {
            "service_area": "TX",
            "partition_key": None,
            "watch_started_at": (NOW - timedelta(days=40)).isoformat(),
            "last_success_at": None,
        },
    )
    gold_staleness.record_success("NYC", "2026-08", NOW)
    monkeypatch.setattr(
        refresh_tasks,
        "datetime",
        type("FixedDateTime", (), {"now": staticmethod(lambda tz=None: NOW)}),
    )

    _run_watchdog("TX", stale_days=10)

    assert len(calls) == 1
    assert calls[0]["partition_key"] is None
    assert calls[0]["days_since_success"] == 40


def test_SLA기준일은_Param_Variable_기본값_순이다(monkeypatch):
    monkeypatch.setattr(
        gold_staleness.Variable,
        "get",
        lambda key, default=None: 45,
    )

    assert gold_staleness.resolve_stale_sla_days({"gold_stale_sla_days": 10}) == 10
    assert gold_staleness.resolve_stale_sla_days({"gold_stale_sla_days": None}) == 45

    monkeypatch.setattr(
        gold_staleness.Variable,
        "get",
        lambda key, default=None: default,
    )
    assert gold_staleness.resolve_stale_sla_days({}) == 31
