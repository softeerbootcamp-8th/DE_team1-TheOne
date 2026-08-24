"""Gold 데이터 스트림의 지역별 성공 상태와 freshness SLA 판정."""

import logging
from datetime import datetime, timezone

from airflow.sdk import Variable

from main.airflow.common.assets import build_partition_key


logger = logging.getLogger(__name__)

DEFAULT_STALE_SLA_DAYS = 31
STALE_SLA_DAYS_VARIABLE = "gold_stale_sla_days"
STATE_KEY_PREFIX = "gold_staleness_state__"


def state_key(service_area: str) -> str:
    """한 지역의 최신 Gold 성공 상태를 저장할 Variable 키."""
    return f"{STATE_KEY_PREFIX}{service_area}"


def resolve_stale_sla_days(params: dict) -> int:
    """Param, Variable, 기본값 순으로 freshness SLA 기준일을 정합니다."""
    configured = params.get("gold_stale_sla_days")
    if configured is not None:
        return int(configured)
    try:
        return int(
            Variable.get(STALE_SLA_DAYS_VARIABLE, default=DEFAULT_STALE_SLA_DAYS)
        )
    except Exception:
        logger.warning(
            "Variable(%s) 조회에 실패해 기본값 %s일을 씁니다",
            STALE_SLA_DAYS_VARIABLE,
            DEFAULT_STALE_SLA_DAYS,
            exc_info=True,
        )
        return DEFAULT_STALE_SLA_DAYS


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def load_state(service_area: str) -> dict | None:
    """지역별 watchdog 상태를 읽습니다."""
    return Variable.get(
        state_key(service_area),
        default=None,
        deserialize_json=True,
    )


def save_state(service_area: str, state: dict) -> None:
    Variable.set(state_key(service_area), state, serialize_json=True)


def record_success(
    service_area: str,
    year_month: str,
    completed_at: datetime,
) -> dict:
    """Gold 검증 성공 뒤 해당 지역의 최신 성공 파티션과 시각을 기록합니다."""
    completed_at = _utc(completed_at)
    previous = load_state(service_area) or {}
    state = {
        "service_area": service_area,
        "partition_key": build_partition_key(service_area, year_month),
        "watch_started_at": previous.get(
            "watch_started_at", completed_at.isoformat()
        ),
        "last_success_at": completed_at.isoformat(),
    }
    save_state(service_area, state)
    return state


def evaluate_staleness(
    service_area: str,
    now: datetime,
) -> tuple[int | None, dict]:
    """지역별 최신 성공(없으면 최초 감시 시작) 이후 경과일을 계산합니다.

    상태가 전혀 없으면 오늘부터 감시를 시작하고 첫 실행에서는 경고하지 않습니다.
    이 기준점이 있어야 Gold Asset 이벤트가 한 번도 오지 않는 경우도 N일 뒤 감지됩니다.
    """
    now = _utc(now)
    state = load_state(service_area)
    if state is None:
        state = {
            "service_area": service_area,
            "partition_key": None,
            "watch_started_at": now.isoformat(),
            "last_success_at": None,
        }
        save_state(service_area, state)
        return None, state

    reference_text = state.get("last_success_at") or state.get("watch_started_at")
    if not reference_text:
        raise ValueError(
            f"Gold staleness 상태에 기준 시각이 없습니다: service_area={service_area}"
        )
    reference = _utc(datetime.fromisoformat(reference_text))
    return (now - reference).days, state
