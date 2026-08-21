"""D15 append-only 이벤트 원장과 이를 접은 현재 상태.

`fold_events` 가 재생 함수입니다 — 이벤트 전체만으로 항상 같은
`driver_vehicle_current` 가 나와야 하고, 전월 상태 캐시는 성능 최적화일 뿐
구조적 의존이 아닙니다 (blue_print.md 4.2).

`sub/prototype/synthesize.py::fold_events`/`_next_driver_ids` 를 그대로 옮겼습니다.
"""

from __future__ import annotations

import pandas as pd

EVENT_JOIN = "join"
EVENT_EXIT = "exit"
EVENT_VEHICLE_CHANGE = "vehicle_change"
EVENT_TYPES = (EVENT_JOIN, EVENT_EXIT, EVENT_VEHICLE_CHANGE)

CURRENT_COLUMNS = [
    "driver_id", "taxi_id", "traits_pool_month",
    "joined_on", "exited_on", "vehicle_since",
]


def fold_events(events: pd.DataFrame) -> pd.DataFrame:
    """append only 원장을 접어 `driver_vehicle_current` 를 만듭니다 (4.2).

    이 함수가 재생 함수입니다. 전월 상태 캐시 없이 이벤트 전체만으로 같은 결과가
    나와야 하고, 그래서 전월 상태가 성능 최적화일 뿐 구조적 의존이 아닙니다.

    D15: 유출 기사의 행을 삭제하지 않습니다. `exited_on` 만 채웁니다.
    """
    if events.empty:
        return pd.DataFrame(columns=CURRENT_COLUMNS)
    ordered = events.sort_values(["driver_id", "event_ts", "event_type"], kind="stable")
    state: dict[str, dict] = {}
    for event in ordered.to_dict("records"):
        driver_id = event["driver_id"]
        kind = event["event_type"]
        if kind == EVENT_JOIN:
            state[driver_id] = {
                "driver_id": driver_id,
                "taxi_id": event["taxi_id"],
                "traits_pool_month": event["traits_pool_month"],
                "joined_on": event["event_ts"],
                "exited_on": None,
                "vehicle_since": event["event_ts"],
            }
        elif kind == EVENT_EXIT:
            if driver_id in state:
                state[driver_id]["exited_on"] = event["event_ts"]
        elif kind == EVENT_VEHICLE_CHANGE:
            if driver_id in state:
                state[driver_id]["taxi_id"] = event["taxi_id"]
                state[driver_id]["vehicle_since"] = event["event_ts"]
        else:
            raise ValueError(f"알 수 없는 이벤트 종류: {kind}")
    return pd.DataFrame(list(state.values())).sort_values("driver_id").reset_index(drop=True)


def next_driver_ids(existing: set[str], target_month: str, count: int) -> list[str]:
    """신규 기사 ID. 월을 담아서 어느 달에 유입됐는지 ID 로 읽힙니다."""
    stamp = target_month.replace("-", "")
    ids: list[str] = []
    serial = 1
    while len(ids) < count:
        candidate = f"DRIVER_{stamp}_{serial:06d}"
        serial += 1
        if candidate not in existing:
            ids.append(candidate)
            existing.add(candidate)
    return ids
