"""D14 · D15 join/exit/vehicle_change lifecycle과 월별 상태 합성.

`synthesize_month` 이 이 패키지의 진입점입니다. 전월 상태가 없으면 초기
스냅샷(SNAPSHOT_INIT)을, 있으면 lifecycle(SNAPSHOT_EVOLVE)을 적용합니다.

`sub/prototype/synthesize.py::synthesize_month` 을 그대로 옮겼고, `fleet`/`traits`/
`assignment`/`events` 모듈로 나눈 부분만 이 파일에서 조립합니다.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from sub.config import GenerationConfig
from sub.generators.synthetic_driver_state import fleet, traits
from sub.generators.synthetic_driver_state.assignment import assign_vehicles
from sub.generators.synthetic_driver_state.events import (
    EVENT_EXIT,
    EVENT_JOIN,
    EVENT_VEHICLE_CHANGE,
    fold_events,
    next_driver_ids,
)
from sub.seeds import Stage, derive_seed


@dataclass(frozen=True)
class SynthesizeResult:
    events: pd.DataFrame            # 이 달에 발생한 이벤트만
    current: pd.DataFrame           # 전체 이벤트를 접은 현재 상태
    profiles: pd.DataFrame          # 배정이 쓰는 기사 선호 (월별 실현값 반영)
    noise_state: pd.DataFrame       # D7 (B) 자기상관 연쇄 — 시드가 아니라 상태
    clip_rate: float                # 클리핑 발생 빈도 (volatility 과다 신호)


def _monthly_count(rng: np.random.Generator, pool_size: int, rate: float) -> int:
    """비율 × 정원의 기대값을 확률적으로 반올림합니다.

    `round()` 로 자르면 `0.007 × 60 = 0.42` 가 0 이 되어 그 이벤트가 **영원히
    발생하지 않습니다.** 설정에 0 이 아닌 값을 넣었는데 결과가 안 바뀌는
    상태이고, D12 가 없애려는 것과 같은 종류의 침묵입니다. 소수부를 확률로
    처리하면 기대값이 보존됩니다.
    """
    expected = pool_size * rate
    whole = int(expected)
    return whole + int(rng.random() < (expected - whole))


def decide_lifecycle(
    active_driver_ids: list[str],
    *,
    global_seed: int,
    target_month: str,
    config: GenerationConfig,
) -> tuple[list[str], list[str], list[str]]:
    """그 달의 join/exit/vehicle_change 대상을 결정적으로 뽑습니다 (D14).

    가입·탈퇴·차량 교체는 서로 **독립**입니다 — 유출 수만큼 유입되는 강제가
    없습니다. `active_driver_ids` 는 신규 기사 ID 채번을 위한 `known_ids` 갱신에
    쓰이지 않습니다 — 그건 호출자가 `next_driver_ids` 로 직접 처리합니다.

    반환: `(join_count로_아직_채번되지_않은_수, exiters, changers)` 가 아니라
    `(exiters, changers, join_count)` — 상세는 반환 타입 참고.
    """
    rng = np.random.default_rng(derive_seed(global_seed, Stage.LIFECYCLE, target_month))
    pool_ids = np.asarray(sorted(active_driver_ids), dtype=object)

    exit_count = _monthly_count(rng, len(pool_ids), config.driver.exit_rate)
    join_count = _monthly_count(rng, len(pool_ids), config.driver.join_rate)
    change_count = _monthly_count(rng, len(pool_ids), config.driver.vehicle_change_rate)

    exiters = list(rng.choice(pool_ids, size=min(exit_count, len(pool_ids)), replace=False))
    remaining = np.asarray([d for d in pool_ids if d not in set(exiters)], dtype=object)
    changers = list(rng.choice(remaining, size=min(change_count, len(remaining)), replace=False))
    return exiters, changers, join_count


def synthesize_month(
    *,
    target_month: str,
    config: GenerationConfig,
    vehicle_master: pd.DataFrame,
    trip_pool: dict[str, np.ndarray],
    previous_current: pd.DataFrame | None,
    previous_events: pd.DataFrame | None,
    previous_noise: pd.DataFrame | None,
) -> SynthesizeResult:
    """그 달의 합성. 전월 상태가 없으면 초기 스냅샷을 만듭니다."""
    global_seed = config.global_seed
    month_ts = pd.Timestamp(f"{target_month}-01")
    stock = fleet.build_fleet_stock(vehicle_master, driver_count=config.driver.initial_count)
    fleet_units = fleet.expand_fleet_units(stock)

    history = (
        previous_events.copy()
        if previous_events is not None and not previous_events.empty
        else pd.DataFrame(columns=["driver_id", "event_type", "event_ts", "taxi_id", "traits_pool_month"])
    )
    known_ids = set(history["driver_id"]) if not history.empty else set()
    new_events: list[dict] = []

    if previous_current is None or previous_current.empty:
        # --- 초기 스냅샷 (SNAPSHOT_INIT, 월을 시드에 넣지 않음) -----------------
        width = len(str(config.driver.initial_count - 1))
        joiners = [f"DRIVER_{i:0{width}d}" for i in range(config.driver.initial_count)]
        known_ids.update(joiners)
        # 가입일은 초기 픽스처의 기준일 하나로 둡니다. 리스 시작일을 과거로 흩는
        # 것은 원천의 현실성이지만, 프로토타입의 측정 대상(매칭률)을 바꾸지
        # 않으면서 상태 연쇄만 복잡하게 만듭니다.
        joined_on = month_ts
        exiters: list[str] = []
        changers: list[str] = []
    else:
        # --- lifecycle (SNAPSHOT_EVOLVE, 월을 시드에 넣음) ---------------------
        active = previous_current[previous_current["exited_on"].isna()]
        if active.empty:
            raise ValueError(
                f"{target_month}: 전월 활성 기사가 0명입니다. exit_rate 를 확인하세요."
            )
        exiters, changers, join_count = decide_lifecycle(
            list(active["driver_id"]),
            global_seed=global_seed, target_month=target_month, config=config,
        )
        joiners = next_driver_ids(known_ids, target_month, join_count)
        joined_on = month_ts

    # D14: 총원 하한만 검증합니다. 상한을 두면 몇 달 뒤 조용히 실패합니다.
    surviving = len(known_ids) - len(set(history[history["event_type"] == EVENT_EXIT]["driver_id"]) if not history.empty else set())
    if surviving < 1:
        raise ValueError(f"{target_month}: 활성 기사가 1명 미만입니다")

    # 신규 기사의 성향 — 가입 시점 월이 traits_pool_month 입니다 (D8).
    traits_rows = [
        traits.base_traits(
            driver_id,
            global_seed=global_seed,
            traits_pool_month=target_month,
            trip_pool=trip_pool,
        )
        for driver_id in joiners
    ]
    # 기존 기사의 성향은 각자의 traits_pool_month 로 **재계산**합니다. 이전 parquet
    # 복사(승계)가 아닙니다 — 그것이 D8 이 없애려는 우연한 안정성입니다.
    if previous_current is not None and not previous_current.empty:
        for record in previous_current[previous_current["exited_on"].isna()].to_dict("records"):
            if record["driver_id"] in set(exiters):
                continue
            traits_rows.append(
                traits.base_traits(
                    record["driver_id"],
                    global_seed=global_seed,
                    traits_pool_month=str(record["traits_pool_month"]),
                    trip_pool=trip_pool,
                )
            )
    traits_df = pd.DataFrame(traits_rows).drop_duplicates("driver_id").reset_index(drop=True)

    # 차량 배정 — 이미 차를 가진 기사의 차량은 재고에서 빼고 시작합니다.
    held = set()
    if previous_current is not None and not previous_current.empty:
        held = set(
            previous_current.loc[previous_current["exited_on"].isna(), "taxi_id"].astype(str)
        )
        held -= {
            str(previous_current.loc[previous_current["driver_id"] == d, "taxi_id"].iloc[0])
            for d in exiters
        }
        held -= {
            str(previous_current.loc[previous_current["driver_id"] == d, "taxi_id"].iloc[0])
            for d in changers
        }
    available = fleet_units[~fleet_units["taxi_id"].astype(str).isin(held)]
    needs_vehicle = traits_df[traits_df["driver_id"].isin(set(joiners) | set(changers))]
    assignment = assign_vehicles(
        needs_vehicle,
        available,
        global_seed=global_seed,
        target_month=target_month,
        rationality=config.synthesize.rationality,
    )
    for driver_id in joiners:
        taxi_id = assignment.get(driver_id)
        if taxi_id is None:
            # 재고 소진. 조용히 넘기지 않습니다 — 유입이 재고를 넘으면 그건 설정
            # 문제이고, 넘어가면 명부에만 있고 차가 없는 기사가 생깁니다.
            raise ValueError(
                f"{target_month}: 신규 기사 {driver_id} 에게 배정할 차량 재고가 없습니다. "
                f"fleet.FLEET_OVERSUPPLY({fleet.FLEET_OVERSUPPLY}) 또는 driver.join_rate 를 확인하세요."
            )
        new_events.append({
            "driver_id": driver_id, "event_type": EVENT_JOIN, "event_ts": joined_on,
            "taxi_id": taxi_id, "traits_pool_month": target_month,
        })
    for driver_id in exiters:
        new_events.append({
            "driver_id": driver_id, "event_type": EVENT_EXIT, "event_ts": month_ts,
            "taxi_id": None, "traits_pool_month": None,
        })
    for driver_id in changers:
        taxi_id = assignment.get(driver_id)
        if taxi_id is None:
            continue  # 교체할 재고가 없으면 그 달은 그대로 탑니다
        new_events.append({
            "driver_id": driver_id, "event_type": EVENT_VEHICLE_CHANGE, "event_ts": month_ts,
            "taxi_id": taxi_id, "traits_pool_month": None,
        })

    events_df = pd.DataFrame(new_events)
    all_events = pd.concat([history, events_df], ignore_index=True) if not events_df.empty else history
    current = fold_events(all_events)

    realized, noise_state, clip_rate = traits.realize_month(
        traits_df, previous_noise,
        global_seed=global_seed, target_month=target_month, config=config,
    )
    active_current = current[current["exited_on"].isna()][["driver_id", "taxi_id"]]
    profiles = realized.merge(active_current, on="driver_id", how="inner")
    if profiles.empty:
        raise ValueError(f"{target_month}: 활성 기사에 대응하는 선호가 없습니다")
    return SynthesizeResult(
        events=events_df, current=current, profiles=profiles,
        noise_state=noise_state, clip_rate=clip_rate,
    )
