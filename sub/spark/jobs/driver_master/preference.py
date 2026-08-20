"""운행 배정 전에 고정하는 기사 선호 마스터."""

import hashlib
from pathlib import Path

import numpy as np
import pandas as pd

from sub.spark.jobs.driver_master.traits import (
    DISTANCE_LABELS,
    DISTANCE_MEDIUM_MAX_MI,
    DISTANCE_SHORT_MAX_MI,
    TIME_BLOCK_LABELS,
    WEEKDAY_LABELS,
    sample_driver_traits,
)

PREFERENCE_COLUMNS = [
    "driver_id",
    "active_weekdays",
    "preferred_time_blocks",
    "time_block_weights",
    "preferred_distance_band",
    "preferred_distance_miles",
    "airport_preference",
    "manhattan_preference",
    "tier_preference",
    # 참고용 근사치입니다 — candidates.py/allocator.py는 더 이상 이 값을 하루 상한으로
    # 읽지 않습니다(#642). 실제 상한은 target_drive_minutes(분 예산)입니다.
    "target_daily_trips",
    "min_daily_trips",
    "max_daily_trips",
    "target_work_minutes",
    "target_drive_minutes",
    "max_deadhead_minutes",
    "buffer_seconds",
]
# 선호 시간대는 **연속된 블록 구간**으로 잡습니다. 예전에는 가중치 상위 2개를 그냥
# 뽑았는데, 그러면 09-12 와 21-24 처럼 떨어진 두 블록이 나오는 기사가 70% 였습니다.
# 배정은 첫 승차부터 하차까지의 경과를 `target_work_minutes`(중앙 405분) 로 재기
# 때문에(allocator.py) 뒤쪽 블록의 운행은 도달할 수 없어 통째로 버려집니다.
#
# 3개인 이유: 창이 9시간이면 실사용 시간이 `target_work_minutes` 에 걸려 405분이
# 되는데, 이게 `target_daily_trips` 를 만들 때 쓰는 값과 같아집니다(아래 참고).
# 4개로 늘려도 그 상한이 먼저 걸려 목표 달성 가능 기사는 36% -> 39% 로만 움직입니다.
PREFERRED_BLOCK_RUN = 3
# 범위 근거는 synthetic-driver-mapping-guide.md — buffer 기본 60초(§6 조건 2),
# 하루 trip 은 4개 미만이면 묶음 폐기·35개 상한(§9). 기사마다 그 안에서 다르게 뽑습니다.
BUFFER_SECONDS_RANGE = (60, 181)
MIN_DAILY_TRIPS_RANGE = (4, 9)
MAX_DAILY_TRIPS_RANGE = (15, 36)


def _driver_seed(seed: int, driver_id: str) -> int:
    digest = hashlib.sha256(f"{seed}:{driver_id}".encode()).digest()
    return int.from_bytes(digest[:8], "big")


def _distance_band(miles: float) -> str:
    if miles <= DISTANCE_SHORT_MAX_MI:
        return DISTANCE_LABELS[0]
    if miles <= DISTANCE_MEDIUM_MAX_MI:
        return DISTANCE_LABELS[1]
    return DISTANCE_LABELS[2]


def _preferred_block_indexes(time_weights: np.ndarray) -> list[int]:
    """가중치 합이 가장 큰 연속 `PREFERRED_BLOCK_RUN` 블록의 인덱스.

    하루 경계를 넘지 않습니다. 21-24 와 다음날 00-03 은 시계로는 붙어 있지만
    배정이 서비스일 단위로 묶여(allocator) 같은 그룹에 오지 않기 때문입니다.
    """
    starts = range(len(time_weights) - PREFERRED_BLOCK_RUN + 1)
    best = max(starts, key=lambda start: time_weights[start:start + PREFERRED_BLOCK_RUN].sum())
    return list(range(best, best + PREFERRED_BLOCK_RUN))


def _validate_driver_ids(driver_ids: list[str]) -> list[str]:
    normalized = [str(driver_id).strip() for driver_id in driver_ids]
    if not normalized or any(not driver_id for driver_id in normalized):
        raise ValueError("driver_id는 비어 있지 않아야 합니다")
    if len(normalized) != len(set(normalized)):
        raise ValueError("driver_id는 중복될 수 없습니다")
    return sorted(normalized)


def build_driver_preferences(
    driver_ids: list[str],
    bootstrap_pools: dict[str, np.ndarray],
    *,
    as_of_date: np.datetime64,
    seed: int,
) -> pd.DataFrame:
    """기사별 안정적인 seed로 배정용 선호를 한 행씩 생성합니다."""
    rows: list[dict] = []
    for driver_id in _validate_driver_ids(driver_ids):
        driver_seed = _driver_seed(seed, driver_id)
        trait = sample_driver_traits(
            1, bootstrap_pools, today=as_of_date, seed=driver_seed
        ).iloc[0]
        rng = np.random.default_rng(driver_seed)
        time_weights = np.asarray(trait["time_pref"], dtype=float)
        preferred_indexes = _preferred_block_indexes(time_weights)
        distance_miles = float(trait["distance_pref_mi"])
        work_minutes = int(round(float(trait["work_mean_h"]) * 60))
        trip_minutes = max(float(trait["avg_trip_duration_min"]), 1.0)
        target_work_minutes = max(60, min(work_minutes, 12 * 60))
        # 근무시간 중 실제로 승객을 태우는 비중 (D7과 같은 idle_frac 기준).
        # `sub/generators/synthetic_driver_state/traits.py`는 반대 방향으로 계산합니다
        # (target_drive_minutes가 1차, target_work_minutes = 그걸 idle_frac로 나눈 값)
        # — 여기는 target_work_minutes가 이미 1차 산출값이라 같은 관계를 거꾸로 씁니다.
        target_drive_minutes = int(round(target_work_minutes * (1.0 - float(trait["idle_frac"]))))

        min_daily_trips = int(rng.integers(*MIN_DAILY_TRIPS_RANGE))
        max_daily_trips = max(min_daily_trips, int(rng.integers(*MAX_DAILY_TRIPS_RANGE)))
        # 근무시간에서 나온 목표치가 기사의 하한·상한 밖이면 범위 안으로 당깁니다.
        target_daily_trips = min(
            max(int(round(work_minutes / trip_minutes)), min_daily_trips), max_daily_trips
        )

        rows.append({
            "driver_id": driver_id,
            "active_weekdays": [WEEKDAY_LABELS[index] for index in trait["active_weekdays"]],
            "preferred_time_blocks": [TIME_BLOCK_LABELS[index] for index in preferred_indexes],
            "time_block_weights": time_weights.tolist(),
            "preferred_distance_band": _distance_band(distance_miles),
            "preferred_distance_miles": distance_miles,
            "airport_preference": float(rng.beta(2.0, 5.0)),
            "manhattan_preference": float(rng.beta(2.5, 2.5)),
            # 자격이 될 때 프리미엄(Uber Comfort / Lyft Extra Comfort) 콜을 얼마나
            # 우선하는가. beta(5,2) 는 평균 0.71 로 프리미엄 쪽에 치우칩니다 —
            # 비싼 프리미엄 차를 렌트한 기사가 그 콜을 우선한다는 가정입니다.
            #
            # ★ 실측이 아니라 **가정**입니다. 합성 기사 행동 모델의 손잡이라 데이터에
            #   정답이 없습니다. 이 값으로 나온 프리미엄 비중을 Gold 의 등급 전환율로
            #   되먹이면 순환이 되니 쓰지 마세요.
            #   없앤 것이 아니라 드러낸 것입니다 — 이 컬럼이 없던 동안에도 배정은
            #   등급을 동등하게 봤고, 그건 0.5 를 박아둔 것과 같았습니다.
            "tier_preference": float(rng.beta(5.0, 2.0)),
            "target_daily_trips": target_daily_trips,
            "min_daily_trips": min_daily_trips,
            "max_daily_trips": max_daily_trips,
            "target_work_minutes": target_work_minutes,
            "target_drive_minutes": target_drive_minutes,
            # 5~15분(중앙 10분)이던 값입니다. 구역쌍 이동시간(taxi_zone_travel_times,
            # 50,633쌍)의 중앙값이 33.4분이라 중앙 기사가 하차 후 이어갈 수 있는
            # 구역쌍이 2.8% 뿐이었습니다. 배정 결과에서 0분 초과 공차의 최대값이
            # 정확히 15.0분 — 상한이 그대로 천장이 되어 있었습니다.
            "max_deadhead_minutes": int(rng.integers(10, 26)),
            "buffer_seconds": int(rng.integers(*BUFFER_SECONDS_RANGE)),
        })
    return pd.DataFrame(rows, columns=PREFERENCE_COLUMNS)


def extend_driver_preferences(
    previous: pd.DataFrame,
    driver_ids: list[str],
    bootstrap_pools: dict[str, np.ndarray],
    *,
    as_of_date: np.datetime64,
    seed: int,
) -> pd.DataFrame:
    """기존 선호는 보존하고 처음 보는 기사만 같은 계약으로 추가합니다."""
    missing = set(PREFERENCE_COLUMNS) - set(previous.columns)
    if missing:
        raise ValueError(f"기존 기사 선호 컬럼 누락: {sorted(missing)}")
    if previous["driver_id"].isna().any() or previous["driver_id"].duplicated().any():
        raise ValueError("기존 기사 선호의 driver_id는 null 없이 고유해야 합니다")

    requested = _validate_driver_ids(driver_ids)
    previous_ids = set(previous["driver_id"].astype(str))
    new_ids = [driver_id for driver_id in requested if driver_id not in previous_ids]
    added = build_driver_preferences(
        new_ids, bootstrap_pools, as_of_date=as_of_date, seed=seed
    ) if new_ids else pd.DataFrame(columns=PREFERENCE_COLUMNS)
    return pd.concat([previous[PREFERENCE_COLUMNS].copy(), added], ignore_index=True).sort_values(
        "driver_id"
    ).reset_index(drop=True)


def write_driver_preferences(preferences: pd.DataFrame, output_path: str | Path) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    preferences[PREFERENCE_COLUMNS].to_parquet(path, index=False)
    return path
