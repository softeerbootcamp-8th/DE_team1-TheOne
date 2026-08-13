"""운행 배정 전에 고정하는 기사 선호 마스터."""

import hashlib
from pathlib import Path

import numpy as np
import pandas as pd

from jobs.driver_master.traits import (
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
    "target_daily_trips",
    "min_daily_trips",
    "max_daily_trips",
    "target_work_minutes",
    "max_deadhead_minutes",
    "buffer_seconds",
]
TOP_TIME_BLOCK_COUNT = 2
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
    seed: int = 42,
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
        preferred_indexes = np.argsort(time_weights)[-TOP_TIME_BLOCK_COUNT:][::-1]
        distance_miles = float(trait["distance_pref_mi"])
        work_minutes = int(round(float(trait["work_mean_h"]) * 60))
        trip_minutes = max(float(trait["avg_trip_duration_min"]), 1.0)

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
            "target_daily_trips": target_daily_trips,
            "min_daily_trips": min_daily_trips,
            "max_daily_trips": max_daily_trips,
            "target_work_minutes": max(60, min(work_minutes, 12 * 60)),
            "max_deadhead_minutes": int(rng.integers(5, 16)),
            "buffer_seconds": int(rng.integers(*BUFFER_SECONDS_RANGE)),
        })
    return pd.DataFrame(rows, columns=PREFERENCE_COLUMNS)


def extend_driver_preferences(
    previous: pd.DataFrame,
    driver_ids: list[str],
    bootstrap_pools: dict[str, np.ndarray],
    *,
    as_of_date: np.datetime64,
    seed: int = 42,
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
