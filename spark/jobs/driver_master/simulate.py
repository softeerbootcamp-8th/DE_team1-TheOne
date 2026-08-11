"""기사 1명의 일별 로그 시뮬레이션. `implementation_plan.md` §2.

트레잇(§1)을 받아 근무일마다 근무시간/휴식/공차/트립수를 생성합니다. 거리 버킷과
시간대는 트립마다 따로 뽑지 않고, 같은 기사는 매일 같은 확률(거리는 `distance_pref_mi`,
시간대는 `time_pref`로 고정)이라는 점을 이용해 **날짜 전체의 트립수 합으로 한 번에
멀티노미얼을 뽑습니다** — 트립 단위로 반복 추출하는 것과 통계적으로 동일하면서 훨씬
빠릅니다.
"""

from dataclasses import dataclass

import numpy as np
from scipy.stats import lognorm

from jobs.driver_master.traits import DISTANCE_MEDIUM_MAX_MI, DISTANCE_SHORT_MAX_MI

TRIP_DISTANCE_SIGMA = 0.35  # 개인 평균거리 대비 트립별 변동성 (로그정규 sigma)
MAX_SIMULATION_DAYS = 90


@dataclass
class DriverDayLog:
    dates: np.ndarray  # datetime64[D]
    work_minutes: np.ndarray
    rest_minutes: np.ndarray
    idle_seconds: np.ndarray
    trip_count: np.ndarray
    distance_bucket_counts: np.ndarray  # shape (3,) SHORT/MEDIUM/LONG
    time_block_counts: np.ndarray  # shape (8,)


def _distance_bucket_probs(distance_pref_mi: float) -> np.ndarray:
    mu = np.log(distance_pref_mi)
    p_short = lognorm.cdf(DISTANCE_SHORT_MAX_MI, s=TRIP_DISTANCE_SIGMA, scale=np.exp(mu))
    p_medium_cum = lognorm.cdf(DISTANCE_MEDIUM_MAX_MI, s=TRIP_DISTANCE_SIGMA, scale=np.exp(mu))
    p_medium = p_medium_cum - p_short
    p_long = 1.0 - p_medium_cum
    return np.array([p_short, p_medium, p_long]).clip(min=0.0)


def _active_dates(joined_at: np.datetime64, churn_at: np.datetime64 | None, today: np.datetime64,
                   active_weekdays: list[int]) -> np.ndarray:
    window_end = today if churn_at is None else min(churn_at, today)
    window_end = min(window_end, joined_at + np.timedelta64(MAX_SIMULATION_DAYS, "D"))
    if window_end <= joined_at:
        return np.array([], dtype="datetime64[D]")

    all_days = np.arange(joined_at, window_end, dtype="datetime64[D]")
    # datetime64[D]의 weekday: 1970-01-01(목)을 기준으로 계산. 파이썬 weekday()(월=0)와
    # 맞추려면 +3 오프셋.
    weekdays = (all_days.astype("int64") + 3) % 7
    mask = np.isin(weekdays, active_weekdays)
    return all_days[mask]


def simulate_driver(
    trait_row,
    today: np.datetime64,
    churn_at: np.datetime64 | None,
    rng: np.random.Generator,
) -> DriverDayLog:
    dates = _active_dates(trait_row["joined_at"], churn_at, today, trait_row["active_weekdays"])
    n_days = len(dates)

    if n_days == 0:
        empty = np.array([])
        return DriverDayLog(
            dates=dates, work_minutes=empty, rest_minutes=empty, idle_seconds=empty,
            trip_count=empty, distance_bucket_counts=np.zeros(3), time_block_counts=np.zeros(8),
        )

    work_cv = trait_row["work_cv"]
    work_mean_min = trait_row["work_mean_h"] * 60.0
    work_minutes = rng.gamma(shape=1.0 / work_cv**2, scale=work_mean_min * work_cv**2, size=n_days)

    rest_ratio = np.clip(trait_row["rest_frac"] + rng.normal(0, 0.02, size=n_days), 0.02, 0.25)
    rest_minutes = work_minutes * rest_ratio
    remaining_minutes = work_minutes - rest_minutes

    idle_ratio = np.clip(trait_row["idle_frac"] + rng.normal(0, 0.05, size=n_days), 0.05, 0.5)
    idle_seconds = remaining_minutes * 60.0 * idle_ratio

    trip_minutes = remaining_minutes - idle_seconds / 60.0
    trip_count = np.round(np.clip(trip_minutes, 0, None) / trait_row["avg_trip_duration_min"])
    trip_count = trip_count.astype(int)

    total_trips = int(trip_count.sum())
    distance_probs = _distance_bucket_probs(trait_row["distance_pref_mi"])
    distance_bucket_counts = (
        rng.multinomial(total_trips, distance_probs) if total_trips > 0 else np.zeros(3, dtype=int)
    )
    time_block_counts = (
        rng.multinomial(total_trips, trait_row["time_pref"]) if total_trips > 0 else np.zeros(8, dtype=int)
    )

    return DriverDayLog(
        dates=dates,
        work_minutes=work_minutes,
        rest_minutes=rest_minutes,
        idle_seconds=idle_seconds,
        trip_count=trip_count,
        distance_bucket_counts=distance_bucket_counts,
        time_block_counts=time_block_counts,
    )
