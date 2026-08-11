"""트레잇 + 일별 로그 → 최종 스키마 필드 집계. `implementation_plan.md` §3."""

import uuid

import numpy as np
import pandas as pd

from jobs.driver_master.simulate import simulate_driver
from jobs.driver_master.traits import DISTANCE_LABELS, TIME_BLOCK_LABELS, WEEKDAY_LABELS

TOP_SHARE_THRESHOLD = 0.20  # 점유율 이 이상인 카테고리를 "주요"로 채택
MIN_CHURN_TENURE_DAYS = 30  # 이보다 짧게 다닌 기사는 이탈 처리하지 않음


def _top_categories(counts: np.ndarray, labels: list[str]) -> list[str]:
    total = counts.sum()
    if total == 0:
        return [labels[0]]
    shares = counts / total
    top = [label for label, share in zip(labels, shares) if share >= TOP_SHARE_THRESHOLD]
    return top or [labels[int(np.argmax(shares))]]


def _resolve_churn_at(joined_at: np.datetime64, today: np.datetime64, churn_flag: bool,
                       rng: np.random.Generator) -> np.datetime64 | None:
    if not churn_flag:
        return None
    tenure_days = int((today - joined_at) / np.timedelta64(1, "D"))
    if tenure_days < MIN_CHURN_TENURE_DAYS:
        return None
    offset = int(rng.integers(MIN_CHURN_TENURE_DAYS, tenure_days + 1))
    return joined_at + np.timedelta64(offset, "D")


def aggregate_driver(trait_row: pd.Series, today: np.datetime64, rng: np.random.Generator) -> dict:
    churn_at = _resolve_churn_at(trait_row["joined_at"], today, trait_row["churn_flag"], rng)
    log = simulate_driver(trait_row, today, churn_at, rng)

    if len(log.work_minutes) == 0:
        work_min = work_max = rest_min = rest_max = idle_min = idle_max = trip_min = trip_max = 0
    else:
        work_min, work_max = float(log.work_minutes.min()), float(log.work_minutes.max())
        rest_min, rest_max = float(log.rest_minutes.min()), float(log.rest_minutes.max())
        idle_min, idle_max = float(log.idle_seconds.min()), float(log.idle_seconds.max())
        trip_min, trip_max = int(log.trip_count.min()), int(log.trip_count.max())

    active_weekdays_labels = [WEEKDAY_LABELS[i] for i in trait_row["active_weekdays"]]

    return {
        "driver_id": str(uuid.uuid4()),
        "driver_name": trait_row["driver_name"],
        "primary_distance_bands": _top_categories(log.distance_bucket_counts, DISTANCE_LABELS),
        "primary_time_blocks": _top_categories(log.time_block_counts, TIME_BLOCK_LABELS),
        "active_weekdays": active_weekdays_labels,
        "max_idle_seconds": idle_max,
        "min_idle_seconds": idle_min,
        "max_trip_count": trip_max,
        "min_trip_count": trip_min,
        "min_work_minutes": work_min,
        "max_work_minutes": work_max,
        "max_rest_minutes": rest_max,
        "min_rest_minutes": rest_min,
        "churned_at": None if churn_at is None else str(churn_at),
        "joined_at": str(trait_row["joined_at"]),
    }


def build_driver_master_table(traits_df: pd.DataFrame, today: np.datetime64, seed: int | None = None) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    rows = [aggregate_driver(row, today, rng) for _, row in traits_df.iterrows()]
    return pd.DataFrame(rows)
