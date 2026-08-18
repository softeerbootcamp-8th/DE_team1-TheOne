"""전월 상태에서 당월 기사·차량·리스와 기사 선호를 결정적으로 갱신합니다."""

from __future__ import annotations

import shutil
import uuid
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

from sub.spark.jobs.driver_master.preference import (
    build_driver_preferences,
    extend_driver_preferences,
    write_driver_preferences,
)
from sub.spark.jobs.driver_master.traits import load_bootstrap_pools
from sub.scripts.synthetic_company_snapshot.snapshot import (
    SnapshotTables,
    evolve_company_snapshot,
    read_snapshot,
    write_snapshot,
)


PREFERENCES_FILE = "driver_preferences.parquet"


@dataclass(frozen=True)
class MonthlyStatePaths:
    snapshot_dir: Path
    preferences_path: Path


def _next_month_start(value: date) -> date:
    return date(value.year + (value.month == 12), value.month % 12 + 1, 1)


def _data_month_partition(root: Path, value: date) -> Path:
    return root / f"data_month={value.strftime('%Y-%m')}"


def _snapshot_date(tables: SnapshotTables) -> date:
    dates = set(pd.to_datetime(tables.lease_contract["snapshot_date"]).dt.date)
    if len(dates) != 1:
        raise ValueError(f"전월 snapshot_date가 하나가 아닙니다: {sorted(dates)}")
    return dates.pop()


def _active_driver_ids(tables: SnapshotTables) -> list[str]:
    active = tables.lease_contract[tables.lease_contract["lease_ended_on"].isna()]
    if active["customer_id"].duplicated().any() or active["taxi_id"].duplicated().any():
        raise ValueError("기사 또는 택시에 활성 리스가 여러 건입니다")
    mapped = active[["customer_id"]].merge(
        tables.customer[["customer_id", "synthetic_driver_id"]],
        on="customer_id",
        how="left",
        validate="one_to_one",
    )
    if mapped["synthetic_driver_id"].isna().any():
        raise ValueError("활성 리스의 기사 ID를 찾을 수 없습니다")
    return sorted(mapped["synthetic_driver_id"].astype(str))


def _validate_state(snapshot_dir: Path) -> MonthlyStatePaths:
    tables = read_snapshot(snapshot_dir)
    preferences_path = snapshot_dir / PREFERENCES_FILE
    if not preferences_path.is_file():
        raise FileNotFoundError(f"기사 선호 파일이 없습니다: {preferences_path}")
    preferences = pd.read_parquet(preferences_path)
    if preferences["driver_id"].isna().any() or preferences["driver_id"].duplicated().any():
        raise ValueError("기사 선호 driver_id는 null 없이 고유해야 합니다")
    missing = set(_active_driver_ids(tables)) - set(preferences["driver_id"].astype(str))
    if missing:
        raise ValueError(f"활성 기사 선호가 없습니다: {sorted(missing)[:5]}")
    return MonthlyStatePaths(snapshot_dir, preferences_path)


def prepare_monthly_state(
    *,
    previous_snapshot_dir: str | Path,
    previous_preferences_path: str | Path | None,
    hvfhv_input_dir: str | Path,
    output_dir: str | Path,
    snapshot_date: date,
    seed: int = 42,
    change_rate: float | None = None,
) -> MonthlyStatePaths:
    """전월 상태를 한 달 진화시켜 완결된 디렉터리로 원자적으로 공개합니다."""
    if snapshot_date.day != 1:
        raise ValueError("월별 snapshot_date는 매월 1일이어야 합니다")

    output_root = Path(output_dir)
    target = _data_month_partition(output_root, snapshot_date)
    if target.exists():
        return _validate_state(target)

    previous = read_snapshot(previous_snapshot_dir)
    previous_date = _snapshot_date(previous)
    if previous_date == snapshot_date:
        current = previous
    elif _next_month_start(previous_date) == snapshot_date:
        vehicle_pool = previous.taxi[
            [
                "make_key",
                "model_key",
                "model_year",
                "weekly_price_usd",
                "uber_comfort_eligible",
                "lyft_extra_comfort_eligible",
                "vehicle_group",
            ]
        ].drop_duplicates()
        current = evolve_company_snapshot(
            previous,
            vehicle_pool,
            snapshot_date=snapshot_date,
            seed=seed,
            change_rate=change_rate,
        )
    else:
        raise ValueError(
            "대상 월 또는 직전 월 스냅샷이 필요합니다: "
            f"previous={previous_date}, target={snapshot_date}"
        )
    pools = load_bootstrap_pools(
        bronze_dir=str(hvfhv_input_dir),
        months=[snapshot_date.strftime("%Y-%m")],
        seed=seed,
    )
    preference_path = Path(previous_preferences_path) if previous_preferences_path else None
    if preference_path and preference_path.is_file():
        preferences = extend_driver_preferences(
            pd.read_parquet(preference_path),
            _active_driver_ids(current),
            pools,
            as_of_date=np.datetime64(snapshot_date),
            seed=seed,
        )
    else:
        preferences = build_driver_preferences(
            _active_driver_ids(current),
            pools,
            as_of_date=np.datetime64(snapshot_date),
            seed=seed,
        )

    output_root.mkdir(parents=True, exist_ok=True)
    staging_root = output_root / f".snapshot-{snapshot_date}-{uuid.uuid4().hex}"
    snapshot_partition = staging_root / f"snapshot_date={snapshot_date.isoformat()}"
    staged_partition = _data_month_partition(staging_root, snapshot_date)
    try:
        write_snapshot(current, staging_root, snapshot_date)
        snapshot_partition.rename(staged_partition)
        write_driver_preferences(preferences, staged_partition / PREFERENCES_FILE)
        _validate_state(staged_partition)
        staged_partition.rename(target)
        return _validate_state(target)
    finally:
        shutil.rmtree(staging_root, ignore_errors=True)
