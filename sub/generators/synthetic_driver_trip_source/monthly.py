"""전월 상태에서 당월 기사·차량·리스와 기사 선호를 결정적으로 갱신합니다.

lifecycle·성향·차량배정의 정본은 `sub/generators/synthetic_driver_state`
(event sourcing, #605)입니다. 여기서 쓰는 `customer`/`taxi`/`lease_contract`/
`driver_preferences`는 그 상태를 `adapters`로 비춘 legacy 뷰일 뿐입니다 —
기존 Spark 경로(`candidates.py`/`allocator.py`)를 바꾸지 않기 위해서입니다
(asistobe.md Phase C).
"""

from __future__ import annotations

import shutil
import uuid
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pandas as pd

from sub.config import GenerationConfig
from sub.generators.synthetic_driver_state import adapters, checkpoint, fleet
from sub.generators.synthetic_driver_state.lifecycle import synthesize_month
from sub.generators.synthetic_company_snapshot.snapshot import read_snapshot, write_snapshot
from sub.run_context import RunContext
from sub.spark.jobs.driver_master.preference import write_driver_preferences
from sub.spark.jobs.driver_master.traits import load_bootstrap_pools

CHECKPOINT_DIR_NAME = "driver_state"
PREFERENCES_FILE = "driver_preferences.parquet"


@dataclass(frozen=True)
class MonthlyStatePaths:
    snapshot_dir: Path
    preferences_path: Path


def _data_month_partition(root: Path, value: date) -> Path:
    return root / f"data_month={value.strftime('%Y-%m')}"


def _active_driver_ids_legacy(snapshot_dir: Path) -> list[str]:
    tables = read_snapshot(snapshot_dir)
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
    preferences_path = snapshot_dir / PREFERENCES_FILE
    if not preferences_path.is_file():
        raise FileNotFoundError(f"기사 선호 파일이 없습니다: {preferences_path}")
    preferences = pd.read_parquet(preferences_path)
    if preferences["driver_id"].isna().any() or preferences["driver_id"].duplicated().any():
        raise ValueError("기사 선호 driver_id는 null 없이 고유해야 합니다")
    missing = set(_active_driver_ids_legacy(snapshot_dir)) - set(preferences["driver_id"].astype(str))
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
    config: GenerationConfig,
    vehicle_master_path: str | Path,
) -> MonthlyStatePaths:
    """전월 체크포인트를 한 달 진화시켜 완결된 디렉터리로 원자적으로 공개합니다.

    `previous_snapshot_dir`/`previous_preferences_path`는 현재 읽지 않습니다 —
    상태의 정본이 `#605`의 체크포인트(`output_dir/driver_state/`)로 옮겨갔기
    때문입니다. `source_job.py` CLI 호환을 위해 인자만 남겨 뒀고, 제거는 `#607`
    (Spark 입력 단순화)에서 합니다.
    """
    if snapshot_date.day != 1:
        raise ValueError("월별 snapshot_date는 매월 1일이어야 합니다")

    output_root = Path(output_dir)
    target = _data_month_partition(output_root, snapshot_date)
    if target.exists():
        return _validate_state(target)

    target_month = snapshot_date.strftime("%Y-%m")
    run = RunContext.create(target_month, config)
    checkpoint_dir = output_root / CHECKPOINT_DIR_NAME

    prev_current, prev_events, prev_noise, prev_month, prev_run_id = (
        checkpoint.resolve_previous_checkpoint(checkpoint_dir, run)
    )
    vehicle_pool = adapters.vehicle_pool_from_silver(pd.read_parquet(vehicle_master_path))
    trip_pool = load_bootstrap_pools(
        bronze_dir=str(hvfhv_input_dir),
        months=[target_month],
        sample_per_month=config.bootstrap.sample_per_month,
        seed=config.global_seed,
    )
    fuel = fleet.load_fuel_prices()

    result = synthesize_month(
        target_month=target_month,
        config=config,
        vehicle_master=vehicle_pool,
        trip_pool=trip_pool,
        previous_current=prev_current,
        previous_events=prev_events,
        previous_noise=prev_noise,
        fuel=fuel,
    )
    events_all = (
        pd.concat([prev_events, result.events], ignore_index=True)
        if prev_events is not None
        else result.events
    )
    checkpoint.write_checkpoint(
        checkpoint_dir,
        run,
        events=result.events,
        events_all=events_all,
        current=result.current,
        noise=result.noise_state,
        previous_month_value=prev_month,
        previous_run_id=prev_run_id,
    )

    tables = adapters.to_snapshot_tables(result.current, vehicle_pool, snapshot_date=snapshot_date)
    preferences = adapters.to_driver_preferences(result.profiles)

    output_root.mkdir(parents=True, exist_ok=True)
    staging_root = output_root / f".snapshot-{snapshot_date}-{uuid.uuid4().hex}"
    snapshot_partition = staging_root / f"snapshot_date={snapshot_date.isoformat()}"
    staged_partition = _data_month_partition(staging_root, snapshot_date)
    try:
        write_snapshot(tables, staging_root, snapshot_date)
        snapshot_partition.rename(staged_partition)
        write_driver_preferences(preferences, staged_partition / PREFERENCES_FILE)
        _validate_state(staged_partition)
        staged_partition.rename(target)
        return _validate_state(target)
    finally:
        shutil.rmtree(staging_root, ignore_errors=True)
