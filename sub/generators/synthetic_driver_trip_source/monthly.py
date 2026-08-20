"""전월 상태에서 당월 기사·차량과 기사 선호를 결정적으로 갱신합니다.

lifecycle·성향·차량배정의 정본은 `sub/generators/synthetic_driver_state`
(event sourcing, #605)입니다. 여기서 쓰는 `driver_preferences`/
`current_driver_vehicle`는 그 상태를 `adapters`로 비춘 뷰일 뿐입니다 —
candidates.py의 후보 생성과 source_job.py의 발행(기사 스냅샷·보유 차량 재고)
이 함께 이 두 뷰를 읽습니다(#643, #609).
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
from sub.run_context import RunContext
from sub.spark.jobs.driver_master.preference import write_driver_preferences
from sub.spark.jobs.driver_master.traits import load_bootstrap_pools

CHECKPOINT_DIR_NAME = "driver_state"
PREFERENCES_FILE = "driver_preferences.parquet"
CURRENT_DRIVER_VEHICLE_FILE = "current_driver_vehicle.parquet"


@dataclass(frozen=True)
class MonthlyStatePaths:
    snapshot_dir: Path
    preferences_path: Path
    current_driver_vehicle_path: Path
    clip_rate: float


def _data_month_partition(root: Path, value: date) -> Path:
    return root / f"data_month={value.strftime('%Y-%m')}"


def _active_driver_ids(snapshot_dir: Path) -> list[str]:
    current_driver_vehicle = pd.read_parquet(snapshot_dir / CURRENT_DRIVER_VEHICLE_FILE)
    active = current_driver_vehicle[current_driver_vehicle["lease_ended_on"].isna()]
    if active["driver_id"].duplicated().any():
        raise ValueError("기사에 활성 리스가 여러 건입니다")
    return sorted(active["driver_id"].astype(str))


def _validate_state(snapshot_dir: Path, checkpoint_dir: Path) -> MonthlyStatePaths:
    preferences_path = snapshot_dir / PREFERENCES_FILE
    current_driver_vehicle_path = snapshot_dir / CURRENT_DRIVER_VEHICLE_FILE
    if not preferences_path.is_file():
        raise FileNotFoundError(f"기사 선호 파일이 없습니다: {preferences_path}")
    if not current_driver_vehicle_path.is_file():
        raise FileNotFoundError(f"current_driver_vehicle 파일이 없습니다: {current_driver_vehicle_path}")
    preferences = pd.read_parquet(preferences_path)
    if preferences["driver_id"].isna().any() or preferences["driver_id"].duplicated().any():
        raise ValueError("기사 선호 driver_id는 null 없이 고유해야 합니다")
    missing = set(_active_driver_ids(snapshot_dir)) - set(preferences["driver_id"].astype(str))
    if missing:
        raise ValueError(f"활성 기사 선호가 없습니다: {sorted(missing)[:5]}")
    target_month = snapshot_dir.name.removeprefix("data_month=")
    manifest = checkpoint.read_manifest(checkpoint_dir, target_month)
    if manifest is None:
        raise FileNotFoundError(f"체크포인트가 없습니다: {checkpoint_dir}")
    return MonthlyStatePaths(
        snapshot_dir, preferences_path, current_driver_vehicle_path, manifest["clip_rate"]
    )


def prepare_monthly_state(
    *,
    hvfhv_input_dir: str | Path,
    output_dir: str | Path,
    snapshot_date: date,
    config: GenerationConfig,
    vehicle_master_path: str | Path,
) -> MonthlyStatePaths:
    """전월 체크포인트를 한 달 진화시켜 완결된 디렉터리로 원자적으로 공개합니다."""
    if snapshot_date.day != 1:
        raise ValueError("월별 snapshot_date는 매월 1일이어야 합니다")

    output_root = Path(output_dir)
    checkpoint_dir = output_root / CHECKPOINT_DIR_NAME
    target = _data_month_partition(output_root, snapshot_date)
    if target.exists():
        return _validate_state(target, checkpoint_dir)

    target_month = snapshot_date.strftime("%Y-%m")
    run = RunContext.create(target_month, config)

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
        clip_rate=result.clip_rate,
    )

    preferences = adapters.to_driver_preferences(result.profiles)
    current_driver_vehicle = adapters.to_current_driver_vehicle(result.current, vehicle_pool)

    output_root.mkdir(parents=True, exist_ok=True)
    staging_root = output_root / f".snapshot-{snapshot_date}-{uuid.uuid4().hex}"
    staged_partition = _data_month_partition(staging_root, snapshot_date)
    try:
        staged_partition.mkdir(parents=True)
        write_driver_preferences(preferences, staged_partition / PREFERENCES_FILE)
        current_driver_vehicle.to_parquet(
            staged_partition / CURRENT_DRIVER_VEHICLE_FILE, index=False
        )
        _validate_state(staged_partition, checkpoint_dir)
        staged_partition.rename(target)
        return _validate_state(target, checkpoint_dir)
    finally:
        shutil.rmtree(staging_root, ignore_errors=True)
