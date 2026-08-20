"""Event/current/noise 체크포인트 I/O와 계보 (blue_print.md 4.3).

`data/source/synthetic_driver_trip_state/` 아래 두 갈래로 씁니다.

    driver_vehicle_event/snapshot_month=YYYY-MM/events.parquet
        그 달에 발생한 이벤트만 (append-only 원장의 월별 파티션)
    state/snapshot_month=YYYY-MM/
        driver_vehicle_current.parquet    그 달 시점의 파생 상태 (fold_events 결과)
        driver_vehicle_event_all.parquet  전체 이벤트 누적 (재생 캐시)
        realization_noise.parquet        D7 (B) 자기상관 상태
        manifest.json                    계보 — target_month/run_id/config_hash/previous_*

`config_hash` 가 다른 전월 체크포인트는 이어받지 않습니다. 조용히 이어받으면
"설정을 바꿨는데 결과가 안 바뀐다" 가 되므로, 어느 월부터 다시 생성해야 하는지
예외 메시지에 명시하며 거부합니다.
"""

from __future__ import annotations

import hashlib
import json
import shutil
import uuid
from collections.abc import Callable
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from sub.run_context import RunContext

EVENT_DIR_NAME = "driver_vehicle_event"
STATE_DIR_NAME = "state"
CURRENT_FILE = "driver_vehicle_current.parquet"
EVENT_ALL_FILE = "driver_vehicle_event_all.parquet"
NOISE_FILE = "realization_noise.parquet"
MANIFEST_FILE = "manifest.json"


class CheckpointLineageError(ValueError):
    """전월 체크포인트의 config_hash 가 요청과 달라 이어받을 수 없거나,
    이 달의 체크포인트가 다른 설정으로 이미 존재할 때."""


def previous_month(target_month: str) -> str:
    ts = pd.Timestamp(f"{target_month}-01") - pd.DateOffset(months=1)
    return ts.strftime("%Y-%m")


def event_partition_dir(base_dir: str | Path, target_month: str) -> Path:
    return Path(base_dir) / EVENT_DIR_NAME / f"snapshot_month={target_month}"


def state_partition_dir(base_dir: str | Path, target_month: str) -> Path:
    return Path(base_dir) / STATE_DIR_NAME / f"snapshot_month={target_month}"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_parquet(frame: pd.DataFrame, path: Path) -> dict:
    pq.write_table(pa.Table.from_pandas(frame, preserve_index=False), path)
    return {
        "file": path.name,
        "row_count": pq.ParquetFile(path).metadata.num_rows,
        "sha256": _sha256(path),
    }


def _atomic_write_dir(final: Path, write: Callable[[Path], None]) -> None:
    """`write` 가 `staging` 안에 다 쓰면 rename 으로 공개합니다.

    중간에 죽어도 반쯤 쓰인 파티션이 하류에 보이지 않습니다.
    """
    final.parent.mkdir(parents=True, exist_ok=True)
    staging = final.parent / f".{final.name}.staging-{uuid.uuid4().hex}"
    try:
        staging.mkdir()
        write(staging)
        if final.exists():
            shutil.rmtree(final)
        staging.rename(final)
    finally:
        shutil.rmtree(staging, ignore_errors=True)


def read_manifest(base_dir: str | Path, target_month: str) -> dict | None:
    path = state_partition_dir(base_dir, target_month) / MANIFEST_FILE
    if not path.is_file():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def read_checkpoint(
    base_dir: str | Path, target_month: str
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict]:
    """그 달 체크포인트를 읽습니다. `(current, events_all, noise, manifest)`."""
    partition = state_partition_dir(base_dir, target_month)
    manifest = read_manifest(base_dir, target_month)
    if manifest is None:
        raise FileNotFoundError(f"체크포인트가 없습니다: {partition}")
    current = pd.read_parquet(partition / CURRENT_FILE)
    events_all = pd.read_parquet(partition / EVENT_ALL_FILE)
    noise = pd.read_parquet(partition / NOISE_FILE)
    return current, events_all, noise, manifest


def resolve_previous_checkpoint(
    base_dir: str | Path, run: RunContext
) -> tuple[pd.DataFrame | None, pd.DataFrame | None, pd.DataFrame | None, str | None, str | None]:
    """전월 체크포인트를 찾습니다.

    없으면(부트스트랩 월) `(None, None, None, None, None)`. `config_hash` 가
    다르면 조용히 무시하지 않고 어느 월부터 다시 생성해야 하는지 명시하며
    거부합니다 — 임의의 과거 월 체크포인트부터 재개할 수 있어야 하므로, 이
    거부가 재개 지점을 알려주는 역할도 합니다.
    """
    prev_month = previous_month(run.target_month)
    manifest = read_manifest(base_dir, prev_month)
    if manifest is None:
        return None, None, None, None, None
    if manifest["config_hash"] != run.config_hash:
        raise CheckpointLineageError(
            f"{prev_month} 체크포인트의 config_hash({manifest['config_hash']!r})가 "
            f"요청({run.config_hash!r})과 다릅니다. {prev_month} 부터 이 설정으로 "
            "다시 생성한 뒤 재실행하세요."
        )
    current, events_all, noise, _ = read_checkpoint(base_dir, prev_month)
    return current, events_all, noise, prev_month, manifest["run_id"]


def write_checkpoint(
    base_dir: str | Path,
    run: RunContext,
    *,
    events: pd.DataFrame,
    events_all: pd.DataFrame,
    current: pd.DataFrame,
    noise: pd.DataFrame,
    previous_month_value: str | None,
    previous_run_id: str | None,
) -> Path:
    """이 달의 이벤트·상태를 staging 에 쓰고 rename 으로 공개합니다.

    이미 같은 `run_id` 로 존재하면 다시 쓰지 않고 그대로 반환합니다(재시도
    안전). 다른 설정으로 이미 존재하면 조용히 덮지 않고 거부합니다 — 어느
    파티션을 지워야 재생성되는지 메시지에 남깁니다.
    """
    state_final = state_partition_dir(base_dir, run.target_month)
    existing = read_manifest(base_dir, run.target_month)
    if existing is not None:
        if existing["run_id"] == run.run_id:
            return state_final
        raise CheckpointLineageError(
            f"{run.target_month} 체크포인트가 다른 설정으로 이미 있습니다 "
            f"(기존 run_id={existing['run_id']!r} != 요청={run.run_id!r}). "
            f"재생성하려면 이 파티션을 지우고 다시 실행하세요: {state_final}"
        )

    event_final = event_partition_dir(base_dir, run.target_month)
    _atomic_write_dir(event_final, lambda staging: _write_parquet(events, staging / "events.parquet"))

    def _write_state(staging: Path) -> None:
        entries = {
            "driver_vehicle_current": _write_parquet(current, staging / CURRENT_FILE),
            "driver_vehicle_event_all": _write_parquet(events_all, staging / EVENT_ALL_FILE),
            "realization_noise": _write_parquet(noise, staging / NOISE_FILE),
        }
        manifest = {
            "target_month": run.target_month,
            "run_id": run.run_id,
            "config_hash": run.config_hash,
            "previous_month": previous_month_value,
            "previous_run_id": previous_run_id,
            "created_at": run.created_at,
            "datasets": entries,
        }
        (staging / MANIFEST_FILE).write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True, default=str),
            encoding="utf-8",
        )

    _atomic_write_dir(state_final, _write_state)
    return state_final
