"""전월 상태에서 당월 기사·차량과 기사 선호를 결정적으로 갱신합니다.

lifecycle·성향·차량배정의 정본은 `sub/generators/synthetic_driver_state`
(event sourcing, #605)입니다. 여기서 쓰는 `driver_preferences`/
`current_driver_vehicle`는 그 상태를 `adapters`로 비춘 뷰일 뿐입니다 —
candidates.py의 후보 생성과 source_job.py의 발행(기사 스냅샷·보유 차량 재고)
이 함께 이 두 뷰를 읽습니다(#643, #609).
"""

from __future__ import annotations

import io
import json
import shutil
import uuid
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from sub.config import GenerationConfig
from sub.generators.synthetic_driver_state import adapters, checkpoint, traits
from sub.generators.synthetic_driver_state.events import (
    EVENT_EXIT,
    EVENT_JOIN,
    EVENT_VEHICLE_CHANGE,
)
from sub.generators.synthetic_driver_state.lifecycle import synthesize_month
from sub.run_context import RunContext
from sub.spark.jobs.driver_master.preference import (
    PREFERENCE_COLUMNS,
    write_driver_preferences,
)
from shared.common.s3_reader import is_s3_uri, parse_s3_uri, read_parquet_uri
from shared.common.source_published_layout import (
    S3_PUBLISHED_RUNTIME_PREFIX,
    dataset_key,
    manifest_key,
)
from sub.spark.jobs.driver_master.traits import load_bootstrap_pools

CHECKPOINT_DIR_NAME = "driver_state"
PREFERENCES_FILE = "driver_preferences.parquet"
CURRENT_DRIVER_VEHICLE_FILE = "current_driver_vehicle.parquet"
# `S3CheckpointStore` 와 같은 뿌리를 씁니다 — 체크포인트와 그 파생 스냅샷이 흩어지면
# 어느 달을 지워야 하는지 사람이 두 곳을 봐야 합니다.
S3_STATE_PREFIX = S3_PUBLISHED_RUNTIME_PREFIX
_CURRENT_DRIVER_VEHICLE_DATE_COLUMNS = ("joined_on", "lease_started_on", "lease_ended_on")


def _current_driver_vehicle_table(frame: pd.DataFrame) -> pa.Table:
    """`joined_on`/`lease_started_on`/`lease_ended_on` 을 명시적으로 date32 로 캐스팅합니다.

    이 셋 중 하나가 전부 같은 값(예: 아무도 퇴사하지 않아 `lease_ended_on` 이
    전부 NaT)이면 pandas 의 dtype 추론이 datetime64[ns] 로 남고, pyarrow 는
    그걸 timestamp[ns] 로 씁니다. Spark 의 Parquet 리더는 그 물리 타입을 아예
    거부합니다(`Illegal Parquet type: INT64 (TIMESTAMP(NANOS,false))`) — 그래서
    pandas 의 스키마 추론에 맡기지 않고 매번 date32 로 강제합니다.
    """
    table = pa.Table.from_pandas(frame, preserve_index=False)
    for name in _CURRENT_DRIVER_VEHICLE_DATE_COLUMNS:
        table = table.set_column(
            table.schema.get_field_index(name), name, table.column(name).cast(pa.date32())
        )
    return table


def _write_current_driver_vehicle(frame: pd.DataFrame, path: Path) -> None:
    pq.write_table(_current_driver_vehicle_table(frame), path)


def _table_bytes(table: pa.Table) -> bytes:
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    return buffer.getvalue()


def _current_driver_vehicle_bytes(frame: pd.DataFrame) -> bytes:
    return _table_bytes(_current_driver_vehicle_table(frame))


def _preferences_bytes(frame: pd.DataFrame) -> bytes:
    """`write_driver_preferences` 와 **같은 컬럼 집합**을 씁니다.

    여기서 컬럼을 안 자르면 로컬과 S3 의 스키마가 갈려, 같은 달을 storage 만 바꿔
    돌렸을 때 하류 Spark 스키마가 달라집니다.
    """
    return _table_bytes(
        pa.Table.from_pandas(frame[list(PREFERENCE_COLUMNS)], preserve_index=False)
    )


@dataclass(frozen=True)
class MonthlyStatePaths:
    """경로가 `Path` 가 아니라 `str` 인 이유 — `s3://` 도 담습니다.

    `Path("s3://b/x")` 는 `s3:/b/x` 로 뭉개져 스킴이 깨집니다. 하류(`source_job`)는
    이 값을 그대로 `spark.read.parquet` 에 넘기므로 문자열 그대로 보존해야 합니다.
    """

    snapshot_dir: str
    preferences_path: str
    current_driver_vehicle_path: str
    clip_rate: float


def _join(base: str, name: str) -> str:
    return f"{base.rstrip('/')}/{name}"


def _data_month_partition(root: str | Path, value: date) -> str:
    return _join(str(root), f"data_month={value.strftime('%Y-%m')}")


def _snapshot_root(output_dir: str | Path, *, storage: str, bucket: str | None) -> str:
    """스냅샷을 어디에 둘지. `storage=s3` 면 로컬 `output_dir` 은 무시합니다.

    EMR 워커는 Airflow 컨테이너의 로컬 디스크를 못 보므로, 운영에서 이 두 파일이
    로컬에 남으면 executor 가 `spark.read.parquet` 에서 죽습니다.
    """
    if storage == "local":
        return str(output_dir)
    if storage == "s3":
        if not bucket:
            raise ValueError("storage=s3 는 bucket 이 필요합니다 (DATA_LAKE_S3_BUCKET)")
        return f"s3://{bucket}/{S3_STATE_PREFIX}"
    raise ValueError(f"알 수 없는 storage: {storage!r} (local 또는 s3)")


def _published_current(snapshot: pd.DataFrame) -> pd.DataFrame:
    required = {"driver_id", "taxi_id", "join_date", "exit_date", "vehicle_since"}
    missing = required - set(snapshot.columns)
    if missing:
        raise ValueError(f"published 기사 스냅샷 필수 컬럼이 없습니다: {sorted(missing)}")
    if snapshot["driver_id"].isna().any() or snapshot["driver_id"].duplicated().any():
        raise ValueError("published 기사 스냅샷 driver_id는 null 없이 고유해야 합니다")
    current = pd.DataFrame(
        {
            "driver_id": snapshot["driver_id"].astype(str),
            "taxi_id": snapshot["taxi_id"].astype(str),
            "traits_pool_month": pd.to_datetime(snapshot["join_date"]).dt.strftime("%Y-%m"),
            "joined_on": pd.to_datetime(snapshot["join_date"]),
            "exited_on": pd.to_datetime(snapshot["exit_date"]),
            "vehicle_since": pd.to_datetime(snapshot["vehicle_since"]),
        }
    )
    if current[["taxi_id", "traits_pool_month", "joined_on", "vehicle_since"]].isna().any().any():
        raise ValueError("published 기사 스냅샷의 현재 상태 필드에 null이 있습니다")
    return current.sort_values("driver_id").reset_index(drop=True)


def _events_from_published(current: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict] = []
    for record in current.to_dict("records"):
        common = {"driver_id": record["driver_id"]}
        rows.append(
            common
            | {
                "event_type": EVENT_JOIN,
                "event_ts": record["joined_on"],
                "taxi_id": record["taxi_id"],
                "traits_pool_month": record["traits_pool_month"],
            }
        )
        if pd.Timestamp(record["vehicle_since"]) > pd.Timestamp(record["joined_on"]):
            rows.append(
                common
                | {
                    "event_type": EVENT_VEHICLE_CHANGE,
                    "event_ts": record["vehicle_since"],
                    "taxi_id": record["taxi_id"],
                    "traits_pool_month": None,
                }
            )
        if pd.notna(record["exited_on"]):
            rows.append(
                common
                | {
                    "event_type": EVENT_EXIT,
                    "event_ts": record["exited_on"],
                    "taxi_id": None,
                    "traits_pool_month": None,
                }
            )
    return pd.DataFrame(rows)


def _resolve_previous_published(
    run: RunContext, *, bucket: str
) -> tuple[pd.DataFrame | None, pd.DataFrame | None, pd.DataFrame | None, str | None, str | None]:
    """S3 운영에서는 전월 published 릴리스만 상태 정본으로 사용합니다."""
    bootstrap_month = run.config.bootstrap.snapshot_date.strftime("%Y-%m")
    if run.target_month <= bootstrap_month:
        return None, None, None, None, None

    from shared.common.s3_reader import get_object_bytes
    import botocore.exceptions

    prev_month = checkpoint.previous_month(run.target_month)
    try:
        manifest = json.loads(
            get_object_bytes(bucket, manifest_key(prev_month)).decode("utf-8")
        )
    except botocore.exceptions.ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in ("NoSuchKey", "404"):
            raise checkpoint.CheckpointLineageError(
                f"{run.target_month} 는 첫 달(config bootstrap.snapshot_date={bootstrap_month})이 "
                f"아닌데 전월({prev_month}) published manifest가 없습니다: "
                f"s3://{bucket}/{manifest_key(prev_month)}"
            ) from exc
        raise
    if manifest.get("config_hash") != run.config_hash:
        raise checkpoint.CheckpointLineageError(
            f"{prev_month} published 릴리스의 config_hash({manifest.get('config_hash')!r})가 "
            f"요청({run.config_hash!r})과 다릅니다. {prev_month}부터 같은 설정으로 다시 생성하세요."
        )
    entry = manifest.get("datasets", {}).get("driver_vehicle_monthly_snapshot", {})
    snapshot_key = entry.get("key") or dataset_key(
        "driver_vehicle_monthly_snapshot", prev_month
    )
    snapshot = read_parquet_uri(f"s3://{bucket}/{snapshot_key}")
    current = _published_current(snapshot)
    events = _events_from_published(current)
    noise = traits.replay_noise_state(current, through_month=prev_month, config=run.config)
    return current, events, noise, prev_month, manifest.get("run_id")


def _exists(uri: str) -> bool:
    if not is_s3_uri(uri):
        return Path(uri).is_file()

    from shared.common.s3_reader import list_keys

    bucket, key = parse_s3_uri(uri)
    return key in set(list_keys(bucket, key))


def _put_bytes(uri: str, body: bytes) -> None:
    import boto3

    bucket, key = parse_s3_uri(uri)
    boto3.client("s3").put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/octet-stream",
        ServerSideEncryption="AES256",
    )


def _active_driver_ids(snapshot_dir: str) -> list[str]:
    current_driver_vehicle = read_parquet_uri(_join(snapshot_dir, CURRENT_DRIVER_VEHICLE_FILE))
    active = current_driver_vehicle[current_driver_vehicle["lease_ended_on"].isna()]
    if active["driver_id"].duplicated().any():
        raise ValueError("기사에 활성 리스가 여러 건입니다")
    return sorted(active["driver_id"].astype(str))


def _validate_state(
    snapshot_dir: str,
    checkpoint_dir: str | Path,
    *,
    storage: str = "local",
    bucket: str | None = None,
) -> MonthlyStatePaths:
    preferences_path = _join(snapshot_dir, PREFERENCES_FILE)
    current_driver_vehicle_path = _join(snapshot_dir, CURRENT_DRIVER_VEHICLE_FILE)
    if not _exists(preferences_path):
        raise FileNotFoundError(f"기사 선호 파일이 없습니다: {preferences_path}")
    if not _exists(current_driver_vehicle_path):
        raise FileNotFoundError(f"current_driver_vehicle 파일이 없습니다: {current_driver_vehicle_path}")
    preferences = read_parquet_uri(preferences_path)
    if preferences["driver_id"].isna().any() or preferences["driver_id"].duplicated().any():
        raise ValueError("기사 선호 driver_id는 null 없이 고유해야 합니다")
    missing = set(_active_driver_ids(snapshot_dir)) - set(preferences["driver_id"].astype(str))
    if missing:
        raise ValueError(f"활성 기사 선호가 없습니다: {sorted(missing)[:5]}")
    target_month = snapshot_dir.rstrip("/").rsplit("/", 1)[-1].removeprefix("data_month=")
    # `checkpoint.read_manifest` 는 로컬 전용입니다 — storage=s3 면 항상 None 을 줘서
    # "체크포인트가 없습니다" 로 잘못 죽습니다.
    manifest = checkpoint.build_store(
        checkpoint_dir, storage=storage, bucket=bucket
    ).read_manifest(target_month)
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
    storage: str = "local",
    bucket: str | None = None,
) -> MonthlyStatePaths:
    """전월 체크포인트를 한 달 진화시켜 완결된 디렉터리로 원자적으로 공개합니다."""
    if snapshot_date.day != 1:
        raise ValueError("월별 snapshot_date는 매월 1일이어야 합니다")

    output_root = Path(output_dir)
    checkpoint_dir = output_root / CHECKPOINT_DIR_NAME
    snapshot_root = _snapshot_root(output_root, storage=storage, bucket=bucket)
    target = _data_month_partition(snapshot_root, snapshot_date)
    # 완결 신호는 **두 파일이 모두** 있는지입니다. S3 에는 rename 이 없어 디렉터리
    # 존재만 보면 반쯤 올라간 파티션을 완결된 것으로 오인합니다.
    if storage == "local" and _exists(_join(target, PREFERENCES_FILE)) and _exists(
        _join(target, CURRENT_DRIVER_VEHICLE_FILE)
    ):
        return _validate_state(target, checkpoint_dir, storage=storage, bucket=bucket)

    target_month = snapshot_date.strftime("%Y-%m")
    run = RunContext.create(target_month, config)

    if storage == "s3":
        if not bucket:
            raise ValueError("storage=s3 는 bucket 이 필요합니다 (DATA_LAKE_S3_BUCKET)")
        prev_current, prev_events, prev_noise, prev_month, prev_run_id = (
            _resolve_previous_published(run, bucket=bucket)
        )
    else:
        prev_current, prev_events, prev_noise, prev_month, prev_run_id = (
            checkpoint.resolve_previous_checkpoint(checkpoint_dir, run)
        )
    # s3:// 도 받습니다. EMR 워커는 컨테이너 로컬 디스크를 못 봅니다.
    vehicle_pool = adapters.vehicle_pool_from_silver(
        read_parquet_uri(str(vehicle_master_path))
    )
    trip_pool = load_bootstrap_pools(
        bronze_dir=str(hvfhv_input_dir),
        months=[target_month],
        sample_per_month=config.bootstrap.sample_per_month,
        seed=config.global_seed,
    )
    result = synthesize_month(
        target_month=target_month,
        config=config,
        vehicle_master=vehicle_pool,
        trip_pool=trip_pool,
        previous_current=prev_current,
        previous_events=prev_events,
        previous_noise=prev_noise,
    )
    events_all = (
        pd.concat([prev_events, result.events], ignore_index=True)
        if prev_events is not None
        else result.events
    )
    if storage == "local":
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

    if is_s3_uri(target):
        return _publish_to_s3(
            target,
            preferences=preferences,
            current_driver_vehicle=current_driver_vehicle,
            bucket=bucket,
            clip_rate=result.clip_rate,
        )

    output_root.mkdir(parents=True, exist_ok=True)
    staging_root = output_root / f".snapshot-{snapshot_date}-{uuid.uuid4().hex}"
    staged_partition = Path(_data_month_partition(staging_root, snapshot_date))
    try:
        staged_partition.mkdir(parents=True)
        write_driver_preferences(preferences, staged_partition / PREFERENCES_FILE)
        _write_current_driver_vehicle(
            current_driver_vehicle, staged_partition / CURRENT_DRIVER_VEHICLE_FILE
        )
        _validate_state(str(staged_partition), checkpoint_dir)
        staged_partition.rename(target)
        return _validate_state(target, checkpoint_dir)
    finally:
        shutil.rmtree(staging_root, ignore_errors=True)


def _publish_to_s3(
    target: str,
    *,
    preferences: pd.DataFrame,
    current_driver_vehicle: pd.DataFrame,
    bucket: str | None,
    clip_rate: float,
) -> MonthlyStatePaths:
    """staging→rename 대신 순서로 원자성을 흉내냅니다.

    S3 에 rename 이 없어서, `current_driver_vehicle` 을 **마지막에** 올립니다.
    완결 판정이 두 파일의 동시 존재라, 중간에 죽으면 다음 실행이 미완결로 봅니다.
    """
    _put_bytes(_join(target, PREFERENCES_FILE), _preferences_bytes(preferences))
    _put_bytes(
        _join(target, CURRENT_DRIVER_VEHICLE_FILE),
        _current_driver_vehicle_bytes(current_driver_vehicle),
    )
    preferences_path = _join(target, PREFERENCES_FILE)
    current_driver_vehicle_path = _join(target, CURRENT_DRIVER_VEHICLE_FILE)
    if not _exists(preferences_path) or not _exists(current_driver_vehicle_path):
        raise FileNotFoundError(f"S3 런타임 상태 공개가 완료되지 않았습니다: {target}")
    return MonthlyStatePaths(
        target, preferences_path, current_driver_vehicle_path, clip_rate
    )
