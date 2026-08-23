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

로컬과 S3 양쪽에 씁니다
--------------------
EMR Serverless 는 워커가 컨테이너 로컬 디스크를 볼 수 없어 S3 여야 합니다. 그리고
EC2 컨테이너에는 `data/` 볼륨이 없어서, 로컬에 두면 컨테이너가 재생성될 때 체크포인트가
사라집니다.

`s3fs` 를 쓰지 않는 이유 — spark 런타임은 `numpy`/`pandas`/`pyarrow` 를 EMR 7.13 이
제공하는 값과 똑같이 고정합니다. `s3fs` 는 `aiobotocore` 를 끌고 와 `boto3` 핀과
충돌할 위험이 있습니다. bytes 로 주고받습니다 — 기사 2000명 규모라 작습니다.
"""

from __future__ import annotations

import hashlib
import io
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


def _sha256_bytes(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def _parquet_bytes(frame: pd.DataFrame) -> tuple[bytes, int]:
    """DataFrame 을 Parquet bytes 로. 로컬·S3 가 같은 바이트를 쓰도록 한 곳에 둡니다."""
    table = pa.Table.from_pandas(frame, preserve_index=False)
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    return buffer.getvalue(), table.num_rows


def _dataset_entry(name: str, body: bytes, row_count: int) -> dict:
    return {"file": name, "row_count": row_count, "sha256": _sha256_bytes(body)}


class CheckpointStore:
    """체크포인트를 어디에 쓰고 어디서 읽는지. 로컬과 S3 가 같은 계약을 씁니다.

    파티션 하나를 **여러 파일의 묶음**으로 다룹니다 — 로컬은 staging 디렉터리를
    rename 해서, S3 는 manifest 를 마지막에 올려서 "반쯤 쓰인 파티션" 이 하류에
    보이지 않게 합니다(S3 에 rename 이 없어 manifest 존재가 완결 신호입니다).
    """

    def read_bytes(self, month: str, name: str) -> bytes:  # pragma: no cover - 인터페이스
        raise NotImplementedError

    def read_manifest(self, month: str) -> dict | None:  # pragma: no cover
        raise NotImplementedError

    def write_partition(self, month: str, files: dict[str, bytes], manifest: dict) -> str:  # pragma: no cover
        raise NotImplementedError

    def write_events(self, month: str, body: bytes) -> None:  # pragma: no cover
        raise NotImplementedError

    def has_any_state(self) -> bool:  # pragma: no cover
        """어떤 달이든 체크포인트가 하나라도 있는지.

        전월이 없을 때 "처음 실행" 과 "유실" 을 구분하는 데 씁니다.
        """
        raise NotImplementedError


class LocalCheckpointStore(CheckpointStore):
    def __init__(self, base_dir: str | Path):
        self._base = Path(base_dir)

    def read_bytes(self, month: str, name: str) -> bytes:
        return (state_partition_dir(self._base, month) / name).read_bytes()

    def read_manifest(self, month: str) -> dict | None:
        path = state_partition_dir(self._base, month) / MANIFEST_FILE
        if not path.is_file():
            return None
        return json.loads(path.read_text(encoding="utf-8"))

    def has_any_state(self) -> bool:
        root = self._base / STATE_DIR_NAME
        return root.is_dir() and any(root.glob("snapshot_month=*/" + MANIFEST_FILE))

    def write_events(self, month: str, body: bytes) -> None:
        final = event_partition_dir(self._base, month)
        _atomic_write_dir(final, lambda staging: (staging / "events.parquet").write_bytes(body))

    def write_partition(self, month: str, files: dict[str, bytes], manifest: dict) -> str:
        final = state_partition_dir(self._base, month)

        def _write(staging: Path) -> None:
            for name, body in files.items():
                (staging / name).write_bytes(body)
            (staging / MANIFEST_FILE).write_text(_manifest_json(manifest), encoding="utf-8")

        _atomic_write_dir(final, _write)
        return str(final)


class S3CheckpointStore(CheckpointStore):
    """`s3://<bucket>/<prefix>/state/snapshot_month=YYYY-MM/...`.

    manifest 를 **마지막에** 올립니다. S3 에는 rename 이 없어서, 중간에 죽으면
    데이터 파일은 남아도 manifest 가 없어 하류가 그 파티션을 없는 것으로 봅니다.
    """

    def __init__(self, bucket: str, prefix: str = "source/synthetic_driver_trip_state"):
        self._bucket = bucket
        self._prefix = prefix.strip("/")

    def _key(self, month: str, name: str, *, event: bool = False) -> str:
        directory = EVENT_DIR_NAME if event else STATE_DIR_NAME
        return f"{self._prefix}/{directory}/snapshot_month={month}/{name}"

    def read_bytes(self, month: str, name: str) -> bytes:
        from shared.common.s3_reader import get_object_bytes

        return get_object_bytes(self._bucket, self._key(month, name))

    def read_manifest(self, month: str) -> dict | None:
        import botocore.exceptions
        from shared.common.s3_reader import get_object_bytes

        try:
            body = get_object_bytes(self._bucket, self._key(month, MANIFEST_FILE))
        except botocore.exceptions.ClientError as exc:
            if exc.response.get("Error", {}).get("Code") in ("NoSuchKey", "404"):
                return None
            raise
        return json.loads(body.decode("utf-8"))

    def has_any_state(self) -> bool:
        from shared.common.s3_reader import list_keys

        prefix = f"{self._prefix}/{STATE_DIR_NAME}/"
        return any(key.endswith(MANIFEST_FILE) for key in list_keys(self._bucket, prefix))

    def _put(self, key: str, body: bytes, content_type: str) -> None:
        import boto3

        boto3.client("s3").put_object(
            Bucket=self._bucket, Key=key, Body=body,
            ContentType=content_type, ServerSideEncryption="AES256",
        )

    def write_events(self, month: str, body: bytes) -> None:
        self._put(self._key(month, "events.parquet", event=True), body, "application/octet-stream")

    def write_partition(self, month: str, files: dict[str, bytes], manifest: dict) -> str:
        for name, body in files.items():
            self._put(self._key(month, name), body, "application/octet-stream")
        # manifest 가 마지막입니다 — 이것이 있으면 파티션이 완결된 것입니다.
        self._put(self._key(month, MANIFEST_FILE), _manifest_json(manifest).encode("utf-8"),
                  "application/json")
        return f"s3://{self._bucket}/{self._prefix}/{STATE_DIR_NAME}/snapshot_month={month}"


def build_store(
    base_dir: str | Path, *, storage: str = "local", bucket: str | None = None
) -> CheckpointStore:
    if storage == "local":
        return LocalCheckpointStore(base_dir)
    if storage == "s3":
        if not bucket:
            raise ValueError("storage=s3 는 bucket 이 필요합니다 (DATA_LAKE_S3_BUCKET)")
        return S3CheckpointStore(bucket)
    raise ValueError(f"알 수 없는 storage: {storage!r} (local 또는 s3)")


def _manifest_json(manifest: dict) -> str:
    return json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True, default=str)


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
    """로컬 전용 하위 호환. 새 코드는 `CheckpointStore.read_manifest` 를 쓰세요."""
    return LocalCheckpointStore(base_dir).read_manifest(target_month)


def read_checkpoint(
    base_dir: str | Path,
    target_month: str,
    *,
    storage: str = "local",
    bucket: str | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict]:
    """그 달 체크포인트를 읽습니다. `(current, events_all, noise, manifest)`."""
    store = build_store(base_dir, storage=storage, bucket=bucket)
    manifest = store.read_manifest(target_month)
    if manifest is None:
        raise FileNotFoundError(f"체크포인트가 없습니다: snapshot_month={target_month}")
    frames = [
        pd.read_parquet(io.BytesIO(store.read_bytes(target_month, name)))
        for name in (CURRENT_FILE, EVENT_ALL_FILE, NOISE_FILE)
    ]
    return frames[0], frames[1], frames[2], manifest


def _is_bootstrap_month(run: RunContext) -> bool:
    """이 달이 설정상 **첫 달**인가.

    첫 달이면 전월 체크포인트가 없는 것이 정상입니다. 그렇지 않은데 없으면 유실입니다.
    """
    bootstrap_month = run.config.bootstrap.snapshot_date.strftime("%Y-%m")
    return run.target_month <= bootstrap_month


def resolve_previous_checkpoint(
    base_dir: str | Path,
    run: RunContext,
    *,
    storage: str = "local",
    bucket: str | None = None,
) -> tuple[pd.DataFrame | None, pd.DataFrame | None, pd.DataFrame | None, str | None, str | None]:
    """전월 체크포인트를 찾습니다.

    부트스트랩 월이면 `(None, None, None, None, None)`.

    부트스트랩이 **아닌데** 전월이 없으면 실패시킵니다
    -------------------------------------------
    전에는 그때도 `None` 을 돌려줘 부트스트랩으로 취급했습니다. 그러면 "기사 2000명이
    그 달에 새로 입사한" 초기 스냅샷이 에러 없이 만들어지고, 기사 연속성(입·퇴사·차량
    변경 이력)이 끊기는데 결과만 보고는 알 수 없습니다. EC2 컨테이너에 `data/` 볼륨이
    없어 재생성될 때마다 실제로 그렇게 됐습니다.

    `config_hash` 가 다르면 조용히 무시하지 않고 어느 월부터 다시 생성해야 하는지
    명시하며 거부합니다 — 임의의 과거 월부터 재개할 수 있어야 하므로, 이 거부가
    재개 지점을 알려주는 역할도 합니다.
    """
    store = build_store(base_dir, storage=storage, bucket=bucket)
    prev_month = previous_month(run.target_month)
    manifest = store.read_manifest(prev_month)
    if manifest is None:
        if _is_bootstrap_month(run):
            return None, None, None, None, None
        bootstrap_month = run.config.bootstrap.snapshot_date.strftime("%Y-%m")
        detail = (
            "다른 달 체크포인트는 있습니다 — 전월만 없습니다"
            if store.has_any_state()
            else "체크포인트가 하나도 없습니다"
        )
        raise CheckpointLineageError(
            f"{run.target_month} 는 첫 달(config bootstrap.snapshot_date={bootstrap_month})이 "
            f"아닌데 전월({prev_month}) 체크포인트가 없습니다. {detail}. "
            f"{bootstrap_month} 부터 순서대로 생성하거나, 첫 달을 다시 지정하세요. "
            "그대로 진행하면 기사 연속성이 끊긴 초기 스냅샷이 만들어집니다."
        )
    if manifest["config_hash"] != run.config_hash:
        raise CheckpointLineageError(
            f"{prev_month} 체크포인트의 config_hash({manifest['config_hash']!r})가 "
            f"요청({run.config_hash!r})과 다릅니다. {prev_month} 부터 이 설정으로 "
            "다시 생성한 뒤 재실행하세요."
        )
    current, events_all, noise, _ = read_checkpoint(
        base_dir, prev_month, storage=storage, bucket=bucket
    )
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
    clip_rate: float,
    storage: str = "local",
    bucket: str | None = None,
) -> str:
    """이 달의 이벤트·상태를 원자적으로 공개합니다.

    로컬은 staging 디렉터리를 rename 하고, S3 는 manifest 를 마지막에 올립니다
    (S3 에 rename 이 없어 manifest 존재가 완결 신호입니다).

    이미 같은 `run_id` 로 존재하면 다시 쓰지 않고 그대로 반환합니다(재시도
    안전). 다른 설정으로 이미 존재하면 조용히 덮지 않고 거부합니다 — 어느
    파티션을 지워야 재생성되는지 메시지에 남깁니다.
    """
    store = build_store(base_dir, storage=storage, bucket=bucket)
    existing = store.read_manifest(run.target_month)
    if existing is not None:
        if existing["run_id"] == run.run_id:
            return _partition_label(base_dir, run.target_month, storage=storage, bucket=bucket)
        raise CheckpointLineageError(
            f"{run.target_month} 체크포인트가 다른 설정으로 이미 있습니다 "
            f"(기존 run_id={existing['run_id']!r} != 요청={run.run_id!r}). "
            "재생성하려면 이 파티션을 지우고 다시 실행하세요: "
            f"{_partition_label(base_dir, run.target_month, storage=storage, bucket=bucket)}"
        )

    events_body, _ = _parquet_bytes(events)
    store.write_events(run.target_month, events_body)

    bodies: dict[str, bytes] = {}
    entries: dict[str, dict] = {}
    for key, name, frame in (
        ("driver_vehicle_current", CURRENT_FILE, current),
        ("driver_vehicle_event_all", EVENT_ALL_FILE, events_all),
        ("realization_noise", NOISE_FILE, noise),
    ):
        body, row_count = _parquet_bytes(frame)
        bodies[name] = body
        entries[key] = _dataset_entry(name, body, row_count)

    manifest = {
        "target_month": run.target_month,
        "run_id": run.run_id,
        "config_hash": run.config_hash,
        "previous_month": previous_month_value,
        "previous_run_id": previous_run_id,
        "created_at": run.created_at,
        # D7 노이즈 클리핑 발생 비율(#608 품질 리포트) — 재생성해도 값이
        # 그대로라 상태와 같이 계보로 남깁니다.
        "clip_rate": clip_rate,
        "datasets": entries,
    }
    return store.write_partition(run.target_month, bodies, manifest)


def _partition_label(
    base_dir: str | Path, month: str, *, storage: str, bucket: str | None
) -> str:
    if storage == "s3":
        return f"s3://{bucket}/source/synthetic_driver_trip_state/{STATE_DIR_NAME}/snapshot_month={month}"
    return str(state_partition_dir(base_dir, month))
