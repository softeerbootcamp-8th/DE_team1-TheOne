"""5 · published — data contract.

경계를 넘는 것은 이 세 개뿐입니다. provenance 가 서로 다르다는 점이 중요합니다
(blue_print.md 2.3).

  published_trip_snapshot   실 사실 + 합성 신원  (hybrid)
  published_driver_vehicle  완전 합성
  published_vehicle_master  **완전 실데이터** — 합성 구간을 경유하지 않습니다 (D2)

컬럼과 타입은 `schema/bronze.py` 가 정합니다. 여기서 목록을 다시 적지 않고
`sub.prototype.contract` 로 그 파일을 읽어 **쓰는 시점에 강제**합니다 — 컬럼이
빠지거나 남으면 `pa.Table.from_pandas` 가 그 자리에서 죽습니다. 계약과 산출물이
갈린 채로 파일이 나가는 것이 이 단계에서 가장 비싼 사고입니다.
"""

from __future__ import annotations

import hashlib
import json
import shutil
import uuid
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from sub.prototype import contract, curated, log
from sub.run_context import RunContext
from sub.seeds import Stage, derive_entity_seed, derive_seed

# 계약 스키마가 바뀌면 이 값을 올립니다. 릴리스 재사용 판정에 들어가므로(publish),
# 올리지 않으면 같은 달을 다시 돌려도 **옛 스키마 파일이 그대로 남습니다**.
SCHEMA_VERSION = "2.0.0-source-contract"

def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _naive(created_at) -> pd.Timestamp:
    """`RunContext.created_at` 은 UTC ISO 문자열입니다. 계약 타입이 tz 없는
    timestamp 라 오프셋을 UTC 로 접고 tz 를 뗍니다."""
    stamp = pd.Timestamp(created_at)
    return stamp.tz_convert(None) if stamp.tz is not None else stamp


def _as_date(series: pd.Series) -> pd.Series:
    """date32 로 쓸 수 있게 날짜만 남깁니다. 결측은 None (계약상 nullable)."""
    stamps = pd.to_datetime(series)
    return stamps.dt.date.where(stamps.notna(), None)


def _as_table(frame: pd.DataFrame, schema: pa.Schema) -> pa.Table:
    """계약 스키마로 캐스팅합니다. 컬럼이 다르면 여기서 죽습니다."""
    if frame.empty:
        return pa.Table.from_pylist([], schema=schema)
    return pa.Table.from_pandas(frame, schema=schema, preserve_index=False)


def build_trip_snapshot(attributed: pd.DataFrame) -> pd.DataFrame:
    """실 사실 + 합성 신원. 계약에 없는 내부 컬럼은 여기서 떨어집니다.

    떨어지는 것: `trip_key`(하류 정제가 자연키로 다시 만듭니다), `driver_id`
    (메인이 기사-택시 스냅샷과 `taxi_id` 로 조인합니다), `service_date`·
    `trip_sequence`(`pickup_datetime` 에서 유도), `deadhead_minutes`.

    `on_scene_datetime` 은 원본에 있던 값을 배정 내내 그대로 실어 온 것입니다
    (`attribution.TRIP_CANDIDATE_COLUMNS`/`ASSIGNED_COLUMNS`) — 여기서 다시
    찾아오지 않습니다.
    """
    if attributed.empty:
        return pd.DataFrame(columns=contract.trip_schema().names)
    trips = attributed.reset_index(drop=True)
    zone_names = curated.load_zone_names()
    return pd.DataFrame({
        "taxi_id": trips["taxi_id"],
        "hvfhs_license_num": trips["platform_name"].map(curated.LICENSE_BY_PLATFORM),
        "on_scene_datetime": trips["on_scene_datetime"],
        "pickup_datetime": trips["pickup_datetime"],
        "dropoff_datetime": trips["dropoff_datetime"],
        "PULocationID": trips["PULocationID"],
        "DOLocationID": trips["DOLocationID"],
        "pickup_zone": trips["PULocationID"].map(zone_names),
        "dropoff_zone": trips["DOLocationID"].map(zone_names),
        "trip_miles": trips["trip_miles"],
        "trip_time": trips["trip_time"],
        "driver_pay": trips["driver_pay"],
        "tips": trips["tips"],
        "estimated_service_tier": trips["estimated_service_tier"],
    })


def _experience_years(driver_ids: pd.Series, *, global_seed: int) -> np.ndarray:
    """계약이 요구하는 운전 경력. 어느 계산에도 쓰이지 않는 순수 합성값입니다.

    기사에 영구 귀속되는 값이라 월을 시드에 넣지 않고 기사 단위로 파생합니다
    (docs/seed_design.md 의 `DRIVER_TRAITS`). 기존 기준값 추첨과 파생 인자가 달라
    (`base_traits` 는 `traits_pool_month` 를 같이 넣습니다) 서로 시드를 밀지
    않습니다 — 이 값을 넣어도 배정 결과가 바뀌지 않는 이유입니다.
    """
    stage_seed = derive_seed(global_seed, Stage.DRIVER_TRAITS)
    return np.array(
        [
            int(np.random.default_rng(derive_entity_seed(stage_seed, driver_id)).integers(1, 26))
            for driver_id in driver_ids
        ],
        dtype="int32",
    )


def build_driver_vehicle(
    current: pd.DataFrame,
    fleet_units: pd.DataFrame,
    *,
    target_month: str,
    created_at,
    global_seed: int,
) -> pd.DataFrame:
    """완전 합성. 한 행 = 기사 (그 달 **월말** 상태). 제원은 대장에서 붙여 옵니다.

    I3: `weekly_lease_fee` 를 **주급 그대로** 싣습니다. 월 환산은 메인이 합니다 —
    원천이 월로 환산해 버리면 월중 교체 시 안분이 불가능해집니다(D4). 그 안분에
    필요한 경계가 `vehicle_since` 입니다.
    """
    columns = [
        "taxi_id", "vehicle_model_id", "make_key", "model_key", "fuel_type",
        "weekly_price_usd", "uber_comfort_eligible", "lyft_extra_comfort_eligible",
    ]
    joined = current.merge(fleet_units[columns], on="taxi_id", how="left")
    if joined["make_key"].isna().any():
        raise ValueError("현재 상태의 taxi_id 가 차량 대장에 없습니다")
    joined = joined.sort_values("driver_id").reset_index(drop=True)
    return pd.DataFrame({
        "snapshot_month": target_month,
        "driver_id": joined["driver_id"],
        "taxi_id": joined["taxi_id"],
        "vehicle_model_id": joined["vehicle_model_id"],
        "manufacturer": joined["make_key"],
        "model_name": joined["model_key"],
        "fuel_type": joined["fuel_type"],
        "comfort_eligible": joined["uber_comfort_eligible"].astype(bool),
        "extra_comfort_eligible": joined["lyft_extra_comfort_eligible"].astype(bool),
        "weekly_lease_fee": joined["weekly_price_usd"].astype("float64"),
        "join_date": _as_date(joined["joined_on"]),
        "exit_date": _as_date(joined["exited_on"]),
        "experience_years": _experience_years(joined["driver_id"], global_seed=global_seed),
        "vehicle_since": _as_date(joined["vehicle_since"]),
        "snapshot_created_at": _naive(created_at),
    })


def build_vehicle_inventory(
    vehicle_master: pd.DataFrame, fleet_units: pd.DataFrame
) -> pd.DataFrame:
    """완전 실데이터 (D2). `stock` 만 가정입니다 — 리스팅이 대수를 주지 않습니다.

    `fuel_efficiency` 는 `combined_mpg` 그대로입니다. fueleconomy 가 전기차도 MPGe
    로 주기 때문입니다. kWh/100mi 가 필요하면 33.7kWh/gal 로 역산합니다.
    """
    stock = fleet_units.groupby("vehicle_model_id").size().rename("stock")
    master = vehicle_master.merge(stock, on="vehicle_model_id", how="left")
    if master["stock"].isna().any():
        raise ValueError("대장에 있는데 재고로 펼쳐지지 않은 차종이 있습니다")
    master = master.sort_values("vehicle_model_id").reset_index(drop=True)
    return pd.DataFrame({
        "vehicle_model_id": master["vehicle_model_id"],
        "manufacturer": master["make_key"],
        "model_name": master["model_key"],
        "model_year": master["model_year"].astype("int16"),
        "fuel_type": master["fuel_type"],
        "fuel_efficiency": master["combined_mpg"].astype("float64"),
        "comfort_eligible": master["uber_comfort_eligible"].astype(bool),
        "extra_comfort_eligible": master["lyft_extra_comfort_eligible"].astype(bool),
        "weekly_lease_fee": master["weekly_price_usd"].astype("float64"),
        "image_url": master["image_url"],
        "stock": master["stock"].astype("int32"),
    })


def publish(
    *,
    attributed: pd.DataFrame,
    current: pd.DataFrame,
    fleet_units: pd.DataFrame,
    vehicle_master: pd.DataFrame,
    output_dir: Path,
    run: RunContext,
    input_scope: str,
) -> Path:
    """세 데이터셋 + manifest 를 staging 에 쓰고 디렉터리 rename 으로 공개합니다.

    rename 이 원자적이라, 중간에 죽어도 반쯤 쓰인 파티션이 하류에 보이지 않습니다.
    """
    final = output_dir / f"year_month={run.target_month}"
    manifest_path = final / "manifest.json"
    stale = None
    if final.exists():
        if not manifest_path.is_file():
            raise ValueError(f"완료되지 않은 릴리스가 남아 있습니다: {final}")
        existing = json.loads(manifest_path.read_text(encoding="utf-8"))
        # 재사용은 **계보와 입력 범위가 둘 다 같을 때만** 합니다.
        #
        # `run_id`(= `{월}_{config_hash}`) 만으로는 부족합니다. 읽은 트립 범위
        # (part_limit)는 config 가 아니라 실행 인자라 해시에 들어가지 않습니다.
        # 그래서 part_limit=1 로 한 번 만든 뒤 part_limit=0 으로 돌리면 계산은 다
        # 하고도 **옛 파일을 그대로 두고** 성공으로 끝납니다. 실제로 그 사고가
        # 났습니다 — 리포트는 369,427건인데 파일은 76,484행.
        # `schema_version` 까지 봅니다. 계약이 바뀐 뒤 같은 달을 다시 돌리면
        # run_id·input_scope 는 그대로라 옛 스키마 파일이 조용히 남습니다.
        if (
            existing.get("run_id") == run.run_id
            and existing.get("input_scope") == input_scope
            and existing.get("schema_version") == SCHEMA_VERSION
        ):
            return final
        # 다르면 옛 릴리스는 무효입니다. 거부하지 않고 **교체**합니다 — 막아야 할
        # 사고는 "요청과 다른 파일이 남는 것"이고, 거부는 그것을 막는 대신 4분치
        # 계산을 버리고 손으로 `rm -rf` 를 하게 만듭니다(그 사이 파일은 여전히 옛
        # 것입니다). 조용히 덮지 않으려고 무엇을 교체하는지 아래에서 찍습니다.
        stale = existing

    output_dir.mkdir(parents=True, exist_ok=True)
    staging = output_dir / f".{run.target_month}.staging-{uuid.uuid4().hex}"
    try:
        staging.mkdir()
        datasets = {
            "published_trip_snapshot": (
                build_trip_snapshot(attributed),
                contract.trip_schema(),
            ),
            "published_driver_vehicle": (
                build_driver_vehicle(
                    current, fleet_units,
                    target_month=run.target_month,
                    created_at=run.created_at,
                    global_seed=run.config.global_seed,
                ),
                contract.driver_vehicle_schema(),
            ),
            "published_vehicle_master": (
                build_vehicle_inventory(vehicle_master, fleet_units),
                contract.vehicle_inventory_schema(),
            ),
        }
        entries = {}
        for name, (frame, schema) in datasets.items():
            path = staging / f"{name}.parquet"
            pq.write_table(_as_table(frame, schema), path)
            log(
                f"published: {name} {len(frame):,}행 · "
                f"{path.stat().st_size / 1_048_576:.1f}MB (sha256 계산 중)"
            )
            entries[name] = {
                "file": path.name,
                "row_count": pq.ParquetFile(path).metadata.num_rows,
                "sha256": _sha256(path),
                "schema_version": SCHEMA_VERSION,
            }
        manifest = {
            "year_month": run.target_month,
            "run_id": run.run_id,
            "config_hash": run.config_hash,
            "input_scope": input_scope,
            "seed": run.config.global_seed,
            "created_at": run.created_at,
            "schema_version": SCHEMA_VERSION,
            "provenance": {
                "published_trip_snapshot": "real_facts+synthetic_identity",
                "published_driver_vehicle": "synthetic",
                "published_vehicle_master": "real",
            },
            "datasets": entries,
        }
        (staging / "manifest.json").write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True, default=str),
            encoding="utf-8",
        )
        if stale is None:
            staging.rename(final)
            return final
        log(
            f"published: 기존 릴리스를 교체합니다 — "
            f"run_id {stale.get('run_id')!r} -> {run.run_id!r}, "
            f"input_scope {stale.get('input_scope')!r} -> {input_scope!r}, "
            f"trip_snapshot {stale.get('datasets', {}).get('published_trip_snapshot', {}).get('row_count')} "
            f"-> {entries['published_trip_snapshot']['row_count']}행"
        )
        # 교체도 rename 두 번으로 합니다. 먼저 지우면 그 사이에 죽었을 때 릴리스가
        # 아예 없는 상태가 남습니다.
        replaced = output_dir / f".{run.target_month}.replaced-{uuid.uuid4().hex}"
        final.rename(replaced)
        try:
            staging.rename(final)
        except BaseException:
            replaced.rename(final)  # 옛 릴리스를 되돌려 놓고 실패합니다
            raise
        shutil.rmtree(replaced, ignore_errors=True)
        return final
    finally:
        shutil.rmtree(staging, ignore_errors=True)
