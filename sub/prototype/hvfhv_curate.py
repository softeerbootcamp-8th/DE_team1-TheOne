"""raw HVFHV(TLC 원본 컬럼)를 curated_hvfhv_trip 계약으로 정제합니다 (#620).

`sub`가 자체 수집한 원천(`data/source/synthetic_driver_trip_inputs/hvfhv/`)만
읽습니다 — main 파이프라인의 Silver(`data/silver/hvfhv`)에 의존하지 않습니다.

등급 판정(`estimated_service_tier`)과 구역 조인은
`shared/spark/hvfhv_clean_transformer.py`와 같은 기준(같은 상수·같은 조건)을
씁니다 — 다른 결과가 나오면 안 됩니다. `trip_key`는 같은 방식(자연키 + 발생
순번을 해시)이지만 Spark의 JSON 직렬화와 바이트가 같지는 않습니다 — 이
프로토타입 안에서만 유일하면 되고, main과 값을 맞대볼 일이 없습니다.

    python -m sub.prototype.hvfhv_curate --target_month 2026-01
"""

from __future__ import annotations

import argparse
import hashlib
import shutil
import uuid
from pathlib import Path

import pandas as pd

from sub.prototype import log, paths

RAW_COLUMNS = [
    "hvfhs_license_num", "on_scene_datetime", "pickup_datetime", "dropoff_datetime",
    "PULocationID", "DOLocationID", "trip_miles", "trip_time",
    "base_passenger_fare", "tolls", "sales_tax", "congestion_surcharge",
    "airport_fee", "tips", "driver_pay",
]

# 자연키. `hvfhv_clean_transformer.py::TRIP_KEY_COLUMNS`와 같은 9개입니다.
TRIP_KEY_COLUMNS = [
    "hvfhs_license_num", "pickup_datetime", "dropoff_datetime",
    "PULocationID", "DOLocationID", "trip_miles", "trip_time",
    "base_passenger_fare", "driver_pay",
]

LICENSE_TO_PLATFORM = {
    "HV0002": "Juno", "HV0003": "Uber", "HV0004": "Via", "HV0005": "Lyft",
}

PREMIUM_FARE_RATIO = 1.15
MIN_OD_OBSERVATIONS = 20
ERROR_THRESHOLD = 0.05
NATURAL_KEY_COLLISION_RATIO_LIMIT = 0.05

# part 파일 하나당 행 수. `curated.py`가 이미 이 크기 기준으로(약 49만 행 =
# part 1개) 설계돼 있어 그 값을 그대로 맞춥니다.
ROWS_PER_PART = 490_000


def _valid_mask(raw: pd.DataFrame) -> pd.Series:
    """`hvfhv_clean_transformer.py::transform`의 `valid_condition`과 같은 조건."""
    return (
        raw["pickup_datetime"].notna()
        & raw["dropoff_datetime"].notna()
        & (raw["pickup_datetime"] < raw["dropoff_datetime"])
        & raw["PULocationID"].notna()
        & raw["DOLocationID"].notna()
        & raw["trip_miles"].between(0, 1000, inclusive="right")
        & raw["trip_time"].between(0, 86400, inclusive="right")
        & raw["base_passenger_fare"].between(0, 5000, inclusive="both")
        & raw["driver_pay"].between(0, 5000, inclusive="both")
    )


def _trip_key(trips: pd.DataFrame, occurrence: pd.Series) -> pd.Series:
    """자연키 + 발생 순번을 sha256으로. 순번은 완전 동일한 자연키 중복만 가릅니다."""
    parts = [trips[col].astype(str) for col in TRIP_KEY_COLUMNS]
    parts.append(occurrence.astype(str))
    canonical = parts[0].str.cat(parts[1:], sep="|")
    return canonical.map(lambda s: hashlib.sha256(s.encode()).hexdigest())


def _estimated_service_tier(trips: pd.DataFrame) -> pd.Series:
    """플랫폼·OD쌍별 기본 운임 중앙값 대비 할증 여부로 등급을 추정.

    TLC 원천에는 실제 상품 등급이 없어 관측값이 아니라 추정값입니다.
    `percentile_approx(0.5)`(Spark) 대신 정확한 중앙값을 쓰지만 판정 기준
    (관측 20건 이상 + 중앙값의 1.15배 이상)은 같습니다.
    """
    grouped = trips.groupby(["platform_name", "PULocationID", "DOLocationID"])["base_passenger_fare"]
    obs_count = grouped.transform("size")
    od_median = grouped.transform("median")
    premium = (obs_count >= MIN_OD_OBSERVATIONS) & (
        trips["base_passenger_fare"] >= od_median * PREMIUM_FARE_RATIO
    )
    tier = pd.Series("Standard", index=trips.index, dtype="object")
    tier[premium & (trips["platform_name"] == "Uber")] = "Comfort"
    tier[premium & (trips["platform_name"] == "Lyft")] = "Extra Comfort"
    return tier


def _join_zones(trips: pd.DataFrame, zone_lookup_path: Path) -> pd.DataFrame:
    zones = pd.read_csv(zone_lookup_path)
    pickup = zones.rename(columns={
        "LocationID": "PULocationID", "Borough": "pickup_borough",
        "Zone": "pickup_zone", "service_zone": "pickup_service_zone",
    })[["PULocationID", "pickup_borough", "pickup_zone", "pickup_service_zone"]]
    dropoff = zones.rename(columns={
        "LocationID": "DOLocationID", "Borough": "dropoff_borough",
        "Zone": "dropoff_zone", "service_zone": "dropoff_service_zone",
    })[["DOLocationID", "dropoff_borough", "dropoff_zone", "dropoff_service_zone"]]
    trips = trips.merge(pickup, on="PULocationID", how="left")
    trips = trips.merge(dropoff, on="DOLocationID", how="left")
    return trips


def curate_month(
    raw_path: str | Path, *, zone_lookup_path: str | Path, error_threshold: float = ERROR_THRESHOLD
) -> pd.DataFrame:
    """raw 파케이 하나를 curated 트립 프레임으로. `curated.py::TRIP_COLUMNS`가 읽는 형태."""
    log(f"curate: raw 로딩 {raw_path}")
    raw = pd.read_parquet(raw_path, columns=RAW_COLUMNS)
    total = len(raw)
    mask = _valid_mask(raw)
    valid_count = int(mask.sum())
    invalid_count = total - valid_count
    log(f"curate: 정상 {valid_count:,}건 / 불합격 {invalid_count:,}건")
    if total and invalid_count / total >= error_threshold:
        raise ValueError(
            f"불합격 비율이 {invalid_count / total:.2%}로 임계치"
            f"({error_threshold:.2%})를 초과했습니다."
        )

    trips = raw.loc[mask].reset_index(drop=True)
    occurrence = trips.groupby(TRIP_KEY_COLUMNS, dropna=False).cumcount() + 1
    collided_count = int((occurrence > 1).sum())
    if valid_count and collided_count / valid_count >= NATURAL_KEY_COLLISION_RATIO_LIMIT:
        raise ValueError(
            f"자연키 충돌 비율이 {collided_count / valid_count:.2%}로 임계치"
            f"({NATURAL_KEY_COLLISION_RATIO_LIMIT:.2%})를 초과했습니다. "
            "같은 기간을 중복 적재했는지 확인하세요."
        )

    trips["trip_key"] = _trip_key(trips, occurrence)
    trips["platform_name"] = trips["hvfhs_license_num"].map(LICENSE_TO_PLATFORM).fillna("Unknown")
    trips["estimated_service_tier"] = _estimated_service_tier(trips)
    trips = _join_zones(trips, Path(zone_lookup_path))
    return trips.drop(columns="hvfhs_license_num")


def curate_and_write(
    target_month: str,
    *,
    raw_dir: str | Path | None = None,
    zone_lookup_path: str | Path | None = None,
    output_dir: str | Path | None = None,
    rows_per_part: int = ROWS_PER_PART,
) -> Path:
    """정제 후 `output_dir/year_month={target_month}/part-*.parquet`로 원자적으로 공개."""
    raw_dir = Path(raw_dir) if raw_dir else paths.RAW_HVFHV_DIR
    zone_lookup_path = Path(zone_lookup_path) if zone_lookup_path else paths.ZONE_LOOKUP
    output_dir = Path(output_dir) if output_dir else paths.CURATED_TRIP_DIR

    raw_path = raw_dir / f"year_month={target_month}" / "hvfhv.parquet"
    if not raw_path.is_file():
        raise FileNotFoundError(
            f"원천 HVFHV가 없습니다: {raw_path}\n"
            "sub 의 수집 절차(synthetic_driver_trip_source DAG 의 "
            "collect_source_input, 또는 tasks.fetch_tlc_hvfhv)를 먼저 실행하세요."
        )
    trips = curate_month(raw_path, zone_lookup_path=zone_lookup_path)

    output_dir.mkdir(parents=True, exist_ok=True)
    staging = output_dir / f".curating-{target_month}-{uuid.uuid4().hex}"
    staging.mkdir(parents=True)
    try:
        part_count = 0
        for part_count, start in enumerate(range(0, len(trips), rows_per_part)):
            chunk = trips.iloc[start:start + rows_per_part]
            chunk.to_parquet(staging / f"part-{part_count:05d}.parquet", index=False)
        part_count += 1
        partition = output_dir / f"year_month={target_month}"
        staging.rename(partition)
    finally:
        shutil.rmtree(staging, ignore_errors=True)
    log(f"curate: {len(trips):,}행 -> {partition} ({part_count}개 part)")
    return partition


def main(argv: list[str] | None = None) -> Path:
    parser = argparse.ArgumentParser(description="raw HVFHV를 curated_hvfhv_trip으로 정제")
    parser.add_argument("--target_month", required=True, help="YYYY-MM")
    args = parser.parse_args(argv)
    partition = curate_and_write(args.target_month)
    print(f"curated: {partition}")
    return partition


if __name__ == "__main__":
    main()
