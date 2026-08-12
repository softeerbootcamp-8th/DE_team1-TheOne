"""가상 운행 기사와 차량 기준정보로 회사 원천 DB 스냅샷을 합성합니다."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

DRIVER_COUNT = 2_000
GROUP_COUNTS = {"BOTH": 400, "STANDARD": 1_200, "SINGLE": 400}
ID_NAMESPACE = uuid.UUID("f795ec33-9231-5f39-aade-fdf81c34bf62")


@dataclass(frozen=True)
class SnapshotTables:
    customer: pd.DataFrame
    taxi: pd.DataFrame
    lease_contract: pd.DataFrame


def driver_ids_from_mapping(mapping: pd.DataFrame) -> list[str]:
    if "synthetic_driver_id" not in mapping.columns:
        raise ValueError("mapping에 synthetic_driver_id 컬럼이 없습니다")
    driver_ids = sorted(mapping["synthetic_driver_id"].dropna().astype(str).unique())
    if len(driver_ids) != DRIVER_COUNT:
        raise ValueError(f"가상 기사는 {DRIVER_COUNT:,}명이어야 합니다: {len(driver_ids):,}명")
    return driver_ids


def build_vehicle_pool(
    catalog: pd.DataFrame,
    uber_eligibility: pd.DataFrame,
    lyft_eligibility: pd.DataFrame,
    model_year: int = 2023,
) -> pd.DataFrame:
    key = ["make_key", "model_key"]
    catalog_columns = [*key, "weekly_price_usd"]
    missing_catalog = set(catalog_columns) - set(catalog.columns)
    eligibility_columns = {*key, "product", "min_year"}
    missing_eligibility = eligibility_columns - set(uber_eligibility.columns)
    missing_eligibility |= eligibility_columns - set(lyft_eligibility.columns)
    if missing_catalog:
        raise ValueError(f"vehicle catalog 필수 컬럼 누락: {sorted(missing_catalog)}")
    if missing_eligibility:
        raise ValueError(f"vehicle eligibility 필수 컬럼 누락: {sorted(missing_eligibility)}")

    uber_comfort = set(
        map(tuple, uber_eligibility.loc[
            (uber_eligibility["product"] == "Comfort")
            & (uber_eligibility["min_year"] <= model_year), key
        ].values)
    )
    lyft_extra_comfort = set(
        map(tuple, lyft_eligibility.loc[
            (lyft_eligibility["product"] == "Extra Comfort")
            & (lyft_eligibility["min_year"] <= model_year), key
        ].values)
    )

    pool = catalog[catalog_columns].drop_duplicates(key).copy()
    identities = list(map(tuple, pool[key].values))
    pool["uber_comfort_eligible"] = [identity in uber_comfort for identity in identities]
    pool["lyft_extra_comfort_eligible"] = [identity in lyft_extra_comfort for identity in identities]
    eligibility_count = (
        pool["uber_comfort_eligible"].astype(int)
        + pool["lyft_extra_comfort_eligible"].astype(int)
    )
    pool["vehicle_group"] = np.select(
        [eligibility_count == 2, eligibility_count == 1],
        ["BOTH", "SINGLE"],
        default="STANDARD",
    )
    pool["model_year"] = model_year
    missing_groups = set(GROUP_COUNTS) - set(pool["vehicle_group"])
    if missing_groups:
        raise ValueError(f"차량 후보가 없는 그룹: {sorted(missing_groups)}")
    return pool.sort_values(key).reset_index(drop=True)


def _stable_id(kind: str, seed: int, driver_id: str) -> str:
    return str(uuid.uuid5(ID_NAMESPACE, f"{kind}:{seed}:{driver_id}"))


def build_company_snapshot(
    driver_ids: list[str],
    vehicle_pool: pd.DataFrame,
    *,
    seed: int = 42,
    snapshot_date: date = date(2026, 8, 12),
    lease_start_min: date = date(2023, 1, 1),
) -> SnapshotTables:
    if len(driver_ids) != DRIVER_COUNT or len(set(driver_ids)) != DRIVER_COUNT:
        raise ValueError(f"중복 없는 가상 기사 {DRIVER_COUNT}명이 필요합니다")
    if lease_start_min > snapshot_date:
        raise ValueError("lease_start_min은 snapshot_date보다 늦을 수 없습니다")

    rng = np.random.default_rng(seed)
    shuffled_drivers = np.asarray(sorted(driver_ids), dtype=object)
    rng.shuffle(shuffled_drivers)
    assigned_groups = np.concatenate(
        [np.repeat(group, count) for group, count in GROUP_COUNTS.items()]
    )

    customers: list[dict] = []
    taxis: list[dict] = []
    contracts: list[dict] = []
    lease_day_range = (snapshot_date - lease_start_min).days

    for driver_id, group in zip(shuffled_drivers, assigned_groups, strict=True):
        candidates = vehicle_pool.loc[vehicle_pool["vehicle_group"] == group]
        if candidates.empty:
            raise ValueError(f"차량 후보가 없는 그룹: {group}")
        vehicle = candidates.iloc[int(rng.integers(0, len(candidates)))]
        customer_id = _stable_id("customer", seed, str(driver_id))
        taxi_id = _stable_id("taxi", seed, str(driver_id))
        lease_id = _stable_id("lease", seed, str(driver_id))
        lease_started_on = lease_start_min + timedelta(days=int(rng.integers(0, lease_day_range + 1)))

        customers.append({
            "customer_id": customer_id,
            "synthetic_driver_id": driver_id,
            "snapshot_date": snapshot_date,
        })
        taxis.append({
            "taxi_id": taxi_id,
            "make_key": vehicle["make_key"],
            "model_key": vehicle["model_key"],
            "model_year": int(vehicle["model_year"]),
            "weekly_price_usd": float(vehicle["weekly_price_usd"]),
            "uber_comfort_eligible": bool(vehicle["uber_comfort_eligible"]),
            "lyft_extra_comfort_eligible": bool(vehicle["lyft_extra_comfort_eligible"]),
            "vehicle_group": group,
            "snapshot_date": snapshot_date,
        })
        contracts.append({
            "lease_id": lease_id,
            "customer_id": customer_id,
            "taxi_id": taxi_id,
            "lease_started_on": lease_started_on,
            "lease_ended_on": None,
            "snapshot_date": snapshot_date,
        })

    return SnapshotTables(
        customer=pd.DataFrame(customers).sort_values("customer_id").reset_index(drop=True),
        taxi=pd.DataFrame(taxis).sort_values("taxi_id").reset_index(drop=True),
        lease_contract=pd.DataFrame(contracts).sort_values("lease_id").reset_index(drop=True),
    )


def write_snapshot(tables: SnapshotTables, output_dir: str | Path, snapshot_date: date) -> list[Path]:
    partition = Path(output_dir) / f"snapshot_date={snapshot_date.isoformat()}"
    partition.mkdir(parents=True, exist_ok=True)
    paths = []
    for name in ("customer", "taxi", "lease_contract"):
        path = partition / f"{name}.parquet"
        getattr(tables, name).to_parquet(path, index=False)
        paths.append(path)
    return paths
