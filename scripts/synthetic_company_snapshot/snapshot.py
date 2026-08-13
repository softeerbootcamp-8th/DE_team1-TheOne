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
GROUP_WEIGHTS = {group: count / DRIVER_COUNT for group, count in GROUP_COUNTS.items()}
MIN_MONTHLY_CHANGE_RATE = 0.005
MAX_MONTHLY_CHANGE_RATE = 0.01
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


def _validate_previous_snapshot(tables: SnapshotTables) -> date:
    for name in ("customer", "taxi", "lease_contract"):
        table = getattr(tables, name)
        if table.empty:
            raise ValueError(f"전월 {name} 스냅샷이 비어 있습니다")
        if table.iloc[:, 0].duplicated().any():
            raise ValueError(f"전월 {name} 기본 키가 중복됩니다")

    customer_ids = set(tables.customer["customer_id"])
    taxi_ids = set(tables.taxi["taxi_id"])
    if not set(tables.lease_contract["customer_id"]).issubset(customer_ids):
        raise ValueError("전월 계약의 customer_id가 고객 스냅샷에 없습니다")
    if not set(tables.lease_contract["taxi_id"]).issubset(taxi_ids):
        raise ValueError("전월 계약의 taxi_id가 택시 스냅샷에 없습니다")

    active = tables.lease_contract[tables.lease_contract["lease_ended_on"].isna()]
    if active.empty:
        raise ValueError("전월 활성 계약이 없습니다")
    if active["customer_id"].duplicated().any() or active["taxi_id"].duplicated().any():
        raise ValueError("기사 또는 택시에 활성 계약이 여러 건입니다")

    dates = set(pd.to_datetime(tables.lease_contract["snapshot_date"]).dt.date)
    if len(dates) != 1:
        raise ValueError("전월 계약의 snapshot_date가 하나가 아닙니다")
    return dates.pop()


def evolve_company_snapshot(
    previous: SnapshotTables,
    vehicle_pool: pd.DataFrame,
    *,
    snapshot_date: date,
    seed: int = 42,
    change_rate: float | None = None,
) -> SnapshotTables:
    """전월 스냅샷에서 소수 계약을 종료하고 같은 수의 신규 계약을 만듭니다."""
    previous_date = _validate_previous_snapshot(previous)
    if snapshot_date <= previous_date:
        raise ValueError("당월 snapshot_date는 전월 snapshot_date보다 늦어야 합니다")

    rng = np.random.default_rng(seed)
    rate = change_rate if change_rate is not None else rng.uniform(
        MIN_MONTHLY_CHANGE_RATE, MAX_MONTHLY_CHANGE_RATE
    )
    if not MIN_MONTHLY_CHANGE_RATE <= rate <= MAX_MONTHLY_CHANGE_RATE:
        raise ValueError("change_rate는 0.005 이상 0.01 이하여야 합니다")

    customers = previous.customer.copy(deep=True)
    taxis = previous.taxi.copy(deep=True)
    contracts = previous.lease_contract.copy(deep=True)
    active_indexes = contracts.index[contracts["lease_ended_on"].isna()].to_numpy()
    change_count = max(1, int(round(len(active_indexes) * rate)))
    ended_indexes = rng.choice(active_indexes, size=change_count, replace=False)
    contracts.loc[ended_indexes, "lease_ended_on"] = snapshot_date

    existing_driver_ids = set(customers["synthetic_driver_id"].astype(str))
    month_key = snapshot_date.strftime("%Y%m")
    groups = list(GROUP_WEIGHTS)
    probabilities = list(GROUP_WEIGHTS.values())
    new_customers: list[dict] = []
    new_taxis: list[dict] = []
    new_contracts: list[dict] = []

    next_number = 1
    for _ in range(change_count):
        while (driver_id := f"DRIVER_{month_key}_{next_number:06d}") in existing_driver_ids:
            next_number += 1
        next_number += 1
        existing_driver_ids.add(driver_id)

        group = str(rng.choice(groups, p=probabilities))
        candidates = vehicle_pool.loc[vehicle_pool["vehicle_group"] == group]
        if candidates.empty:
            raise ValueError(f"차량 후보가 없는 그룹: {group}")
        vehicle = candidates.iloc[int(rng.integers(0, len(candidates)))]
        customer_id = _stable_id("customer", seed, driver_id)
        taxi_id = _stable_id("taxi", seed, driver_id)
        lease_id = _stable_id("lease", seed, driver_id)

        new_customers.append({
            "customer_id": customer_id,
            "synthetic_driver_id": driver_id,
            "snapshot_date": snapshot_date,
        })
        new_taxis.append({
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
        new_contracts.append({
            "lease_id": lease_id,
            "customer_id": customer_id,
            "taxi_id": taxi_id,
            "lease_started_on": snapshot_date,
            "lease_ended_on": None,
            "snapshot_date": snapshot_date,
        })

    for table in (customers, taxis, contracts):
        table["snapshot_date"] = snapshot_date

    return SnapshotTables(
        customer=pd.concat([customers, pd.DataFrame(new_customers)], ignore_index=True)
        .sort_values("customer_id").reset_index(drop=True),
        taxi=pd.concat([taxis, pd.DataFrame(new_taxis)], ignore_index=True)
        .sort_values("taxi_id").reset_index(drop=True),
        lease_contract=pd.concat([contracts, pd.DataFrame(new_contracts)], ignore_index=True)
        .sort_values("lease_id").reset_index(drop=True),
    )


def read_snapshot(snapshot_dir: str | Path) -> SnapshotTables:
    partition = Path(snapshot_dir)
    return SnapshotTables(**{
        name: pd.read_parquet(partition / f"{name}.parquet")
        for name in ("customer", "taxi", "lease_contract")
    })


def write_snapshot(tables: SnapshotTables, output_dir: str | Path, snapshot_date: date) -> list[Path]:
    partition = Path(output_dir) / f"snapshot_date={snapshot_date.isoformat()}"
    partition.mkdir(parents=True, exist_ok=True)
    paths = []
    for name in ("customer", "taxi", "lease_contract"):
        path = partition / f"{name}.parquet"
        getattr(tables, name).to_parquet(path, index=False)
        paths.append(path)
    return paths
