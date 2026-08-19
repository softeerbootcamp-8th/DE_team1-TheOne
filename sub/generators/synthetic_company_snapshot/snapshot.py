"""가상 운행 기사와 차량 기준정보로 회사 원천 DB 스냅샷을 합성합니다."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from schema.source import COMPANY_SNAPSHOT_SCHEMAS

# 기본 스냅샷 시점.
#
# 실행일(`date.today()`)을 쓰지 않는 이유
# ------------------------------------
# 이건 수집한 데이터가 아니라 회사 DB 를 대신해 **생성한 픽스처**입니다. 픽스처는
# 고정되어야 합니다.
#
#  - 내용이 이 날짜에 의존합니다 — 리스 시작일이 `[lease_start_min, snapshot_date]`
#    범위에서 뽑힙니다. 실행일이 기본이면 팀원마다 다른 데이터가 나와 결과를
#    비교할 수 없습니다.
#  - `seed` 를 고정해 둔 의도와 모순됩니다. 시드로 재현성을 확보하면서 날짜를
#    움직이면 그 의도가 깨집니다.
#  - 안내 문서와 DAG 실행 예시가 이 경로를 참조합니다.
#
# 값은 임의로 고른 상수입니다. 실제 사건이 아니라 **우리가 정한 값**으로 읽히도록
# 딱 떨어지는 날짜를 씁니다. 바꿀 때는 여기만 고치면 되고, 테스트도 이 상수를
# 참조하므로 리터럴을 따라다닐 필요가 없습니다.
DEFAULT_SNAPSHOT_DATE = date(2026, 1, 1)
DEFAULT_LEASE_START_MIN = date(2023, 1, 1)

# 회사 원천 스냅샷 저장 스키마는 `schema/source` 가 소유합니다.
SCHEMAS = COMPANY_SNAPSHOT_SCHEMAS


DRIVER_COUNT = 2_000
DRIVER_ID_PREFIX = "SD"
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


def build_driver_ids(driver_count: int = DRIVER_COUNT) -> list[str]:
    """가상 기사 ID 목록. 자리수 고정이라 정렬 순서와 생성 순서가 같습니다."""
    if driver_count < 1:
        raise ValueError(f"가상 기사는 1명 이상이어야 합니다: {driver_count:,}명")
    width = len(str(driver_count - 1))
    return [f"{DRIVER_ID_PREFIX}{index:0{width}d}" for index in range(driver_count)]


def build_vehicle_pool(
    vehicle_master: pd.DataFrame,
    model_year: int = 2023,
) -> pd.DataFrame:
    """리스 업체 차량 마스터(플랫폼·상품 한 행씩)를 차종 한 행으로 접습니다."""
    key = ["make_key", "model_key"]
    required = {*key, "vendor", "platform", "product", "min_year", "weekly_lease_fee"}
    missing = required - set(vehicle_master.columns)
    if missing:
        raise ValueError(f"vehicle master 필수 컬럼 누락: {sorted(missing)}")
    # 업체가 여럿이면 같은 차종이 업체별로 다른 주간요금을 갖는데, 아래 dedup 이
    # 그중 하나를 조용히 고르게 됩니다. 업체가 늘어나면 여기서 먼저 멈추게 둡니다.
    # ponytail: 단일 업체 가정. 업체가 늘면 vendor 를 key 에 넣고 taxi 테이블까지 확장
    vendors = sorted(vehicle_master["vendor"].dropna().unique())
    if len(vendors) > 1:
        raise ValueError(f"차량 마스터에 업체가 둘 이상입니다: {vendors}")

    def _keys_for(platform: str, product: str) -> set[tuple]:
        matched = vehicle_master.loc[
            (vehicle_master["platform"] == platform)
            & (vehicle_master["product"] == product)
            & (vehicle_master["min_year"] <= model_year), key
        ]
        return set(map(tuple, matched.values))

    uber_comfort = _keys_for("uber", "Comfort")
    lyft_extra_comfort = _keys_for("lyft", "Extra Comfort")

    pool = vehicle_master[[*key, "weekly_lease_fee"]].drop_duplicates(key).copy()
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
    snapshot_date: date = DEFAULT_SNAPSHOT_DATE,
    lease_start_min: date = DEFAULT_LEASE_START_MIN,
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
            "weekly_lease_fee": float(vehicle["weekly_lease_fee"]),
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
            "weekly_lease_fee": float(vehicle["weekly_lease_fee"]),
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
        frame = getattr(tables, name)
        schema = SCHEMAS[name]
        missing = set(schema.names) - set(frame.columns)
        if missing:
            raise ValueError(f"{name} 에 컬럼이 없습니다: {sorted(missing)}")
        # 스키마를 넘겨 pandas 추론을 막습니다. 컬럼 순서도 여기서 고정됩니다.
        table = pa.Table.from_pandas(
            frame[schema.names], schema=schema, preserve_index=False
        )
        pq.write_table(table, path)
        paths.append(path)
    return paths
