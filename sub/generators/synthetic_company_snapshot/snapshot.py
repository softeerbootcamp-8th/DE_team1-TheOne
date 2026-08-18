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

# 스냅샷 시점(`snapshot_date`)은 이제 `config/generation.json` 의
# `bootstrap.snapshot_date` 가 소유합니다. 실행일(`date.today()`)을 쓰지 않는 이유는
# 그 파일과 `config/README.md` 에 적어 두었습니다 — 요약하면 이건 수집한 데이터가
# 아니라 회사 DB 를 대신해 **생성한 픽스처**이고, 픽스처는 고정되어야 합니다.
#
# 리스 시작 하한은 config 로 올리지 않았습니다. 바꿔가며 돌려볼 손잡이가 아니라
# "리스 이력이 언제부터 있다고 볼 것인가" 라는 픽스처의 기준점이고, 분류상
# 가정 파라미터입니다(`docs/config_inventory.md`). 이름에서 `DEFAULT_` 를 뺀 것은
# 이게 폴백이 아니라 **유일한 소유자**라는 뜻입니다 — 시그니처 기본값으로는 두지
# 않고 진입점이 명시적으로 넘깁니다.
LEASE_START_MIN = date(2023, 1, 1)
# 초기 기사단이 리스하는 차량의 연식. `LEASE_START_MIN` 과 같은 이유로 config 가
# 아니라 이 상수 한 곳이 소유합니다.
MODEL_YEAR = 2023

# 회사 원천 스냅샷의 저장 스키마.
#
# pandas 가 추론하게 두면 안 됩니다. 초기 스냅샷은 모든 계약이 진행 중이라
# `lease_ended_on` 이 전량 결측이고, 그러면 Parquet 타입이 `null` 로 굳어
# Spark 가 날짜로 못 읽습니다 — 기사 배정이 분석 단계에서 죽습니다(#353).
#
# 더 나쁜 건 타입이 스냅샷마다 달라진다는 점입니다. `evolve_company_snapshot` 이
# 일부 계약을 종료시키면 그때는 값이 생겨 날짜로 추론됩니다. 소비하는 쪽은
# 어느 달 스냅샷을 읽느냐에 따라 되기도 하고 안 되기도 합니다.
SCHEMAS = {
    "customer": pa.schema(
        [
            ("customer_id", pa.string()),
            ("synthetic_driver_id", pa.string()),
            ("snapshot_date", pa.date32()),
        ]
    ),
    "taxi": pa.schema(
        [
            ("taxi_id", pa.string()),
            ("make_key", pa.string()),
            ("model_key", pa.string()),
            ("model_year", pa.int64()),
            ("weekly_price_usd", pa.float64()),
            ("uber_comfort_eligible", pa.bool_()),
            ("lyft_extra_comfort_eligible", pa.bool_()),
            ("vehicle_group", pa.string()),
            ("snapshot_date", pa.date32()),
        ]
    ),
    "lease_contract": pa.schema(
        [
            ("lease_id", pa.string()),
            ("customer_id", pa.string()),
            ("taxi_id", pa.string()),
            ("lease_started_on", pa.date32()),
            # 진행 중이면 결측입니다. 전량 결측이어도 날짜여야 합니다.
            ("lease_ended_on", pa.date32()),
            ("snapshot_date", pa.date32()),
        ]
    ),
}

DRIVER_ID_PREFIX = "SD"
# 초기 기사단의 차량 자격 구성. 총원은 `config/generation.json` 의
# `driver.initial_count` 가 소유하고, 여기는 **구성비**만 소유합니다.
GROUP_COUNTS = {"BOTH": 400, "STANDARD": 1_200, "SINGLE": 400}
# 총원으로 나누지 않고 자기 합으로 정규화합니다. 총원과 비율을 분리해 두면 활성
# 기사 수가 달마다 변동하게 되는 후속 lifecycle 작업에서 이 코드를 다시 건드릴
# 필요가 없습니다 — 신규 기사의 그룹 추첨은 언제나 이 비율을 씁니다.
_GROUP_TOTAL = sum(GROUP_COUNTS.values())
GROUP_WEIGHTS = {group: count / _GROUP_TOTAL for group, count in GROUP_COUNTS.items()}
MIN_MONTHLY_CHANGE_RATE = 0.005
MAX_MONTHLY_CHANGE_RATE = 0.01
ID_NAMESPACE = uuid.UUID("f795ec33-9231-5f39-aade-fdf81c34bf62")


@dataclass(frozen=True)
class SnapshotTables:
    customer: pd.DataFrame
    taxi: pd.DataFrame
    lease_contract: pd.DataFrame


def build_driver_ids(driver_count: int) -> list[str]:
    """가상 기사 ID 목록. 자리수 고정이라 정렬 순서와 생성 순서가 같습니다."""
    if driver_count < 1:
        raise ValueError(f"가상 기사는 1명 이상이어야 합니다: {driver_count:,}명")
    width = len(str(driver_count - 1))
    return [f"{DRIVER_ID_PREFIX}{index:0{width}d}" for index in range(driver_count)]


def build_vehicle_pool(
    vehicle_master: pd.DataFrame,
    model_year: int,
) -> pd.DataFrame:
    """리스 업체 차량 마스터(플랫폼·상품 한 행씩)를 차종 한 행으로 접습니다."""
    key = ["make_key", "model_key"]
    required = {*key, "vendor", "platform", "product", "min_year", "weekly_price_usd"}
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

    pool = vehicle_master[[*key, "weekly_price_usd"]].drop_duplicates(key).copy()
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
    seed: int,
    snapshot_date: date,
    lease_start_min: date,
) -> SnapshotTables:
    if not driver_ids or len(set(driver_ids)) != len(driver_ids):
        raise ValueError(f"중복 없는 가상 기사 1명 이상이 필요합니다: {len(driver_ids)}명")
    # 총원은 config(`driver.initial_count`), 구성비는 `GROUP_COUNTS` 가 소유합니다.
    # 소유자가 둘이라 둘이 어긋날 수 있고, 어긋나면 아래 `zip(strict=True)` 가
    # 원인을 알기 어려운 메시지로 죽습니다. 두 출처를 함께 지목하고 먼저 멈춥니다.
    if len(driver_ids) != _GROUP_TOTAL:
        raise ValueError(
            f"기사 수와 그룹 구성이 어긋납니다: driver.initial_count={len(driver_ids)}, "
            f"GROUP_COUNTS 합={_GROUP_TOTAL} ({GROUP_COUNTS}). "
            "config/generation.json 의 driver.initial_count 또는 snapshot.py 의 "
            "GROUP_COUNTS 를 맞추세요."
        )
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
    seed: int,
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
