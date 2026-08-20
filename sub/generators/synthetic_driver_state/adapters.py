"""신규 event-sourced 상태 모델과 기존 legacy 스냅샷 사이의 뷰 변환.

기존 Spark 경로(`candidates.py`/`allocator.py`)는 `customer`/`taxi`/`lease_contract`/
`driver_preferences` 스키마를 그대로 소비하므로 바꾸지 않는다(asistobe.md Phase C).
이 모듈이 신규 상태모델(`lifecycle.synthesize_month`)의 산출물을 그 스키마로
바꾸는 유일한 통로다 — 정본은 event-sourced 상태이고, legacy 스냅샷은 그 뷰다.
"""

from __future__ import annotations

import pandas as pd

from sub.generators.synthetic_company_snapshot.snapshot import SnapshotTables
from sub.spark.jobs.driver_master.preference import PREFERENCE_COLUMNS
from sub.spark.jobs.driver_master.traits import TIME_BLOCK_LABELS, WEEKDAY_LABELS

CUSTOMER_ID_PREFIX = "CUST"
LEASE_ID_PREFIX = "LEASE"


def vehicle_master_with_model_id(vehicle_pool: pd.DataFrame) -> pd.DataFrame:
    """`synthetic_company_snapshot.build_vehicle_pool()` 산출물에 `vehicle_model_id`
    를 붙입니다.

    `sub/generators/synthetic_driver_state/fleet.py`(모델별 재고 확장)와
    `taxi_id` 파싱(`f"{vehicle_model_id}#{serial}"`)이 이 컬럼을 전제합니다.
    `sub/prototype/curated.py` 와 같은 조합 규칙입니다 — 두 곳에서 다시 정의하면
    갈립니다.
    """
    if "vehicle_model_id" in vehicle_pool.columns:
        return vehicle_pool
    pool = vehicle_pool.copy()
    pool["vehicle_model_id"] = (
        pool["make_key"] + "|" + pool["model_key"] + "|" + pool["model_year"].astype(str)
    )
    return pool


def _parse_vehicle_model_id(taxi_id: str) -> str:
    """`f"{vehicle_model_id}#{serial:05d}"` 에서 모델 부분만 되돌립니다."""
    return str(taxi_id).rsplit("#", 1)[0]


def to_snapshot_tables(
    current: pd.DataFrame, vehicle_pool: pd.DataFrame, *, snapshot_date
) -> SnapshotTables:
    """`driver_vehicle_current` 를 legacy `customer`/`taxi`/`lease_contract` 뷰로 바꿉니다.

    D15 와 같은 규칙입니다 — 퇴사 기사도 행을 남기고 `lease_ended_on` 만 채웁니다.
    `customer_id`/`lease_id`는 `driver_id` 에서 결정적으로 파생합니다 — legacy
    스키마가 문자열이기만 하면 되므로 uuid5 를 다시 만들 이유가 없습니다.
    """
    if current.empty:
        raise ValueError("current 가 비어 있습니다")
    pool = vehicle_master_with_model_id(vehicle_pool)
    by_model = pool.drop_duplicates("vehicle_model_id").set_index("vehicle_model_id")

    rows = current.copy()
    rows["customer_id"] = CUSTOMER_ID_PREFIX + "_" + rows["driver_id"].astype(str)
    rows["lease_id"] = LEASE_ID_PREFIX + "_" + rows["driver_id"].astype(str)
    rows["_vehicle_model_id"] = rows["taxi_id"].astype(str).map(_parse_vehicle_model_id)
    unknown = sorted(set(rows["_vehicle_model_id"]) - set(by_model.index))
    if unknown:
        raise ValueError(f"vehicle_pool 에 없는 차종입니다: {unknown}")

    customer = pd.DataFrame({
        "customer_id": rows["customer_id"],
        "synthetic_driver_id": rows["driver_id"],
        "snapshot_date": snapshot_date,
    })

    joined = rows.join(by_model, on="_vehicle_model_id")
    taxi = (
        joined[[
            "taxi_id", "make_key", "model_key", "model_year", "weekly_lease_fee",
            "uber_comfort_eligible", "lyft_extra_comfort_eligible", "vehicle_group",
        ]]
        .drop_duplicates("taxi_id")
        .assign(snapshot_date=snapshot_date)
    )

    lease = pd.DataFrame({
        "lease_id": rows["lease_id"],
        "customer_id": rows["customer_id"],
        "taxi_id": rows["taxi_id"],
        "lease_started_on": pd.to_datetime(rows["vehicle_since"]).dt.date,
        "lease_ended_on": pd.to_datetime(rows["exited_on"]).dt.date,
        "snapshot_date": snapshot_date,
    })

    return SnapshotTables(
        customer=customer.reset_index(drop=True),
        taxi=taxi.reset_index(drop=True),
        lease_contract=lease.reset_index(drop=True),
    )


def to_driver_preferences(profiles: pd.DataFrame) -> pd.DataFrame:
    """`profiles`(`synthesize_month` 산출물)를 legacy `driver_preferences` 뷰로 바꿉니다.

    이름이 다른 필드만 명시적으로 맞춥니다(asistobe.md 8.3) — `distance_pref_mi`
    와 `preferred_distance_miles` 를 두 모듈에 중복 생성하지 않습니다.

    `target_daily_trips` 는 `candidates.py`/`allocator.py` 가 더 이상 읽지 않는
    필드입니다(`preference.py` 의 같은 주석 참고) — 참고용 근사치만 채웁니다.
    """
    target_daily_trips = (
        profiles["target_drive_minutes"] / profiles["avg_trip_duration_min"]
    ).round().astype(int).clip(lower=profiles["min_daily_trips"], upper=profiles["max_daily_trips"])

    out = pd.DataFrame({
        "driver_id": profiles["driver_id"],
        "active_weekdays": profiles["active_weekdays"].apply(
            lambda days: [WEEKDAY_LABELS[d] for d in days]
        ),
        "preferred_time_blocks": profiles["preferred_time_blocks"].apply(
            lambda blocks: [TIME_BLOCK_LABELS[b] for b in blocks]
        ),
        "time_block_weights": profiles["time_block_weights"],
        "preferred_distance_band": profiles["preferred_distance_band"],
        "preferred_distance_miles": profiles["distance_pref_mi"],
        "airport_preference": profiles["airport_preference"],
        "manhattan_preference": profiles["manhattan_preference"],
        "tier_preference": profiles["tier_preference"],
        "target_daily_trips": target_daily_trips,
        "min_daily_trips": profiles["min_daily_trips"],
        "max_daily_trips": profiles["max_daily_trips"],
        "target_work_minutes": profiles["target_work_minutes"],
        "target_drive_minutes": profiles["target_drive_minutes"],
        "idle_frac": profiles["idle_frac"],
        "max_deadhead_minutes": profiles["max_deadhead_minutes"],
        "buffer_seconds": profiles["buffer_seconds"],
    })
    return out[PREFERENCE_COLUMNS].sort_values("driver_id").reset_index(drop=True)
