"""신규 event-sourced 상태 모델과 기존 legacy 스냅샷 사이의 뷰 변환.

기존 Spark 경로(`candidates.py`/`allocator.py`)는 `customer`/`taxi`/`lease_contract`/
`driver_preferences` 스키마를 그대로 소비하므로 바꾸지 않는다(asistobe.md Phase C).
이 모듈이 신규 상태모델(`lifecycle.synthesize_month`)의 산출물을 그 스키마로
바꾸는 유일한 통로다 — 정본은 event-sourced 상태이고, legacy 스냅샷은 그 뷰다.
"""

from __future__ import annotations

import pandas as pd

from sub.generators.synthetic_company_snapshot.snapshot import SnapshotTables, build_vehicle_pool
from sub.spark.jobs.driver_master.preference import PREFERENCE_COLUMNS

CUSTOMER_ID_PREFIX = "CUST"
LEASE_ID_PREFIX = "LEASE"


def _bitmask(indexes) -> int:
    """정수 인덱스 리스트 -> 비트마스크. `preference.py::_bitmask`와 같은 규칙."""
    return int(sum(1 << int(index) for index in indexes))


def vehicle_pool_from_silver(vehicle_master: pd.DataFrame) -> pd.DataFrame:
    """실측 Silver `vehicle_master.parquet`(vendor·platform·product 행 여러 개)를
    `synthesize_month`이 기대하는 차종 한 행짜리 풀로 바꿉니다.

    `build_vehicle_pool()`이 자격·그룹 판정(같은 조인 키)을 이미 하므로 그대로
    쓰고, 여기서는 그 결과에 빠진 두 값만 채웁니다 — `combined_mpg`/
    `combined_kwh_per_100mi`. 실측은 트림 범위(min/max)인데
    `fleet.py`/`assignment.py`는 단일 값을 기대합니다.

    ponytail: min/max 중앙값. Gold의 트림 선택만큼 정밀하지 않지만, 이 값은
    D5 "정답"(비용 순위)에만 쓰이고 산출물 계약엔 실리지 않습니다. 트림별
    정밀도가 필요해지면 Gold와 같은 트림 선택 규칙을 가져오세요.
    """
    pool = build_vehicle_pool(vehicle_master)
    economy = (
        vehicle_master[[
            "make_key", "model_key",
            "combined_mpg_min", "combined_mpg_max",
            "combined_kwh_per_100mi_min", "combined_kwh_per_100mi_max",
        ]]
        .drop_duplicates(["make_key", "model_key"])
        .copy()
    )
    economy["combined_mpg"] = (economy["combined_mpg_min"] + economy["combined_mpg_max"]) / 2
    economy["combined_kwh_per_100mi"] = (
        (economy["combined_kwh_per_100mi_min"] + economy["combined_kwh_per_100mi_max"]) / 2
    ).fillna(0.0)
    economy = economy[["make_key", "model_key", "combined_mpg", "combined_kwh_per_100mi"]]

    merged = pool.merge(economy, on=["make_key", "model_key"], how="left", validate="one_to_one")
    missing = merged.loc[merged["combined_mpg"].isna(), ["make_key", "model_key"]]
    if not missing.empty:
        raise ValueError(f"제원(mpg)이 없는 차종입니다: {missing.to_dict('records')}")
    merged = merged.rename(columns={"weekly_lease_fee": "weekly_price_usd"})
    return vehicle_master_with_model_id(merged)


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
    # `fleet.py`/`assignment.py` 는 `weekly_price_usd` 를 쓰고(`vehicle_pool_from_silver`
    # 가 그렇게 이름 붙임), legacy `taxi` 스키마는 `weekly_lease_fee` 를 씁니다.
    # 어느 이름으로 들어오든 legacy 쪽으로 맞춥니다.
    if "weekly_price_usd" in pool.columns and "weekly_lease_fee" not in pool.columns:
        pool = pool.rename(columns={"weekly_price_usd": "weekly_lease_fee"})
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


def to_current_driver_vehicle(current: pd.DataFrame, vehicle_pool: pd.DataFrame) -> pd.DataFrame:
    """`driver_vehicle_current` 를 candidates.py 가 읽는 단일 테이블로 바꿉니다 (#643).

    `customer`/`taxi`/`lease_contract` 3-테이블(`to_snapshot_tables`)은
    `source_job.py::build_driver_vehicle_monthly_snapshot()`(이력 기반
    `join_date`/`experience_years` 계산에 필요)가 계속 쓰므로 남겨 둡니다.
    candidates.py는 그 달의 활성 계약 여부와 차량 자격만 있으면 되고, 그 정보는
    이 달 `current` 한 장에 이미 다 있어서 조인 3개를 거칠 이유가 없습니다.

    D15 와 같은 규칙 — 퇴사 기사도 행을 남기고 `lease_ended_on` 만 채웁니다.
    """
    if current.empty:
        raise ValueError("current 가 비어 있습니다")
    pool = vehicle_master_with_model_id(vehicle_pool)
    by_model = pool.drop_duplicates("vehicle_model_id").set_index("vehicle_model_id")

    rows = current.copy()
    rows["_vehicle_model_id"] = rows["taxi_id"].astype(str).map(_parse_vehicle_model_id)
    unknown = sorted(set(rows["_vehicle_model_id"]) - set(by_model.index))
    if unknown:
        raise ValueError(f"vehicle_pool 에 없는 차종입니다: {unknown}")

    joined = rows.join(by_model, on="_vehicle_model_id")
    return pd.DataFrame({
        "driver_id": rows["driver_id"],
        "taxi_id": rows["taxi_id"],
        "lease_started_on": pd.to_datetime(rows["vehicle_since"]).dt.date,
        "lease_ended_on": pd.to_datetime(rows["exited_on"]).dt.date,
        "uber_comfort_eligible": joined["uber_comfort_eligible"].to_numpy(),
        "lyft_extra_comfort_eligible": joined["lyft_extra_comfort_eligible"].to_numpy(),
    }).reset_index(drop=True)


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
        "weekday_mask": profiles["active_weekdays"].apply(_bitmask),
        "time_block_mask": profiles["preferred_time_blocks"].apply(_bitmask),
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
