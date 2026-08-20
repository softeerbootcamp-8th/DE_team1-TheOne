"""신규 event-sourced 상태 모델과 기존 Spark 경로 사이의 뷰 변환.

기존 Spark 경로(`candidates.py`/`allocator.py`/`source_job.py`)는
`current_driver_vehicle`/`driver_preferences` 스키마를 소비한다. 이 모듈이
신규 상태모델(`lifecycle.synthesize_month`)의 산출물을 그 스키마로 바꾸는
유일한 통로다 — 정본은 event-sourced 상태이고, 이 뷰들은 그 파생이다.

`to_current_driver_vehicle()`이 기사당 한 행짜리 유일한 차량 배정 뷰다(#609).
예전에는 이력 기반 계산(join_date/experience_years)용으로 customer/taxi/
lease_contract 3-테이블 뷰(`to_snapshot_tables`)를 따로 뒀지만, 기사당 리스
이력을 한 행으로 재구성하는 과정에서 진짜 입사일을 잃어버리는 조용한 버그가
있었다 — `driver_vehicle_current`가 이미 갖고 있는 `joined_on`을 재구성하지
않고 그대로 흘려보내 없앴다.
"""

from __future__ import annotations

import pandas as pd

from sub.generators.synthetic_company_snapshot.snapshot import build_vehicle_pool
from sub.spark.jobs.driver_master.preference import PREFERENCE_COLUMNS


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


def to_current_driver_vehicle(current: pd.DataFrame, vehicle_pool: pd.DataFrame) -> pd.DataFrame:
    """`driver_vehicle_current` 를 기사·차량 월간 뷰로 바꿉니다 (#643, #609).

    candidates.py 의 배정 후보 생성과 source_job.py 의 발행(기사 스냅샷·보유
    차량 재고)이 함께 읽는 단일 뷰입니다. candidates.py 는 이 중 활성 계약
    여부와 차량 자격만 읽고, 발행 쪽은 최초 입사일(`joined_on`)과 차종 스펙
    (`make_key`/`model_key`/`model_year`/`weekly_lease_fee`)까지 씁니다.

    D15 와 같은 규칙 — 퇴사 기사도 행을 남기고 `lease_ended_on` 만 채웁니다.
    """
    if current.empty:
        raise ValueError("current 가 비어 있습니다")
    pool = vehicle_master_with_model_id(vehicle_pool)
    # `fleet.py`/`assignment.py` 는 `weekly_price_usd` 를 쓰고(`vehicle_pool_from_silver`
    # 가 그렇게 이름 붙임), 발행 스키마는 `weekly_lease_fee` 를 씁니다. 어느
    # 이름으로 들어오든 발행 쪽으로 맞춥니다.
    if "weekly_price_usd" in pool.columns and "weekly_lease_fee" not in pool.columns:
        pool = pool.rename(columns={"weekly_price_usd": "weekly_lease_fee"})
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
        "joined_on": pd.to_datetime(rows["joined_on"]).dt.date,
        "lease_started_on": pd.to_datetime(rows["vehicle_since"]).dt.date,
        "lease_ended_on": pd.to_datetime(rows["exited_on"]).dt.date,
        "make_key": joined["make_key"].to_numpy(),
        "model_key": joined["model_key"].to_numpy(),
        "model_year": joined["model_year"].to_numpy(),
        "weekly_lease_fee": joined["weekly_lease_fee"].to_numpy(),
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
