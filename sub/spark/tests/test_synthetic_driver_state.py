"""`sub/generators/synthetic_driver_state`의 체크포인트 계약.

config_hash가 다른 전월은 이어받지 않고, 임의 과거 월부터 재개 가능해야
한다 (blue_print.md 4.3).

`sub/prototype/synthesize.py`와의 동등성 테스트는 마이그레이션 검증(#605,
#609) 완료 후 prototype과 함께 제거했다.
"""

from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd
import pytest
from conftest import TEST_CONFIG_DATA

from sub.config import build_config
from sub.generators.synthetic_driver_state import adapters, checkpoint, events, fleet
from sub.generators.synthetic_driver_state.lifecycle import synthesize_month
from sub.run_context import RunContext
from sub.spark.jobs.driver_assignment.candidates import REQUIRED as CANDIDATES_REQUIRED
from sub.spark.jobs.driver_master.preference import PREFERENCE_COLUMNS

SEED = 42
POOL = {
    "trip_miles": np.linspace(0.5, 30.0, 500),
    "trip_time_min": np.linspace(3.0, 90.0, 500),
}
FUEL = {"gallon_usd": 4.1471, "kwh_usd": 0.417143}


def _config(initial_count: int = 400):
    data = {k: (dict(v) if isinstance(v, dict) else v) for k, v in TEST_CONFIG_DATA.items()}
    # 유출·유입·교체가 실제로 발생하도록 정원을 키웁니다 (400 × 0.007 = 2.8건 기대).
    data["driver"] = {**data["driver"], "initial_count": initial_count}
    return build_config(data)


def _vehicle_master() -> pd.DataFrame:
    rows = []
    for i, (price, mpg, group) in enumerate([
        (500.0, 30.0, "STANDARD"), (600.0, 25.0, "SINGLE"), (700.0, 40.0, "BOTH"),
    ]):
        rows.append({
            "vehicle_model_id": f"MAKE{i}|MODEL{i}|2024",
            "make_key": f"MAKE{i}", "model_key": f"MODEL{i}", "model_year": 2024,
            "weekly_price_usd": price, "combined_mpg": mpg,
            "combined_kwh_per_100mi": 0.0, "range_miles": 0.0,
            "uber_comfort_eligible": group != "STANDARD",
            "lyft_extra_comfort_eligible": group == "BOTH",
            "vehicle_group": group,
        })
    return pd.DataFrame(rows)


# ── 1. 체크포인트 계약 (blue_print.md 4.3) ──────────────────────────────


def test_체크포인트를_쓰고_그대로_읽는다(tmp_path):
    config, master = _config(50), _vehicle_master()
    run = RunContext.create("2024-01", config)
    result = synthesize_month(
        target_month="2024-01", config=config, vehicle_master=master, trip_pool=POOL,
        previous_current=None, previous_events=None, previous_noise=None, fuel=FUEL,
    )
    checkpoint.write_checkpoint(
        tmp_path, run,
        events=result.events, events_all=result.events, current=result.current,
        noise=result.noise_state, previous_month_value=None, previous_run_id=None,
        clip_rate=result.clip_rate,
    )
    current, events_all, noise, manifest = checkpoint.read_checkpoint(tmp_path, "2024-01")
    pd.testing.assert_frame_equal(current, result.current)
    pd.testing.assert_frame_equal(events_all, result.events)
    pd.testing.assert_frame_equal(noise, result.noise_state)
    assert manifest["run_id"] == run.run_id
    assert manifest["config_hash"] == run.config_hash
    assert manifest["previous_month"] is None


def test_같은_run_id로_다시_쓰면_그대로_반환한다(tmp_path):
    """재시도 안전 — 같은 설정으로 다시 써도 실패하지 않는다."""
    config, master = _config(50), _vehicle_master()
    run = RunContext.create("2024-01", config)
    result = synthesize_month(
        target_month="2024-01", config=config, vehicle_master=master, trip_pool=POOL,
        previous_current=None, previous_events=None, previous_noise=None, fuel=FUEL,
    )
    kwargs = dict(
        events=result.events, events_all=result.events, current=result.current,
        noise=result.noise_state, previous_month_value=None, previous_run_id=None,
        clip_rate=result.clip_rate,
    )
    first = checkpoint.write_checkpoint(tmp_path, run, **kwargs)
    second = checkpoint.write_checkpoint(tmp_path, run, **kwargs)
    assert first == second


def test_다른_설정으로_같은_달을_다시_쓰면_거부한다(tmp_path):
    config, master = _config(50), _vehicle_master()
    run = RunContext.create("2024-01", config)
    result = synthesize_month(
        target_month="2024-01", config=config, vehicle_master=master, trip_pool=POOL,
        previous_current=None, previous_events=None, previous_noise=None, fuel=FUEL,
    )
    checkpoint.write_checkpoint(
        tmp_path, run,
        events=result.events, events_all=result.events, current=result.current,
        noise=result.noise_state, previous_month_value=None, previous_run_id=None,
        clip_rate=result.clip_rate,
    )
    other_run = RunContext.create("2024-01", _config(51))
    with pytest.raises(checkpoint.CheckpointLineageError, match="이미 있습니다"):
        checkpoint.write_checkpoint(
            tmp_path, other_run,
            events=result.events, events_all=result.events, current=result.current,
            noise=result.noise_state, previous_month_value=None, previous_run_id=None,
            clip_rate=result.clip_rate,
        )


def test_config_hash가_다른_전월_체크포인트는_이어받지_않는다(tmp_path):
    config, master = _config(50), _vehicle_master()
    first_run = RunContext.create("2024-01", config)
    result = synthesize_month(
        target_month="2024-01", config=config, vehicle_master=master, trip_pool=POOL,
        previous_current=None, previous_events=None, previous_noise=None, fuel=FUEL,
    )
    checkpoint.write_checkpoint(
        tmp_path, first_run,
        events=result.events, events_all=result.events, current=result.current,
        noise=result.noise_state, previous_month_value=None, previous_run_id=None,
        clip_rate=result.clip_rate,
    )
    changed_config = _config(51)
    second_run = RunContext.create("2024-02", changed_config)
    with pytest.raises(checkpoint.CheckpointLineageError, match="2024-01.*다시 생성"):
        checkpoint.resolve_previous_checkpoint(tmp_path, second_run)


def test_전월_체크포인트가_없으면_부트스트랩으로_전부_None이다(tmp_path):
    config = _config(50)
    run = RunContext.create("2024-01", config)
    current, events_all, noise, prev_month, prev_run_id = checkpoint.resolve_previous_checkpoint(
        tmp_path, run
    )
    assert (current, events_all, noise, prev_month, prev_run_id) == (None, None, None, None, None)


def test_임의_과거_월_체크포인트부터_재개할_수_있다(tmp_path):
    """2024-03을 만들 때 2024-02가 없어도, 2024-02를 먼저 채우면 이어진다."""
    config, master = _config(50), _vehicle_master()
    first = synthesize_month(
        target_month="2024-01", config=config, vehicle_master=master, trip_pool=POOL,
        previous_current=None, previous_events=None, previous_noise=None, fuel=FUEL,
    )
    run1 = RunContext.create("2024-01", config)
    checkpoint.write_checkpoint(
        tmp_path, run1, events=first.events, events_all=first.events, current=first.current,
        noise=first.noise_state, previous_month_value=None, previous_run_id=None,
        clip_rate=first.clip_rate,
    )

    second = synthesize_month(
        target_month="2024-02", config=config, vehicle_master=master, trip_pool=POOL,
        previous_current=first.current, previous_events=first.events,
        previous_noise=first.noise_state, fuel=FUEL,
    )
    # events_all은 그 달 이벤트만이 아니라 **누적 전체**입니다 — 아니면 다음 재개가
    # 이번 달 이전 역사를 잃습니다.
    second_events_all = pd.concat([first.events, second.events], ignore_index=True)
    run2 = RunContext.create("2024-02", config)
    checkpoint.write_checkpoint(
        tmp_path, run2, events=second.events, events_all=second_events_all, current=second.current,
        noise=second.noise_state, previous_month_value="2024-01", previous_run_id=run1.run_id,
        clip_rate=second.clip_rate,
    )

    run3 = RunContext.create("2024-03", config)
    current, events_all, noise, prev_month, prev_run_id = checkpoint.resolve_previous_checkpoint(
        tmp_path, run3
    )
    assert prev_month == "2024-02"
    assert prev_run_id == run2.run_id
    pd.testing.assert_frame_equal(current, second.current)
    # events_all에서 재생(fold_events)해도 같은 current가 나와야 재개 후 이어지는
    # 월들이 올바른 역사를 봅니다.
    pd.testing.assert_frame_equal(events.fold_events(events_all), second.current)
    assert len(events_all) == len(first.events) + len(second.events)


# ── 2. 기존 Spark 경로용 뷰 변환 (#606, #609) ──────────────────────────────


def _vehicle_pool_no_model_id() -> pd.DataFrame:
    """`synthetic_company_snapshot.build_vehicle_pool()` 산출물 모양 (model_id 없음)."""
    return pd.DataFrame([
        {"make_key": f"MAKE{i}", "model_key": f"MODEL{i}", "model_year": 2024,
         "weekly_lease_fee": price, "uber_comfort_eligible": group != "STANDARD",
         "lyft_extra_comfort_eligible": group == "BOTH", "vehicle_group": group}
        for i, (price, group) in enumerate([
            (500.0, "STANDARD"), (600.0, "SINGLE"), (700.0, "BOTH"),
        ])
    ])


def test_vehicle_model_id를_한_번만_붙인다():
    pool = _vehicle_pool_no_model_id()
    with_id = adapters.vehicle_master_with_model_id(pool)
    assert (with_id["vehicle_model_id"] == with_id["make_key"] + "|" + with_id["model_key"] + "|2024").all()
    # 이미 있으면 그대로 반환합니다 (두 번 불러도 안전).
    assert adapters.vehicle_master_with_model_id(with_id) is with_id


def _synthesize_for_adapters(initial_count=50):
    config = _config(initial_count)
    pool = adapters.vehicle_master_with_model_id(_vehicle_pool_no_model_id())
    master = pool.rename(columns={"weekly_lease_fee": "weekly_price_usd"}).assign(
        combined_mpg=30.0, combined_kwh_per_100mi=0.0,
    )
    result = synthesize_month(
        target_month="2024-01", config=config, vehicle_master=master, trip_pool=POOL,
        previous_current=None, previous_events=None, previous_noise=None, fuel=FUEL,
    )
    return result, pool


def test_current_driver_vehicle_뷰가_candidates_필수컬럼과_발행용_차종스펙을_전부_채운다():
    """#609 — candidates.py 의 필수 컬럼과, 발행 쪽(`build_driver_vehicle_monthly_snapshot`)
    이 이력 계산에 쓰는 `joined_on`·차종 스펙을 한 뷰가 함께 채웁니다."""
    result, pool = _synthesize_for_adapters()
    current_driver_vehicle = adapters.to_current_driver_vehicle(result.current, pool)

    assert CANDIDATES_REQUIRED["current_driver_vehicle"] <= set(current_driver_vehicle.columns)
    assert {
        "joined_on", "make_key", "model_key", "model_year", "weekly_lease_fee",
    } <= set(current_driver_vehicle.columns)
    assert set(current_driver_vehicle["driver_id"]) == set(result.current["driver_id"])
    # 아직 아무도 퇴사하지 않은 초기 스냅샷 — lease_ended_on 이 전부 결측입니다.
    assert current_driver_vehicle["lease_ended_on"].isna().all()
    # joined_on 은 이 초기 스냅샷에서 vehicle_since(=최초 배정일)와 같아야 합니다 —
    # 아직 차량을 바꾼 적이 없으므로.
    pd.testing.assert_series_equal(
        current_driver_vehicle["joined_on"],
        current_driver_vehicle["lease_started_on"],
        check_names=False,
    )


def test_current_driver_vehicle_뷰는_퇴사기사의_lease_ended_on을_채운다():
    result, pool = _synthesize_for_adapters()
    current = result.current.copy()
    exited_id = current.iloc[0]["driver_id"]
    current.loc[current["driver_id"] == exited_id, "exited_on"] = pd.Timestamp("2024-02-01")

    current_driver_vehicle = adapters.to_current_driver_vehicle(current, pool)
    row = current_driver_vehicle.loc[current_driver_vehicle["driver_id"] == exited_id].iloc[0]
    assert row["lease_ended_on"] == date(2024, 2, 1)


def test_preferences_뷰가_candidates_필수컬럼을_전부_채운다():
    result, _ = _synthesize_for_adapters()
    preferences = adapters.to_driver_preferences(result.profiles)
    profiles = result.profiles.set_index("driver_id")
    preferences_by_driver = preferences.set_index("driver_id")

    assert CANDIDATES_REQUIRED["preferences"] <= set(preferences.columns)
    assert list(preferences.columns) == PREFERENCE_COLUMNS
    assert set(preferences["driver_id"]) == set(profiles.index)
    # weekday_mask/time_block_mask는 profiles의 정수 인덱스 리스트를 그대로
    # 비트마스크로 인코딩한 값이어야 합니다(#643).
    for driver_id in profiles.index[:5]:
        weekday_mask = int(sum(1 << int(i) for i in profiles.loc[driver_id, "active_weekdays"]))
        time_block_mask = int(sum(1 << int(i) for i in profiles.loc[driver_id, "preferred_time_blocks"]))
        assert preferences_by_driver.loc[driver_id, "weekday_mask"] == weekday_mask
        assert preferences_by_driver.loc[driver_id, "time_block_mask"] == time_block_mask


# ── 3. 실측 Silver 변환 (#628) ──────────────────────────────────────────


def _silver_vehicle_master() -> pd.DataFrame:
    """`schema.source.VEHICLE_MASTER_SCHEMA` 모양의 축소 픽스처. 같은 차종이
    플랫폼별로 여러 행일 수 있습니다(vendor/platform/product 조합)."""
    return pd.DataFrame([
        {"make_key": "MAKE0", "model_key": "MODEL0", "vendor": "V1", "platform": None,
         "product": None, "min_year": None, "weekly_lease_fee": 500.0,
         "combined_mpg_min": 28.0, "combined_mpg_max": 32.0,
         "combined_kwh_per_100mi_min": 0.0, "combined_kwh_per_100mi_max": 0.0},
        {"make_key": "MAKE1", "model_key": "MODEL1", "vendor": "V1", "platform": "uber",
         "product": "Comfort", "min_year": 2020, "weekly_lease_fee": 600.0,
         "combined_mpg_min": 24.0, "combined_mpg_max": 26.0,
         "combined_kwh_per_100mi_min": 0.0, "combined_kwh_per_100mi_max": 0.0},
        {"make_key": "MAKE2", "model_key": "MODEL2", "vendor": "V1", "platform": "uber",
         "product": "Comfort", "min_year": 2020, "weekly_lease_fee": 700.0,
         "combined_mpg_min": 0.0, "combined_mpg_max": 0.0,
         "combined_kwh_per_100mi_min": 28.0, "combined_kwh_per_100mi_max": 30.0},
        {"make_key": "MAKE2", "model_key": "MODEL2", "vendor": "V1", "platform": "lyft",
         "product": "Extra Comfort", "min_year": 2020, "weekly_lease_fee": 700.0,
         "combined_mpg_min": 0.0, "combined_mpg_max": 0.0,
         "combined_kwh_per_100mi_min": 28.0, "combined_kwh_per_100mi_max": 30.0},
    ])


def test_실측_vehicle_master의_min_max_제원을_중앙값_하나로_합친다():
    pool = adapters.vehicle_pool_from_silver(_silver_vehicle_master())
    assert set(pool.columns) >= {
        "vehicle_model_id", "weekly_price_usd", "combined_mpg",
        "combined_kwh_per_100mi", "vehicle_group",
    }
    row0 = pool.loc[pool["make_key"] == "MAKE0"].iloc[0]
    assert row0["combined_mpg"] == 30.0  # (28+32)/2
    assert row0["weekly_price_usd"] == 500.0
    # MAKE2|MODEL2 는 uber Comfort + lyft Extra Comfort 자격이 둘 다 있음 -> BOTH.
    row2 = pool.loc[pool["make_key"] == "MAKE2"].iloc[0]
    assert row2["vehicle_group"] == "BOTH"
    assert row2["combined_kwh_per_100mi"] == 29.0  # (28+30)/2
    # 같은 차종이 여러 vendor/platform 행으로 왔어도 결과는 차종당 한 행.
    assert len(pool) == pool["vehicle_model_id"].nunique()


def test_실측_유가_전기요금을_평균낸다(tmp_path):
    gas_dir = tmp_path / "silver" / "gas_price" / "collected_month=2026-08"
    ev_dir = tmp_path / "silver" / "ev_charging_price" / "collected_month=2026-08"
    gas_dir.mkdir(parents=True)
    ev_dir.mkdir(parents=True)
    pd.DataFrame({"price_usd_per_gallon": [4.0, 4.2]}).to_parquet(gas_dir / "p.parquet")
    pd.DataFrame({"average_price_usd_per_kwh": [0.40, 0.42]}).to_parquet(ev_dir / "p.parquet")

    prices = fleet.load_fuel_prices(data_dir=tmp_path)
    assert prices == {"gallon_usd": pytest.approx(4.1), "kwh_usd": pytest.approx(0.41)}
