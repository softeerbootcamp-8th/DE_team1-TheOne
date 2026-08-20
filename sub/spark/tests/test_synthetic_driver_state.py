"""`sub/generators/synthetic_driver_state`의 순수 로직 동등성과 체크포인트 계약.

붙잡는 것 두 가지다.

  1. `sub/prototype/synthesize.py`와의 동등성 — 같은 config/seed/input이면
     events/current/profiles/noise가 완전히 같아야 한다 (#605 완료 조건).
  2. 체크포인트 계약 — config_hash가 다른 전월은 이어받지 않고, 임의 과거
     월부터 재개 가능해야 한다 (blue_print.md 4.3).
"""

from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd
import pytest
from conftest import TEST_CONFIG_DATA

from sub.config import build_config
from sub.generators.synthetic_driver_state import adapters, checkpoint
from sub.generators.synthetic_driver_state.lifecycle import synthesize_month
from sub.prototype import synthesize as prototype_synthesize
from sub.run_context import RunContext
from sub.spark.jobs.driver_assignment.candidates import REQUIRED as CANDIDATES_REQUIRED
from sub.spark.jobs.driver_master.preference import PREFERENCE_COLUMNS
from sub.spark.jobs.driver_master.traits import TIME_BLOCK_LABELS, WEEKDAY_LABELS

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


def _run_both(config, master, *, months: int = 1):
    """prototype과 새 모듈을 같은 입력으로 나란히 돌립니다."""
    proto = None
    new = None
    proto_prev = (None, None, None)
    new_prev = (None, None, None)
    for i in range(months):
        month = f"2024-{i + 1:02d}"
        proto = prototype_synthesize.synthesize_month(
            target_month=month, config=config, vehicle_master=master, trip_pool=POOL,
            previous_current=proto_prev[0], previous_events=proto_prev[1],
            previous_noise=proto_prev[2], fuel=FUEL,
        )
        new = synthesize_month(
            target_month=month, config=config, vehicle_master=master, trip_pool=POOL,
            previous_current=new_prev[0], previous_events=new_prev[1],
            previous_noise=new_prev[2], fuel=FUEL,
        )
        proto_prev = (proto.current, proto.events, proto.noise_state)
        new_prev = (new.current, new.events, new.noise_state)
    return proto, new


# ── 1. prototype과의 동등성 ──────────────────────────────────────────────


def test_초기_스냅샷이_prototype과_완전히_같다():
    config, master = _config(), _vehicle_master()
    proto, new = _run_both(config, master, months=1)

    pd.testing.assert_frame_equal(proto.events, new.events)
    pd.testing.assert_frame_equal(proto.current, new.current)
    pd.testing.assert_frame_equal(proto.profiles, new.profiles)
    pd.testing.assert_frame_equal(proto.noise_state, new.noise_state)
    assert proto.clip_rate == new.clip_rate


def test_lifecycle_두달째도_prototype과_완전히_같다():
    """join/exit/vehicle_change가 실제로 섞인 두 번째 달까지 동등성을 본다."""
    config, master = _config(), _vehicle_master()
    proto, new = _run_both(config, master, months=2)

    pd.testing.assert_frame_equal(proto.events, new.events)
    pd.testing.assert_frame_equal(proto.current, new.current)
    pd.testing.assert_frame_equal(proto.profiles, new.profiles)
    pd.testing.assert_frame_equal(proto.noise_state, new.noise_state)
    # 실제로 유출·유입·교체가 있었는지 확인 — 아무 일도 안 일어나면 동등성이
    # events fold 정도만 검증하고 lifecycle 분기는 못 잡는다.
    assert set(new.events["event_type"]) == {
        prototype_synthesize.EVENT_JOIN,
        prototype_synthesize.EVENT_EXIT,
        prototype_synthesize.EVENT_VEHICLE_CHANGE,
    }


# ── 2. 체크포인트 계약 (blue_print.md 4.3) ──────────────────────────────


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
    )
    other_run = RunContext.create("2024-01", _config(51))
    with pytest.raises(checkpoint.CheckpointLineageError, match="이미 있습니다"):
        checkpoint.write_checkpoint(
            tmp_path, other_run,
            events=result.events, events_all=result.events, current=result.current,
            noise=result.noise_state, previous_month_value=None, previous_run_id=None,
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
    pd.testing.assert_frame_equal(prototype_synthesize.fold_events(events_all), second.current)
    assert len(events_all) == len(first.events) + len(second.events)


# ── 3. legacy 어댑터 (#606) ──────────────────────────────────────────────


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


def test_snapshot_뷰가_candidates_필수컬럼을_전부_채운다():
    result, pool = _synthesize_for_adapters()
    tables = adapters.to_snapshot_tables(result.current, pool, snapshot_date=date(2024, 1, 1))

    assert CANDIDATES_REQUIRED["customers"] <= set(tables.customer.columns)
    assert CANDIDATES_REQUIRED["leases"] <= set(tables.lease_contract.columns)
    assert CANDIDATES_REQUIRED["taxis"] <= set(tables.taxi.columns)
    assert set(tables.customer["synthetic_driver_id"]) == set(result.current["driver_id"])
    # 아직 아무도 퇴사하지 않은 초기 스냅샷 — lease_ended_on 이 전부 결측입니다.
    assert tables.lease_contract["lease_ended_on"].isna().all()


def test_snapshot_뷰는_퇴사기사의_lease_ended_on을_채운다():
    result, pool = _synthesize_for_adapters()
    current = result.current.copy()
    exited_id = current.iloc[0]["driver_id"]
    current.loc[current["driver_id"] == exited_id, "exited_on"] = pd.Timestamp("2024-02-01")

    tables = adapters.to_snapshot_tables(current, pool, snapshot_date=date(2024, 2, 1))
    row = tables.lease_contract.loc[
        tables.lease_contract["customer_id"] == f"CUST_{exited_id}"
    ].iloc[0]
    assert row["lease_ended_on"] == date(2024, 2, 1)


def test_preferences_뷰가_candidates_필수컬럼을_전부_채운다():
    result, _ = _synthesize_for_adapters()
    preferences = adapters.to_driver_preferences(result.profiles)

    assert CANDIDATES_REQUIRED["preferences"] <= set(preferences.columns)
    assert list(preferences.columns) == PREFERENCE_COLUMNS
    assert set(preferences["driver_id"]) == set(result.profiles["driver_id"])
    # 요일·시간대는 문자열 라벨로 바뀌어야 합니다 (bitmask 인코딩이 라벨 문자열을 봄).
    assert preferences["active_weekdays"].iloc[0][0] in WEEKDAY_LABELS
    assert preferences["preferred_time_blocks"].iloc[0][0] in TIME_BLOCK_LABELS
