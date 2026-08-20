"""프로토타입의 비자명한 로직만 붙잡습니다.

붙잡는 것 세 가지입니다.

  1. 시드 파생 — blue_print.md D9 의 완료 조건
  2. 이벤트 fold — 4.2 의 "파생물은 재생으로 복원 가능" 이 실제로 성립하는가
  3. 기사 성향 안정성 — D7/D8 의 핵심. 월이 달라도 같은 값이 나오는가

배정 알고리즘 자체는 테스트하지 않습니다. 정답이 없어서 단정할 것이 없고, 대신
`metrics.py` 가 매 실행마다 품질을 숫자로 냅니다.

    uv run --with pandas --with pyarrow --with pytest pytest sub/prototype/tests -q
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from sub.prototype import attribution, contract, curated, published, synthesize
from sub.prototype.assign import assign_vehicles, model_weekly_cost
from sub.seeds import SEED_MASK, Stage, derive_entity_seed, derive_seed

SEED = 42
POOL = {
    "trip_miles": np.linspace(0.5, 30.0, 500),
    "trip_time_min": np.linspace(3.0, 90.0, 500),
}
FUEL = {"gallon_usd": 4.1471, "kwh_usd": 0.417143}


# --- 1. 시드 파생 (D9) ------------------------------------------------------


def test_derive_seed_is_pure():
    assert derive_seed(SEED, Stage.SNAPSHOT_INIT) == derive_seed(SEED, Stage.SNAPSHOT_INIT)


def test_stage_alone_changes_seed():
    seeds = {derive_seed(SEED, stage, "2026-01") for stage in Stage}
    assert len(seeds) == len(list(Stage))


def test_month_alone_changes_seed():
    a = derive_seed(SEED, Stage.SNAPSHOT_EVOLVE, "2026-01")
    b = derive_seed(SEED, Stage.SNAPSHOT_EVOLVE, "2026-02")
    none = derive_seed(SEED, Stage.SNAPSHOT_EVOLVE)
    assert len({a, b, none}) == 3


def test_seed_fits_signed_bigint():
    """Spark `lit(seed)` 가 DecimalType 으로 승격되지 않아야 합니다."""
    for stage in Stage:
        assert 0 <= derive_seed(SEED, stage, "2026-01") <= SEED_MASK < 2**63


def test_string_stage_rejected():
    with pytest.raises(TypeError):
        derive_seed(SEED, "snapshot_init")  # type: ignore[arg-type]


def test_stage_independence():
    """한 stage 의 시드가 바뀌어도 다른 stage 는 변하지 않습니다."""
    before = derive_seed(SEED, Stage.ALLOCATION_BUCKET, "2026-01")
    # DRIVER_PROFILE 의 파생을 아무리 소비해도
    other = derive_seed(SEED, Stage.DRIVER_PROFILE)
    for i in range(100):
        derive_entity_seed(other, f"DRIVER_{i:04d}")
    assert derive_seed(SEED, Stage.ALLOCATION_BUCKET, "2026-01") == before


def test_extra_draws_do_not_shift_other_drivers():
    """한 기사의 난수 소비를 늘려도 다른 기사의 값은 변하지 않습니다.

    전역 시드에서 벗어나는 목적이 이것입니다. 단계 시드 하나를 루프가 돌려쓰면
    앞 기사가 draw 를 하나 더 뽑는 순간 뒤 기사 전원이 바뀝니다.
    """
    stage_seed = derive_seed(SEED, Stage.DRIVER_TRAITS)
    rng_b_before = np.random.default_rng(derive_entity_seed(stage_seed, "B")).random(3)

    rng_a = np.random.default_rng(derive_entity_seed(stage_seed, "A"))
    rng_a.random(50)  # A 가 더미 draw 를 50번 더 뽑아도

    rng_b_after = np.random.default_rng(derive_entity_seed(stage_seed, "B")).random(3)
    assert np.array_equal(rng_b_before, rng_b_after)


# --- 2. 기사 성향 안정성 (D7 A, D8) -----------------------------------------


def test_base_traits_stable_across_target_months():
    """기사 A 의 기준값은 어느 달을 처리하든 같습니다.

    `traits_pool_month` 가 같으면 같은 값입니다. 대상 월은 입력에 없습니다 —
    있으면 "그 기사가 처음 등장한 달"에 의존하는 경로 의존이 생깁니다.
    """
    a = synthesize.base_traits("DRIVER_0007", global_seed=SEED, traits_pool_month="2024-01", trip_pool=POOL)
    b = synthesize.base_traits("DRIVER_0007", global_seed=SEED, traits_pool_month="2024-01", trip_pool=POOL)
    assert a == b
    for field in ("base_weekly_hours", "distance_pref_mi", "max_deadhead_minutes", "preferred_time_blocks"):
        assert a[field] == b[field]


def test_base_traits_differ_by_pool_month():
    """가입 시점 월이 다르면 다른 기사입니다 (D8 시점 정합)."""
    a = synthesize.base_traits("DRIVER_0007", global_seed=SEED, traits_pool_month="2024-01", trip_pool=POOL)
    b = synthesize.base_traits("DRIVER_0007", global_seed=SEED, traits_pool_month="2024-02", trip_pool=POOL)
    assert a["base_weekly_hours"] != b["base_weekly_hours"]


def test_realization_chain_uses_previous_state_not_seed():
    """자기상관은 시드가 아니라 전월 상태에서 옵니다 (D7 '중요')."""
    from sub.config import build_config

    from sub.spark.tests.conftest import TEST_CONFIG_DATA

    config = build_config(TEST_CONFIG_DATA)
    traits = pd.DataFrame([
        synthesize.base_traits(f"DRIVER_{i:04d}", global_seed=SEED, traits_pool_month="2026-01", trip_pool=POOL)
        for i in range(20)
    ])
    no_prior, noise_a, _ = synthesize.realize_month(
        traits, None, global_seed=SEED, target_month="2026-02", config=config
    )
    with_prior, _, _ = synthesize.realize_month(
        traits, noise_a, global_seed=SEED, target_month="2026-02", config=config
    )
    # 같은 (seed, month) 인데 전월 상태만 다르면 실현값이 달라야 합니다.
    assert not np.allclose(no_prior["realization_factor"], with_prior["realization_factor"])


def test_weekly_hours_and_drive_budget_move_together():
    """근무 시간이 늘었는데 운행 예산이 그대로면 모순입니다 (D7 '연동 필수')."""
    from sub.config import build_config

    from sub.spark.tests.conftest import TEST_CONFIG_DATA

    config = build_config(TEST_CONFIG_DATA)
    traits = pd.DataFrame([
        synthesize.base_traits(f"DRIVER_{i:04d}", global_seed=SEED, traits_pool_month="2026-01", trip_pool=POOL)
        for i in range(200)
    ])
    realized, _, _ = synthesize.realize_month(
        traits, None, global_seed=SEED, target_month="2026-01", config=config
    )
    # 같은 기사 안에서 주 근무시간과 하루 운행 예산이 같은 방향이어야 합니다.
    correlation = np.corrcoef(realized["weekly_hours"], realized["target_drive_minutes"])[0, 1]
    assert correlation > 0.3, f"근무시간-운행예산 상관이 너무 낮습니다: {correlation:.3f}"
    # 그리고 예산은 기사별 하한(4~8h)·상한(8~12h) 안에 있어야 합니다.
    assert (realized["target_drive_minutes"] >= realized["min_drive_minutes"]).all()
    assert (realized["target_drive_minutes"] <= realized["max_drive_minutes"]).all()
    assert realized["target_drive_minutes"].between(240, 720).all()


# --- 3. 이벤트 fold (4.2) ---------------------------------------------------


def _event(driver_id, kind, day, taxi_id=None, pool_month=None):
    return {
        "driver_id": driver_id, "event_type": kind,
        "event_ts": pd.Timestamp(day), "taxi_id": taxi_id,
        "traits_pool_month": pool_month,
    }


def test_fold_events_replays_current_state():
    events = pd.DataFrame([
        _event("D1", synthesize.EVENT_JOIN, "2024-01-01", "CAR_A", "2024-01"),
        _event("D2", synthesize.EVENT_JOIN, "2024-01-01", "CAR_B", "2024-01"),
        _event("D1", synthesize.EVENT_VEHICLE_CHANGE, "2024-03-01", "CAR_C"),
        _event("D2", synthesize.EVENT_EXIT, "2024-04-01"),
    ])
    current = synthesize.fold_events(events).set_index("driver_id")
    assert current.at["D1", "taxi_id"] == "CAR_C"
    assert current.at["D1", "vehicle_since"] == pd.Timestamp("2024-03-01")
    # D15: 유출 기사의 행을 삭제하지 않습니다.
    assert "D2" in current.index
    assert current.at["D2", "exited_on"] == pd.Timestamp("2024-04-01")


def test_fold_is_order_independent():
    """원장을 어떤 순서로 읽어도 같은 상태가 나와야 재생이 성립합니다."""
    rows = [
        _event("D1", synthesize.EVENT_JOIN, "2024-01-01", "CAR_A", "2024-01"),
        _event("D1", synthesize.EVENT_VEHICLE_CHANGE, "2024-02-01", "CAR_B"),
        _event("D1", synthesize.EVENT_VEHICLE_CHANGE, "2024-03-01", "CAR_C"),
    ]
    forward = synthesize.fold_events(pd.DataFrame(rows))
    backward = synthesize.fold_events(pd.DataFrame(list(reversed(rows))))
    pd.testing.assert_frame_equal(forward, backward)


def test_unknown_event_type_is_loud():
    events = pd.DataFrame([_event("D1", "teleport", "2024-01-01", "CAR_A", "2024-01")])
    with pytest.raises(ValueError, match="알 수 없는 이벤트"):
        synthesize.fold_events(events)


# --- 4. 배정 (D5 · D6) ------------------------------------------------------


def _fleet(count_per_model: int = 3) -> pd.DataFrame:
    models = [
        ("CHEAP|THIRSTY|2024", 400.0, 15.0, 0.0, "STANDARD"),
        ("PRICEY|FRUGAL|2024", 700.0, 50.0, 0.0, "BOTH"),
        ("MID|MID|2024", 550.0, 30.0, 0.0, "SINGLE"),
    ]
    rows = []
    for model_id, price, mpg, kwh, group in models:
        for serial in range(count_per_model):
            rows.append({
                "vehicle_model_id": model_id, "taxi_id": f"{model_id}#{serial:03d}",
                "weekly_price_usd": price, "combined_mpg": mpg,
                "combined_kwh_per_100mi": kwh, "vehicle_group": group,
            })
    return pd.DataFrame(rows)


def _drivers(count: int, tier_preference: float) -> pd.DataFrame:
    return pd.DataFrame([
        {
            "driver_id": f"DRIVER_{i:04d}", "tier_preference": tier_preference,
            "base_weekly_hours": 40.0, "avg_trip_duration_min": 20.0,
            "distance_pref_mi": 8.0,
        }
        for i in range(count)
    ])


def test_assignment_respects_inventory():
    """재고보다 많은 기사에게 차를 줄 수 없습니다."""
    fleet = _fleet(count_per_model=2)  # 총 6대
    assigned = assign_vehicles(
        _drivers(10, 0.1), fleet,
        global_seed=SEED, target_month="2026-01", rationality=1.0, fuel=FUEL,
    )
    assert len(assigned) == 6
    assert len(set(assigned.values())) == 6, "한 차량이 두 기사에게 갔습니다"


def test_rationality_zero_and_one_differ():
    """rationality 가 실제로 배정을 바꿔야 D6 이 장식이 아닙니다."""
    # 재고를 기사 수보다 넉넉히 둡니다. 모자라면 최저비용 모델이 소진되어
    # rationality 가 아니라 재고 제약이 배정을 정하고, 이 테스트가 그것을
    # 합리성 실패로 오독합니다.
    fleet = _fleet(count_per_model=40)
    drivers = _drivers(30, 0.1)
    rational = assign_vehicles(
        drivers, fleet, global_seed=SEED, target_month="2026-01", rationality=1.0, fuel=FUEL
    )
    random_pick = assign_vehicles(
        drivers, fleet, global_seed=SEED, target_month="2026-01", rationality=0.0, fuel=FUEL
    )
    assert rational != random_pick
    # rationality=1.0 이면 전원이 비용 최적 모델을 골라야 합니다.
    miles = 40.0 * 60.0 / 20.0 * 8.0
    models = fleet.drop_duplicates("vehicle_model_id").sort_values("vehicle_model_id").reset_index(drop=True)
    cheapest = models.at[int(np.argmin(model_weekly_cost(models, miles, FUEL))), "vehicle_model_id"]
    assert all(taxi_id.startswith(cheapest) for taxi_id in rational.values())


def test_assignment_is_deterministic():
    fleet = _fleet(count_per_model=5)
    drivers = _drivers(12, 0.6)
    kwargs = dict(global_seed=SEED, target_month="2026-01", rationality=0.6, fuel=FUEL)
    assert assign_vehicles(drivers, fleet, **kwargs) == assign_vehicles(drivers, fleet, **kwargs)


def test_premium_seeker_prefers_eligible_pool():
    """프리미엄 선호가 높은 기사는 자격 있는 차만 봅니다 (eligible pool 단계)."""
    fleet = _fleet(count_per_model=5)
    assigned = assign_vehicles(
        _drivers(4, 0.9), fleet,
        global_seed=SEED, target_month="2026-01", rationality=0.0, fuel=FUEL,
    )
    premium = set(fleet.loc[fleet["vehicle_group"] != "STANDARD", "taxi_id"])
    assert set(assigned.values()) <= premium


# --- 5. lifecycle 2개월 연쇄 (D14 · D15 · 4.2) -------------------------------
# 로컬에 HVFHV 가 2026-01 한 달만 있어서 `run.py` 로는 두 번째 달을 돌릴 수
# 없습니다. 그 경로가 유일하게 검증되지 않는 곳이라 여기서 직접 태웁니다.


def _config():
    from sub.config import build_config

    from sub.spark.tests.conftest import TEST_CONFIG_DATA

    data = {k: (dict(v) if isinstance(v, dict) else v) for k, v in TEST_CONFIG_DATA.items()}
    # 400명. `monthly_count` 가 확률 반올림이라 60명으로도 유출이 나올 수는
    # 있지만 매달 나오지는 않습니다. 이 테스트는 유출이 실제로 발생하는 것을
    # 단정하므로 기대값이 1을 넘는 규모로 둡니다 (400 × 0.007 = 2.8).
    data["driver"] = {**data["driver"], "initial_count": 400}
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


def test_two_month_lifecycle_chain():
    config = _config()
    master = _vehicle_master()
    first = synthesize.synthesize_month(
        target_month="2024-01", config=config, vehicle_master=master, trip_pool=POOL,
        previous_current=None, previous_events=None, previous_noise=None, fuel=FUEL,
    )
    assert len(first.current) == config.driver.initial_count
    assert first.current["exited_on"].isna().all()
    assert first.current["taxi_id"].nunique() == len(first.current), "차량이 중복 배정됐습니다"

    second = synthesize.synthesize_month(
        target_month="2024-02", config=config, vehicle_master=master, trip_pool=POOL,
        previous_current=first.current,
        previous_events=first.events,
        previous_noise=first.noise_state,
        fuel=FUEL,
    )
    # D14: 총원이 자유 변동한다. 상한 검증은 없고 하한만 있다.
    active_before = int(first.current["exited_on"].isna().sum())
    active_after = int(second.current["exited_on"].isna().sum())
    assert active_after >= 1
    # D15: 유출 기사의 행이 남아 있다.
    assert len(second.current) >= len(first.current)
    exited = second.current[second.current["exited_on"].notna()]
    assert len(exited) >= 1, "exit_rate 가 0이 아닌데 유출이 없습니다"
    # 활성 기사끼리 차량이 겹치지 않는다 (제약 5 의 재고 수준 보장).
    active = second.current[second.current["exited_on"].isna()]
    assert active["taxi_id"].nunique() == len(active)
    # 신규 기사의 traits_pool_month 는 가입한 달이다 (D8).
    joined_second = second.events[second.events["event_type"] == synthesize.EVENT_JOIN]
    assert set(joined_second["traits_pool_month"]) <= {"2024-02"}
    print(f"활성 {active_before} -> {active_after}, 누적 {len(second.current)}")


def test_second_month_is_deterministic():
    config, master = _config(), _vehicle_master()
    def chain():
        a = synthesize.synthesize_month(
            target_month="2024-01", config=config, vehicle_master=master, trip_pool=POOL,
            previous_current=None, previous_events=None, previous_noise=None, fuel=FUEL,
        )
        return synthesize.synthesize_month(
            target_month="2024-02", config=config, vehicle_master=master, trip_pool=POOL,
            previous_current=a.current, previous_events=a.events,
            previous_noise=a.noise_state, fuel=FUEL,
        )
    pd.testing.assert_frame_equal(chain().current, chain().current)


def test_traits_survive_the_month_boundary():
    """2024-01 가입 기사의 기준값이 2024-02 실행에서도 같아야 합니다 (D8 의 목표).

    승계(이전 parquet 복사)가 아니라 재계산으로 같아야 합니다 — 그래서 이 테스트가
    비교하는 것은 파일이 아니라 `base_traits` 의 반환값입니다.
    """
    config, master = _config(), _vehicle_master()
    first = synthesize.synthesize_month(
        target_month="2024-01", config=config, vehicle_master=master, trip_pool=POOL,
        previous_current=None, previous_events=None, previous_noise=None, fuel=FUEL,
    )
    second = synthesize.synthesize_month(
        target_month="2024-02", config=config, vehicle_master=master, trip_pool=POOL,
        previous_current=first.current, previous_events=first.events,
        previous_noise=first.noise_state, fuel=FUEL,
    )
    stable = ["base_weekly_hours", "distance_pref_mi", "max_deadhead_minutes", "volatility"]
    a = first.profiles.set_index("driver_id")[stable]
    b = second.profiles.set_index("driver_id")[stable]
    shared = a.index.intersection(b.index)
    assert len(shared) > 10
    pd.testing.assert_frame_equal(a.loc[shared], b.loc[shared])
    # 반면 월별 실현값은 달라야 합니다 (D7 의 B).
    ra = first.profiles.set_index("driver_id").loc[shared, "weekly_hours"]
    rb = second.profiles.set_index("driver_id").loc[shared, "weekly_hours"]
    assert not np.allclose(ra, rb), "월별 실현값이 안 움직입니다 — D7 B 가 죽어 있습니다"


# --- 5. 귀속 청크 경계 -------------------------------------------------------
#
# A1: 후보 테이블을 하루 통째로 만들지 않고 버킷 하나씩 흘려 처리합니다. 메모리
# 최대치를 버킷 수만큼 나누는 것이 목적이고, **결과가 같다**는 것이 그 전제입니다.
# 여기서 붙잡는 게 정확히 그 전제입니다 — 깨지면 매칭 지표가 조용히 달라집니다.


def _attribution_fixture(driver_count: int = 24, trip_count: int = 400):
    rng = np.random.default_rng(7)
    fleet = pd.DataFrame([
        {
            "taxi_id": f"CAR_{i:04d}",
            "uber_comfort_eligible": bool(i % 3 == 0),
            "lyft_extra_comfort_eligible": bool(i % 4 == 0),
        }
        for i in range(driver_count)
    ])
    profiles = pd.DataFrame([
        {
            "driver_id": f"DRIVER_{i:04d}", "taxi_id": f"CAR_{i:04d}",
            "joined_on": pd.Timestamp("2026-01-01"), "exited_on": pd.NaT,
            "active_weekdays": sorted(rng.choice(7, size=5, replace=False).tolist()),
            "preferred_time_blocks": sorted(rng.choice(8, size=4, replace=False).tolist()),
            "time_block_weights": rng.random(8).tolist(),
            "distance_pref_mi": float(rng.uniform(3, 12)),
            "airport_preference": float(rng.random()),
            "manhattan_preference": float(rng.random()),
            "tier_preference": float(rng.random()),
            "target_drive_minutes": int(rng.integers(240, 720)),
            "target_work_minutes": int(rng.integers(400, 900)),
            "max_deadhead_minutes": int(rng.integers(10, 25)),
            "buffer_seconds": 120,
        }
        for i in range(driver_count)
    ])
    day = pd.Timestamp("2026-01-15")
    minutes = np.sort(rng.integers(0, 24 * 60, size=trip_count))
    pickup = day + pd.to_timedelta(minutes, unit="m")
    duration = rng.integers(5, 45, size=trip_count)
    trips = pd.DataFrame({
        "trip_key": [f"T{i:06d}" for i in range(trip_count)],
        "on_scene_datetime": pickup - pd.to_timedelta(rng.integers(0, 300, size=trip_count), unit="s"),
        "pickup_datetime": pickup,
        "dropoff_datetime": pickup + pd.to_timedelta(duration, unit="m"),
        "PULocationID": rng.integers(1, 40, size=trip_count),
        "DOLocationID": rng.integers(1, 40, size=trip_count),
        "trip_miles": rng.uniform(0.5, 25.0, size=trip_count),
        "trip_time": duration * 60,
        "base_passenger_fare": rng.uniform(8, 130, size=trip_count),
        "tips": rng.uniform(0, 12, size=trip_count),
        "driver_pay": rng.uniform(5, 90, size=trip_count),
        "platform_name": rng.choice(["Uber", "Lyft"], size=trip_count),
        "estimated_service_tier": rng.choice(
            ["Standard", "Comfort", "Extra Comfort"], size=trip_count
        ),
        "service_date": day,
        "weekday": day.weekday(),
        "time_block": (minutes // 180).astype(int),
        "is_airport": rng.random(trip_count) < 0.1,
        "is_manhattan": rng.random(trip_count) < 0.4,
    })
    travel = {
        (a, b): float(rng.uniform(3, 40))
        for a in range(1, 40) for b in range(1, 40)
        if rng.random() < 0.7
    }
    return trips, profiles, fleet, travel


WEIGHTS = {"time": 0.3, "distance": 0.2, "airport": 0.2, "manhattan": 0.15, "tier": 0.15}


@pytest.mark.parametrize("bucket_size", [3, 6])
def test_bucket_streaming_matches_whole_day(bucket_size):
    """버킷을 흘려 처리한 결과 == 하루 후보를 통째로 만든 결과."""
    trips, profiles, fleet, travel = _attribution_fixture()
    kwargs = dict(
        global_seed=42, target_month="2026-01",
        bucket_size=bucket_size, score_weights=WEIGHTS,
    )

    whole, whole_pre = attribution.build_candidates(trips, profiles, fleet, **kwargs)
    whole_assigned, whole_post = attribution.allocate(whole, travel)

    streamed, streamed_counts, cand_rows, surv_rows = attribution.attribute_chunk(
        trips, profiles, fleet, travel, **kwargs
    )

    assert not whole_assigned.empty, "픽스처가 아무것도 배정하지 못하면 대조가 무의미합니다"
    pd.testing.assert_frame_equal(
        whole_assigned.sort_values("trip_key").reset_index(drop=True),
        streamed.sort_values("trip_key").reset_index(drop=True),
    )
    expected = {k: v for k, v in whole_pre.items() if not k.startswith("_")}
    for reason, value in whole_post.items():
        expected[reason] = expected.get(reason, 0) + value
    assert streamed_counts == expected
    assert cand_rows == whole_pre["_candidate_rows"]
    assert surv_rows == len(whole)


def test_bucket_streaming_peak_is_one_bucket():
    """최대 후보 프레임이 하루 전체가 아니라 버킷 하나 크기여야 합니다."""
    trips, profiles, fleet, _ = _attribution_fixture()
    kwargs = dict(
        global_seed=42, target_month="2026-01", bucket_size=3, score_weights=WEIGHTS,
    )
    side = attribution.prepare_drivers(profiles, fleet, bucket_size=3)
    seed = derive_seed(42, Stage.ALLOCATION_BUCKET, "2026-01")
    keyed = trips[attribution.TRIP_CANDIDATE_COLUMNS].copy()
    keyed["_bucket"] = attribution.trip_buckets(
        keyed["trip_key"], bucket_seed=seed, bucket_count=side.bucket_count
    )
    whole, _ = attribution.build_candidates(trips, profiles, fleet, **kwargs)
    peak = max(
        len(attribution.candidates_for(g, side, bucket_seed=seed, score_weights=WEIGHTS)[0])
        for _, g in keyed.groupby("_bucket")
    )
    assert peak * 2 < len(whole), f"버킷 최대 {peak} vs 하루 전체 {len(whole)} — 안 줄었습니다"


def test_tips_survive_attribution():
    """계약이 요구하는데 배정이 보지 않는 컬럼이 경계를 넘는가.

    `tips` 는 제약도 점수도 읽지 않아서 후보 컬럼을 추릴 때 가장 먼저 떨어지는
    부류입니다. 떨어져도 산출물은 만들어지고 행 수도 맞아서 조용히 지나갑니다 —
    그래서 값까지 대조합니다 (schema/bronze.py MONTHLY_TAXI_TRIP_SCHEMA).
    """
    trips, profiles, fleet, travel = _attribution_fixture()
    assigned, *_ = attribution.attribute_chunk(
        trips, profiles, fleet, travel,
        global_seed=42, target_month="2026-01", bucket_size=6, score_weights=WEIGHTS,
    )
    assert not assigned.empty
    assert "tips" in contract.trip_schema().names
    source = trips.set_index("trip_key")["tips"]
    got = assigned.set_index("trip_key")["tips"]
    pd.testing.assert_series_equal(got, source.loc[got.index], check_names=False)


def _publish_fixture():
    current = pd.DataFrame({
        "driver_id": ["DRIVER_0001", "DRIVER_0002"],
        "taxi_id": ["TOYOTA|CAMRY|2024#00000", "KIA|SOUL|2024#00000"],
        "traits_pool_month": ["2026-01", "2026-01"],
        "joined_on": pd.to_datetime(["2026-01-01", "2026-01-05"]),
        "exited_on": [pd.NaT, pd.Timestamp("2026-01-20")],
        "vehicle_since": pd.to_datetime(["2026-01-01", "2026-01-05"]),
    })
    master = pd.DataFrame({
        "vehicle_model_id": ["TOYOTA|CAMRY|2024", "KIA|SOUL|2024"],
        "make_key": ["TOYOTA", "KIA"],
        "model_key": ["CAMRY", "SOUL"],
        "model_year": [2024, 2024],
        "weekly_price_usd": [420.0, 380.0],
        "combined_mpg": [32.7, 30.5],
        "fuel_type": ["MIXED", "GAS"],
        "uber_comfort_eligible": [True, False],
        "lyft_extra_comfort_eligible": [False, False],
        "image_url": ["https://x/camry.jpg", "https://x/soul.jpg"],
    })
    fleet_units = master.assign(
        taxi_id=[f"{m}#00000" for m in master["vehicle_model_id"]]
    )
    return current, fleet_units, master


def test_published_frames_match_contract(monkeypatch):
    """세 산출물이 `schema/bronze.py` 로 그대로 캐스팅되는가.

    컬럼 하나가 빠지거나 남거나 타입이 안 맞으면 실제 실행에서 파일이 안 나옵니다.
    그 사고를 데이터 없이 붙잡는 유일한 지점입니다 — 구역 이름 조회는 여기서 대체합니다.
    """
    trips, profiles, fleet, travel = _attribution_fixture()
    assigned, *_ = attribution.attribute_chunk(
        trips, profiles, fleet, travel,
        global_seed=42, target_month="2026-01", bucket_size=6, score_weights=WEIGHTS,
    )
    assert not assigned.empty
    monkeypatch.setattr(
        curated, "load_zone_names",
        lambda: pd.Series({i: f"ZONE_{i}" for i in range(1, 41)}),
    )
    current, fleet_units, master = _publish_fixture()
    built = [
        (published.build_trip_snapshot(assigned),
         contract.trip_schema()),
        (published.build_driver_vehicle(
            current, fleet_units, target_month="2026-01",
            created_at="2026-02-01T00:00:00+00:00", global_seed=42),
         contract.driver_vehicle_schema()),
        (published.build_vehicle_inventory(master, fleet_units),
         contract.vehicle_inventory_schema()),
    ]
    for frame, schema in built:
        assert list(frame.columns) == schema.names, schema.names
        table = published._as_table(frame, schema)
        assert table.schema.equals(schema), table.schema


def test_experience_years_does_not_shift_driver_traits():
    """계약용 랜덤값이 기존 기준값 추첨을 밀지 않는가 (배정 결과 불변의 근거)."""
    before = synthesize.base_traits(
        "DRIVER_0007", global_seed=SEED, traits_pool_month="2026-01", trip_pool=POOL
    )
    published._experience_years(pd.Series(["DRIVER_0007"] * 50), global_seed=SEED)
    after = synthesize.base_traits(
        "DRIVER_0007", global_seed=SEED, traits_pool_month="2026-01", trip_pool=POOL
    )
    assert before == after
