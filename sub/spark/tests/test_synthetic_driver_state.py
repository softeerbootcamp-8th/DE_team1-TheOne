"""`sub/generators/synthetic_driver_state`의 체크포인트 계약.

config_hash가 다른 전월은 이어받지 않고, 임의 과거 월부터 재개 가능해야
한다 (blue_print.md 4.3).

`sub/prototype/synthesize.py`와의 동등성 테스트는 마이그레이션 검증(#605,
#609) 완료 후 prototype과 함께 제거했다.

#974: 첫 달에 만든 전체 차량 목록과 차종별 대수는 이후 달에도 유지한다.
"""

from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd
import pytest
from conftest import TEST_CONFIG_DATA

from sub.config import build_config
from sub.generators.synthetic_driver_state import adapters, checkpoint, events, traits
from sub.generators.synthetic_driver_state.lifecycle import synthesize_month
from sub.run_context import RunContext
from sub.spark.jobs.driver_assignment.candidates import REQUIRED as CANDIDATES_REQUIRED
from sub.spark.jobs.driver_master.preference import PREFERENCE_COLUMNS

SEED = 42
POOL = {
    "trip_miles": np.linspace(0.5, 30.0, 500),
    "trip_time_min": np.linspace(3.0, 90.0, 500),
}


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
        previous_current=None, previous_events=None, previous_noise=None,
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
        previous_current=None, previous_events=None, previous_noise=None,
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
        previous_current=None, previous_events=None, previous_noise=None,
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
        previous_current=None, previous_events=None, previous_noise=None,
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
        previous_current=None, previous_events=None, previous_noise=None,
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
        previous_noise=first.noise_state,
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


def test_published_현재상태로_재생한_노이즈는_전월_체크포인트와_같다():
    """published에는 노이즈를 중복 저장하지 않아도 가입 월부터 정확히 재생됩니다."""
    config, master = _config(50), _vehicle_master()
    first = synthesize_month(
        target_month="2024-01", config=config, vehicle_master=master, trip_pool=POOL,
        previous_current=None, previous_events=None, previous_noise=None,
    )
    second = synthesize_month(
        target_month="2024-02", config=config, vehicle_master=master, trip_pool=POOL,
        previous_current=first.current, previous_events=first.events,
        previous_noise=first.noise_state,
    )

    replayed = traits.replay_noise_state(
        second.current, through_month="2024-02", config=config
    )

    pd.testing.assert_frame_equal(
        replayed.sort_values("driver_id").reset_index(drop=True),
        second.noise_state.sort_values("driver_id").reset_index(drop=True),
    )


def test_연속된_두달의_전체차량과_차종별대수는_같다():
    config, master = _config(400), _vehicle_master()
    first = synthesize_month(
        target_month="2024-01", config=config, vehicle_master=master, trip_pool=POOL,
        previous_current=None, previous_events=None, previous_noise=None,
    )
    second = synthesize_month(
        target_month="2024-02", config=config, vehicle_master=master, trip_pool=POOL,
        previous_current=first.current, previous_events=first.events,
        previous_noise=first.noise_state,
    )

    first_fleet = first.fleet_units.sort_values("taxi_id").reset_index(drop=True)
    second_fleet = second.fleet_units.sort_values("taxi_id").reset_index(drop=True)
    pd.testing.assert_frame_equal(first_fleet, second_fleet)
    assert first_fleet["taxi_id"].is_unique

    active = second.current.loc[second.current["exited_on"].isna(), "taxi_id"].astype(str)
    assert active.is_unique
    assert set(active) <= set(second_fleet["taxi_id"].astype(str))


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
        previous_current=None, previous_events=None, previous_noise=None,
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



# ── 체크포인트 S3 저장소와 유실 감지 (#763) ─────────────────────────────

S3_BUCKET = "test-de-theone"
S3_REGION = "ap-northeast-2"


def _synthesize(month: str, config):
    return synthesize_month(
        target_month=month, config=config, vehicle_master=_vehicle_master(), trip_pool=POOL,
        previous_current=None, previous_events=None, previous_noise=None,
    )


def _write_kwargs(result):
    return dict(
        events=result.events, events_all=result.events, current=result.current,
        noise=result.noise_state, previous_month_value=None, previous_run_id=None,
        clip_rate=result.clip_rate,
    )


def _bootstrap_config(month: str, initial_count: int = 50):
    """`month` 를 첫 달로 지정한 설정. 그 달은 전월 없이 돌 수 있습니다."""
    data = {k: (dict(v) if isinstance(v, dict) else v) for k, v in TEST_CONFIG_DATA.items()}
    data["driver"] = {**data["driver"], "initial_count": initial_count}
    data["bootstrap"] = {**data["bootstrap"], "snapshot_date": f"{month}-01"}
    return build_config(data)


def test_첫_달은_전월_체크포인트가_없어도_된다(tmp_path):
    config = _bootstrap_config("2024-01")
    run = RunContext.create("2024-01", config)

    assert checkpoint.resolve_previous_checkpoint(tmp_path, run) == (None, None, None, None, None)


def test_첫_달이_아닌데_전월이_없으면_실패한다(tmp_path):
    """전에는 이때도 부트스트랩으로 취급해 초기 스냅샷을 조용히 만들었습니다.

    EC2 컨테이너에 `data/` 볼륨이 없어 재생성될 때마다 실제로 그랬습니다 — 기사
    2000명이 그 달에 새로 입사한 데이터가 에러 없이 나왔습니다.
    """
    config = _bootstrap_config("2024-01")
    run = RunContext.create("2024-03", config)

    with pytest.raises(checkpoint.CheckpointLineageError) as error:
        checkpoint.resolve_previous_checkpoint(tmp_path, run)

    message = str(error.value)
    assert "2024-02" in message           # 없는 전월을 지목
    assert "2024-01" in message           # 어디부터 생성해야 하는지
    assert "기사 연속성" in message        # 그대로 두면 무엇이 깨지는지


def test_다른_달은_있는데_전월만_없으면_그것을_알려준다(tmp_path):
    config = _bootstrap_config("2024-01")
    result = _synthesize("2024-01", config)
    checkpoint.write_checkpoint(
        tmp_path, RunContext.create("2024-01", config), **_write_kwargs(result)
    )
    run = RunContext.create("2024-03", config)

    with pytest.raises(checkpoint.CheckpointLineageError, match="다른 달 체크포인트는 있습니다"):
        checkpoint.resolve_previous_checkpoint(tmp_path, run)


def test_알_수_없는_storage는_거부한다(tmp_path):
    with pytest.raises(ValueError, match="알 수 없는 storage"):
        checkpoint.build_store(tmp_path, storage="gcs")


def test_S3에_버킷이_없으면_무엇을_설정해야_하는지_알려준다(tmp_path):
    with pytest.raises(ValueError, match="DATA_LAKE_S3_BUCKET"):
        checkpoint.build_store(tmp_path, storage="s3", bucket=None)


def _s3_client():
    import boto3

    client = boto3.client("s3", region_name=S3_REGION)
    client.create_bucket(
        Bucket=S3_BUCKET, CreateBucketConfiguration={"LocationConstraint": S3_REGION}
    )
    return client


def test_S3에_쓰고_그대로_읽는다(tmp_path):
    """EMR 워커는 컨테이너 로컬 디스크를 못 봅니다 — S3 왕복이 성립해야 합니다."""
    from moto import mock_aws

    config = _bootstrap_config("2024-01")
    result = _synthesize("2024-01", config)
    run = RunContext.create("2024-01", config)

    with mock_aws():
        _s3_client()
        location = checkpoint.write_checkpoint(
            tmp_path, run, storage="s3", bucket=S3_BUCKET, **_write_kwargs(result)
        )
        current, events_all, noise, manifest = checkpoint.read_checkpoint(
            tmp_path, "2024-01", storage="s3", bucket=S3_BUCKET
        )

    assert location.startswith(f"s3://{S3_BUCKET}/")
    pd.testing.assert_frame_equal(current, result.current)
    pd.testing.assert_frame_equal(events_all, result.events)
    pd.testing.assert_frame_equal(noise, result.noise_state)
    assert manifest["run_id"] == run.run_id


def test_S3에서도_같은_run_id면_다시_쓰지_않는다(tmp_path):
    from moto import mock_aws

    config = _bootstrap_config("2024-01")
    result = _synthesize("2024-01", config)
    run = RunContext.create("2024-01", config)
    kwargs = dict(storage="s3", bucket=S3_BUCKET, **_write_kwargs(result))

    with mock_aws():
        _s3_client()
        first = checkpoint.write_checkpoint(tmp_path, run, **kwargs)
        second = checkpoint.write_checkpoint(tmp_path, run, **kwargs)

    assert first == second


def test_S3에서_전월을_이어받는다(tmp_path):
    from moto import mock_aws

    config = _bootstrap_config("2024-01")
    first = _synthesize("2024-01", config)

    with mock_aws():
        _s3_client()
        checkpoint.write_checkpoint(
            tmp_path, RunContext.create("2024-01", config),
            storage="s3", bucket=S3_BUCKET, **_write_kwargs(first),
        )
        current, events_all, noise, prev_month, prev_run_id = (
            checkpoint.resolve_previous_checkpoint(
                tmp_path, RunContext.create("2024-02", config),
                storage="s3", bucket=S3_BUCKET,
            )
        )

    assert prev_month == "2024-01"
    assert prev_run_id is not None
    pd.testing.assert_frame_equal(current, first.current)


def test_S3에_전월이_없으면_유실로_실패한다(tmp_path):
    """로컬과 같은 판정을 S3 에서도 해야 합니다."""
    from moto import mock_aws

    config = _bootstrap_config("2024-01")

    with mock_aws():
        _s3_client()
        with pytest.raises(checkpoint.CheckpointLineageError, match="기사 연속성"):
            checkpoint.resolve_previous_checkpoint(
                tmp_path, RunContext.create("2024-03", config),
                storage="s3", bucket=S3_BUCKET,
            )
