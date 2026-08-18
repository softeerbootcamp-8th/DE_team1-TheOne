"""driver_master 기사 ID 생성 시나리오 및 CSV 계약 검증. 이슈 #206.

1. 같은 trait 입력과 seed로 재실행하면 기사별 ID가 동일
2. 한 실행의 서로 다른 기사 행은 서로 다른 ID를 가짐
3. seed가 다르면 같은 기사 순번의 ID도 달라짐
4. seed가 없어도 실행 내 ID는 중복되지 않음
5. `_resolve_churn_at` — 재직 30일 미만이면 None, 이상이면 joined_at 초과 today 이하
6. `_top_categories` — 빈 카운트/균등 분포에서도 빈 리스트를 내지 않음
7. `job.main` 을 같은 seed/today로 두 번 돌리면 완전히 같은 CSV
8. 리스트 필드 3개가 `|`로 합쳐지고 값 안에 `|`나 콤마가 섞이지 않음
9. 출력 컬럼 집합이 `aggregate_driver` 반환 키와 정확히 일치
10. `--today`를 비우면 실행 시각 UTC 날짜를 씀
"""

from types import SimpleNamespace

import numpy as np
import pandas as pd

from sub.spark.jobs.driver_master import aggregate, job
from sub.spark.jobs.driver_master.traits import DISTANCE_LABELS, TIME_BLOCK_LABELS, sample_driver_traits


def _traits() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "driver_name": "Alex Kim",
                "joined_at": np.datetime64("2025-01-01"),
                "churn_flag": False,
                "active_weekdays": [0, 2, 4],
            },
            {
                "driver_name": "Jamie Lee",
                "joined_at": np.datetime64("2025-02-01"),
                "churn_flag": False,
                "active_weekdays": [1, 3, 5],
            },
        ]
    )


def _empty_log() -> SimpleNamespace:
    return SimpleNamespace(
        work_minutes=np.array([]),
        rest_minutes=np.array([]),
        idle_seconds=np.array([]),
        trip_count=np.array([]),
        distance_bucket_counts=np.zeros(3, dtype=int),
        time_block_counts=np.zeros(8, dtype=int),
    )


def _build(monkeypatch, seed: int | None) -> pd.DataFrame:
    monkeypatch.setattr(aggregate, "simulate_driver", lambda *args: _empty_log())
    return aggregate.build_driver_master_table(
        _traits(), today=np.datetime64("2026-08-12"), seed=seed
    )


def test_같은_입력과_seed로_재실행하면_driver_id가_같다(monkeypatch):
    first = _build(monkeypatch, seed=42)
    second = _build(monkeypatch, seed=42)

    assert first["driver_id"].tolist() == second["driver_id"].tolist()


def test_한_실행의_기사별_driver_id는_서로_다르다(monkeypatch):
    result = _build(monkeypatch, seed=42)

    assert result["driver_id"].is_unique


def test_seed가_다르면_driver_id도_달라진다(monkeypatch):
    first = _build(monkeypatch, seed=42)
    second = _build(monkeypatch, seed=43)

    assert first["driver_id"].tolist() != second["driver_id"].tolist()


def test_seed가_없어도_실행_내_driver_id는_중복되지_않는다(monkeypatch):
    result = _build(monkeypatch, seed=None)

    assert result["driver_id"].is_unique


def test_재직_30일_미만이면_churn_at은_None이다():
    joined_at = np.datetime64("2026-01-01")
    today = joined_at + np.timedelta64(20, "D")

    result = aggregate._resolve_churn_at(joined_at, today, churn_flag=True, rng=np.random.default_rng(1))

    assert result is None


def test_churn_flag가_False이면_재직_기간과_무관하게_churn_at은_None이다():
    joined_at = np.datetime64("2026-01-01")
    today = joined_at + np.timedelta64(200, "D")

    result = aggregate._resolve_churn_at(joined_at, today, churn_flag=False, rng=np.random.default_rng(1))

    assert result is None


def test_재직_30일_이상이면_churn_at은_가입일_초과_오늘_이하이다():
    joined_at = np.datetime64("2026-01-01")
    today = joined_at + np.timedelta64(200, "D")

    for seed in range(10):
        result = aggregate._resolve_churn_at(joined_at, today, churn_flag=True, rng=np.random.default_rng(seed))

        assert result is not None
        assert joined_at < result <= today


def test_top_categories는_카운트가_0이어도_빈_리스트를_내지_않는다():
    result = aggregate._top_categories(np.zeros(8), TIME_BLOCK_LABELS)

    assert len(result) >= 1


def test_top_categories는_모든_카테고리가_20퍼센트_미만이어도_빈_리스트를_내지_않는다():
    counts = np.full(8, 10)  # 균등 분포, 카테고리당 12.5%

    result = aggregate._top_categories(counts, TIME_BLOCK_LABELS)

    assert len(result) >= 1


def test_top_categories는_점유율_20퍼센트_이상인_카테고리를_전부_반환한다():
    counts = np.array([50, 50, 0])  # SHORT/MEDIUM 각 50%

    result = aggregate._top_categories(counts, DISTANCE_LABELS)

    assert set(result) == {DISTANCE_LABELS[0], DISTANCE_LABELS[1]}


def _bootstrap_pools() -> dict[str, np.ndarray]:
    return {
        "trip_miles": np.array([0.8, 1.5, 3.0, 4.75, 8.0, 15.0]),
        "trip_time_min": np.array([5.0, 10.0, 15.0, 20.0, 30.0]),
    }


def test_main을_같은_seed_today로_두번_돌리면_완전히_같은_csv가_나온다(tmp_path, monkeypatch):
    monkeypatch.setattr(job, "load_bootstrap_pools", lambda **kwargs: _bootstrap_pools())
    first_path = tmp_path / "first.csv"
    second_path = tmp_path / "second.csv"

    job.main(["--seed", "1", "--today", "2026-01-01", "--n_drivers", "20", "--output_path", str(first_path)])
    job.main(["--seed", "1", "--today", "2026-01-01", "--n_drivers", "20", "--output_path", str(second_path)])

    assert first_path.read_text() == second_path.read_text()


def test_리스트_필드가_파이프로_합쳐지고_구분자가_섞이지_않는다(tmp_path, monkeypatch):
    monkeypatch.setattr(job, "load_bootstrap_pools", lambda **kwargs: _bootstrap_pools())
    output_path = tmp_path / "driver_master.csv"

    job.main(["--seed", "2", "--today", "2026-01-01", "--n_drivers", "10", "--output_path", str(output_path)])

    result = pd.read_csv(output_path)
    for field in job.LIST_FIELDS:
        assert not result[field].str.contains(",").any()
        for cell in result[field]:
            parts = cell.split(job.LIST_FIELD_SEP)
            assert all(part for part in parts)


def test_출력_컬럼이_aggregate_driver_반환_키와_정확히_일치한다(tmp_path, monkeypatch):
    monkeypatch.setattr(job, "load_bootstrap_pools", lambda **kwargs: _bootstrap_pools())
    output_path = tmp_path / "driver_master.csv"

    job.main(["--seed", "3", "--today", "2026-01-01", "--n_drivers", "5", "--output_path", str(output_path)])

    result = pd.read_csv(output_path)
    today = np.datetime64("2026-01-01")
    trait_row = sample_driver_traits(1, _bootstrap_pools(), today=today, seed=3).iloc[0]
    expected_keys = set(aggregate.aggregate_driver(trait_row, today, np.random.default_rng(3)).keys())

    assert set(result.columns) == expected_keys


def test_today를_비우면_실행_시각_UTC_날짜를_쓴다(tmp_path, monkeypatch):
    monkeypatch.setattr(job, "load_bootstrap_pools", lambda **kwargs: _bootstrap_pools())
    output_path = tmp_path / "driver_master.csv"

    fixed_now = pd.Timestamp("2026-03-05T12:00:00", tz="UTC").to_pydatetime()

    class _FixedDatetime(job.datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed_now

    monkeypatch.setattr(job, "datetime", _FixedDatetime)

    captured = {}
    original_build = job.build_driver_master_table

    def spy_build(traits_df, today, seed=None):
        captured["today"] = today
        return original_build(traits_df, today, seed=seed)

    monkeypatch.setattr(job, "build_driver_master_table", spy_build)

    job.main(["--seed", "4", "--n_drivers", "5", "--output_path", str(output_path)])

    assert captured["today"] == np.datetime64("2026-03-05")
