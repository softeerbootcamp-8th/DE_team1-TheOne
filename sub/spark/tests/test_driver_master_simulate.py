"""기사 1명 근무일 시뮬레이션(`simulate.py`) 불변식 검증. 이슈 #205.

1. `_active_dates` 요일 계산 — `active_weekdays=[0]`이면 결과가 전부 월요일 (`+3` 오프셋 검증)
2. 같은 seed로 두 번 돌리면 결과 배열이 완전히 동일 (재현성)
3. `churn_at <= joined_at`이면 빈 로그 반환 — 예외 없이 처리
4. `MAX_SIMULATION_DAYS`를 넘는 재직 구간이 90일로 잘림
5. `rest_minutes <= work_minutes`, `trip_count >= 0` 불변식
6. `_distance_bucket_probs` 합이 1.0(오차 1e-9)이고 음수가 없음
"""

import numpy as np

from sub.spark.jobs.driver_master import simulate


def _trait_row(**overrides) -> dict:
    row = {
        "joined_at": np.datetime64("2024-01-01"),
        "active_weekdays": [0, 1, 2, 3, 4, 5, 6],
        "work_cv": 0.35,
        "work_mean_h": 7.2,
        "rest_frac": 0.10,
        "idle_frac": 0.25,
        "avg_trip_duration_min": 18.0,
        "distance_pref_mi": 3.0,
        "time_pref": np.full(8, 1 / 8),
    }
    row.update(overrides)
    return row


def test_active_weekdays가_월요일뿐이면_결과_날짜가_전부_월요일이다():
    joined_at = np.datetime64("2024-01-01")
    today = joined_at + np.timedelta64(28, "D")

    dates = simulate._active_dates(joined_at, None, today, active_weekdays=[0])

    assert len(dates) > 0
    weekdays = [date.astype("datetime64[D]").astype(object).weekday() for date in dates]
    assert all(weekday == 0 for weekday in weekdays)


def test_같은_seed로_두번_돌리면_결과가_완전히_동일하다():
    trait_row = _trait_row()
    today = trait_row["joined_at"] + np.timedelta64(30, "D")

    first = simulate.simulate_driver(trait_row, today, None, np.random.default_rng(42))
    second = simulate.simulate_driver(trait_row, today, None, np.random.default_rng(42))

    assert np.array_equal(first.dates, second.dates)
    assert np.array_equal(first.work_minutes, second.work_minutes)
    assert np.array_equal(first.rest_minutes, second.rest_minutes)
    assert np.array_equal(first.idle_seconds, second.idle_seconds)
    assert np.array_equal(first.trip_count, second.trip_count)
    assert np.array_equal(first.distance_bucket_counts, second.distance_bucket_counts)
    assert np.array_equal(first.time_block_counts, second.time_block_counts)


def test_churn_at가_joined_at_이전이면_빈_로그를_반환한다():
    joined_at = np.datetime64("2024-06-01")
    churn_at = joined_at - np.timedelta64(1, "D")
    trait_row = _trait_row(joined_at=joined_at)

    log = simulate.simulate_driver(trait_row, joined_at + np.timedelta64(10, "D"), churn_at, np.random.default_rng(1))

    assert len(log.dates) == 0
    assert len(log.work_minutes) == 0
    assert len(log.rest_minutes) == 0
    assert len(log.trip_count) == 0
    assert np.array_equal(log.distance_bucket_counts, np.zeros(3))
    assert np.array_equal(log.time_block_counts, np.zeros(8))


def test_churn_at가_joined_at와_같으면_빈_로그를_반환한다():
    joined_at = np.datetime64("2024-06-01")
    trait_row = _trait_row(joined_at=joined_at)

    log = simulate.simulate_driver(trait_row, joined_at + np.timedelta64(10, "D"), joined_at, np.random.default_rng(1))

    assert len(log.dates) == 0


def test_재직_구간이_90일을_넘으면_활성일이_90일로_잘린다():
    joined_at = np.datetime64("2024-01-01")
    today = joined_at + np.timedelta64(200, "D")

    dates = simulate._active_dates(joined_at, None, today, active_weekdays=[0, 1, 2, 3, 4, 5, 6])

    assert len(dates) == simulate.MAX_SIMULATION_DAYS
    assert dates.max() < joined_at + np.timedelta64(simulate.MAX_SIMULATION_DAYS, "D")


def test_rest_minutes는_work_minutes를_넘지_않고_trip_count는_음수가_아니다():
    trait_row = _trait_row()
    today = trait_row["joined_at"] + np.timedelta64(90, "D")

    for seed in range(10):
        log = simulate.simulate_driver(trait_row, today, None, np.random.default_rng(seed))

        assert np.all(log.rest_minutes <= log.work_minutes)
        assert np.all(log.trip_count >= 0)


def test_distance_bucket_probs_합은_1이고_음수가_없다():
    for distance_pref_mi in [0.5, 1.93, 3.0, 4.75, 20.0]:
        probs = simulate._distance_bucket_probs(distance_pref_mi)

        assert abs(probs.sum() - 1.0) < 1e-9
        assert np.all(probs >= 0)
