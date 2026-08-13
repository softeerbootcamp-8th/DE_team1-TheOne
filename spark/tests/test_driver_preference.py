"""운행 배정용 기사 선호 마스터 시나리오. 이슈 #286.

1. 기사 ID마다 선호 한 행 생성 및 허용된 요일·시간대 사용
2. 시간대 가중치 합계 1과 거리 구간 경계 일치
3. 점수·작업량·공차 한도 범위 보장
4. 같은 seed 재실행 결과 동일
5. 기존 선호 보존 및 신규 기사만 동일 스키마로 추가
6. Parquet 저장 후 리스트·숫자 타입 보존
7. 빈 값·중복 기사 ID와 잘못된 기존 스키마 거부
"""

import numpy as np
import pandas as pd
import pytest

from jobs.driver_master.preference import (
    PREFERENCE_COLUMNS,
    build_driver_preferences,
    extend_driver_preferences,
    write_driver_preferences,
)
from jobs.driver_master.traits import TIME_BLOCK_LABELS, WEEKDAY_LABELS


def _pools() -> dict[str, np.ndarray]:
    return {
        "trip_miles": np.array([1.0, 3.0, 8.0]),
        "trip_time_min": np.array([10.0, 20.0, 30.0]),
    }


def _build(driver_ids=None) -> pd.DataFrame:
    return build_driver_preferences(
        driver_ids or ["DRIVER_000001", "DRIVER_000002"],
        _pools(),
        as_of_date=np.datetime64("2026-08-12"),
        seed=42,
    )


def test_기사마다_선호_한행과_허용된_요일_시간대를_생성한다():
    result = _build()

    assert result["driver_id"].is_unique
    assert set(result.columns) == set(PREFERENCE_COLUMNS)
    for row in result.itertuples():
        assert 3 <= len(row.active_weekdays) <= 7
        assert set(row.active_weekdays) <= set(WEEKDAY_LABELS)
        assert len(row.preferred_time_blocks) == 2
        assert set(row.preferred_time_blocks) <= set(TIME_BLOCK_LABELS)


def test_시간대_가중치와_거리구간이_값에_맞는다():
    result = _build(["DRIVER_000001", "DRIVER_000002", "DRIVER_000003"])

    for row in result.itertuples():
        assert len(row.time_block_weights) == 8
        assert sum(row.time_block_weights) == pytest.approx(1.0)
        expected = "SHORT" if row.preferred_distance_miles <= 1.93 else (
            "MEDIUM" if row.preferred_distance_miles <= 4.75 else "LONG"
        )
        assert row.preferred_distance_band == expected


def test_선호점수와_작업한도가_허용범위다():
    result = _build([f"DRIVER_{index:06d}" for index in range(100)])

    assert result["airport_preference"].between(0, 1).all()
    assert result["manhattan_preference"].between(0, 1).all()
    assert (result["target_daily_trips"] >= 1).all()
    assert result["target_work_minutes"].between(60, 720).all()
    assert result["max_deadhead_minutes"].between(5, 15).all()


def test_같은_기사와_seed는_입력순서와_무관하게_동일하다():
    first = _build(["DRIVER_000001", "DRIVER_000002"])
    second = _build(["DRIVER_000002", "DRIVER_000001"])

    pd.testing.assert_frame_equal(first, second)


def test_기존선호는_보존하고_신규기사만_추가한다():
    previous = _build(["DRIVER_000001", "DRIVER_000002"])
    result = extend_driver_preferences(
        previous,
        ["DRIVER_000001", "DRIVER_000002", "DRIVER_202609_000001"],
        _pools(),
        as_of_date=np.datetime64("2026-09-12"),
        seed=42,
    )

    pd.testing.assert_frame_equal(
        result[result["driver_id"].isin(previous["driver_id"])].reset_index(drop=True),
        previous.reset_index(drop=True),
    )
    assert result.iloc[-1]["driver_id"] == "DRIVER_202609_000001"
    assert list(result.columns) == PREFERENCE_COLUMNS


def test_parquet_저장후_리스트와_숫자타입이_보존된다(tmp_path):
    path = write_driver_preferences(_build(), tmp_path / "driver_preference.parquet")
    written = pd.read_parquet(path)

    assert isinstance(written.iloc[0]["active_weekdays"], np.ndarray)
    assert isinstance(written.iloc[0]["time_block_weights"], np.ndarray)
    assert pd.api.types.is_float_dtype(written["airport_preference"])
    assert pd.api.types.is_integer_dtype(written["target_daily_trips"])


@pytest.mark.parametrize("driver_ids", [[], [""], ["DRIVER_1", "DRIVER_1"]])
def test_빈값과_중복기사_id를_거부한다(driver_ids):
    with pytest.raises(ValueError, match="driver_id"):
        build_driver_preferences(
            driver_ids, _pools(), as_of_date=np.datetime64("2026-08-12")
        )


def test_기존선호_스키마가_깨지면_갱신을_거부한다():
    with pytest.raises(ValueError, match="컬럼 누락"):
        extend_driver_preferences(
            _build().drop(columns="active_weekdays"),
            ["DRIVER_000001"],
            _pools(),
            as_of_date=np.datetime64("2026-09-12"),
        )
