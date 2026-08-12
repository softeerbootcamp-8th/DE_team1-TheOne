"""driver_master 기사 ID 생성 시나리오.

1. 같은 trait 입력과 seed로 재실행하면 기사별 ID가 동일
2. 한 실행의 서로 다른 기사 행은 서로 다른 ID를 가짐
3. seed가 다르면 같은 기사 순번의 ID도 달라짐
4. seed가 없어도 실행 내 ID는 중복되지 않음
"""

from types import SimpleNamespace

import numpy as np
import pandas as pd

from jobs.driver_master import aggregate


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
