"""합성 회사 고객·택시·리스 원천 스냅샷 생성 시나리오.

1. 기사 2,000명 → 고객·고유 택시·활성 계약 각 2,000건
2. BOTH/STANDARD/SINGLE → 400/1,200/400명과 실제 등급 조건 일치
3. 같은 입력과 seed → ID·차량·리스 시작일 동일
4. 리스 시작일 → 2023-01-01~2026-08-12 범위
5. 기사 수 또는 차량 후보 그룹 부족 → 명시적 실패
6. 저장한 세 스냅샷 → PK/FK와 스키마 보존
"""

from datetime import date

import pandas as pd
import pytest

from scripts.synthetic_company_snapshot.snapshot import (
    build_company_snapshot,
    build_vehicle_pool,
    driver_ids_from_mapping,
    write_snapshot,
)


def _driver_ids() -> list[str]:
    return [f"DRIVER_{index:06d}" for index in range(2_000)]


def _vehicle_pool() -> pd.DataFrame:
    catalog = pd.DataFrame([
        {"make_key": "A", "model_key": "BOTH", "weekly_price_usd": 700.0},
        {"make_key": "B", "model_key": "STANDARD", "weekly_price_usd": 500.0},
        {"make_key": "C", "model_key": "UBER_ONLY", "weekly_price_usd": 600.0},
        {"make_key": "D", "model_key": "LYFT_ONLY", "weekly_price_usd": 650.0},
    ])
    uber = pd.DataFrame([
        {"make_key": "A", "model_key": "BOTH", "product": "Comfort", "min_year": 2020},
        {"make_key": "C", "model_key": "UBER_ONLY", "product": "Comfort", "min_year": 2020},
    ])
    lyft = pd.DataFrame([
        {"make_key": "A", "model_key": "BOTH", "product": "Extra Comfort", "min_year": 2020},
        {"make_key": "D", "model_key": "LYFT_ONLY", "product": "Extra Comfort", "min_year": 2020},
    ])
    return build_vehicle_pool(catalog, uber, lyft)


def test_기사마다_고객_고유택시_활성계약을_하나씩_생성한다():
    tables = build_company_snapshot(_driver_ids(), _vehicle_pool())

    assert len(tables.customer) == len(tables.taxi) == len(tables.lease_contract) == 2_000
    assert tables.customer["customer_id"].is_unique
    assert tables.customer["synthetic_driver_id"].is_unique
    assert tables.taxi["taxi_id"].is_unique
    assert tables.lease_contract["lease_id"].is_unique
    assert tables.lease_contract["lease_ended_on"].isna().all()


def test_차량그룹별_배정수와_등급조건이_일치한다():
    taxis = build_company_snapshot(_driver_ids(), _vehicle_pool()).taxi

    assert taxis["vehicle_group"].value_counts().to_dict() == {
        "STANDARD": 1_200,
        "BOTH": 400,
        "SINGLE": 400,
    }
    count = taxis[["uber_comfort_eligible", "lyft_extra_comfort_eligible"]].sum(axis=1)
    assert (count[taxis["vehicle_group"] == "BOTH"] == 2).all()
    assert (count[taxis["vehicle_group"] == "STANDARD"] == 0).all()
    assert (count[taxis["vehicle_group"] == "SINGLE"] == 1).all()


def test_같은_seed로_전체_스냅샷이_동일하다():
    first = build_company_snapshot(_driver_ids(), _vehicle_pool(), seed=42)
    second = build_company_snapshot(_driver_ids(), _vehicle_pool(), seed=42)

    for name in ("customer", "taxi", "lease_contract"):
        pd.testing.assert_frame_equal(getattr(first, name), getattr(second, name))


def test_리스시작일이_지정한_기간_안에_있다():
    started = pd.to_datetime(
        build_company_snapshot(_driver_ids(), _vehicle_pool()).lease_contract["lease_started_on"]
    )

    assert started.min().date() >= date(2023, 1, 1)
    assert started.max().date() <= date(2026, 8, 12)


def test_기사수와_차량후보가_부족하면_실패한다():
    with pytest.raises(ValueError, match="2,000명"):
        driver_ids_from_mapping(pd.DataFrame({"synthetic_driver_id": _driver_ids()[:-1]}))
    with pytest.raises(ValueError, match="차량 후보가 없는 그룹"):
        build_company_snapshot(
            _driver_ids(),
            _vehicle_pool().query("vehicle_group != 'BOTH'"),
        )


def test_저장한_세_스냅샷의_pk_fk와_스키마가_보존된다(tmp_path):
    tables = build_company_snapshot(_driver_ids(), _vehicle_pool())
    paths = write_snapshot(tables, tmp_path, date(2026, 8, 12))
    written = {path.stem: pd.read_parquet(path) for path in paths}

    assert set(written) == {"customer", "taxi", "lease_contract"}
    assert set(written["lease_contract"]["customer_id"]) == set(written["customer"]["customer_id"])
    assert set(written["lease_contract"]["taxi_id"]) == set(written["taxi"]["taxi_id"])
    assert set(written["taxi"].columns) >= {
        "taxi_id", "make_key", "model_key", "model_year", "vehicle_group", "snapshot_date"
    }
