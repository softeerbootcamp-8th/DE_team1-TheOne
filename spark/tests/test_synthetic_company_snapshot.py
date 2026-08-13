"""합성 회사 고객·택시·리스 원천 스냅샷 생성 시나리오.

1. 기사 2,000명 → 고객·고유 택시·활성 계약 각 2,000건
2. BOTH/STANDARD/SINGLE → 400/1,200/400명과 실제 등급 조건 일치
3. 같은 입력과 seed → ID·차량·리스 시작일 동일
4. 리스 시작일 → 2023-01-01~2026-08-12 범위
5. 기사 수 또는 차량 후보 그룹 부족 → 명시적 실패
6. 저장한 세 스냅샷 → PK/FK와 스키마 보존
7. 월별 갱신 → 0.5~1% 계약 종료와 동일 수 신규 계약
8. 월별 갱신 → 이력·PK/FK·활성 계약 수 보존 및 결정적 재실행
9. 잘못된 월·변경률·전월 관계 → 명시적 실패
"""

from datetime import date

import pandas as pd
import pytest

from scripts.synthetic_company_snapshot.snapshot import (
    build_company_snapshot,
    build_driver_ids,
    build_vehicle_pool,
    evolve_company_snapshot,
    read_snapshot,
    write_snapshot,
)


def _driver_ids() -> list[str]:
    return [f"DRIVER_{index:06d}" for index in range(2_000)]


def _vehicle_master(vendor: str = "fasttrack") -> pd.DataFrame:
    """차량 마스터는 (차종 × 플랫폼 × 상품) 한 행씩입니다. 등급이 없는 차종도 행이 있습니다."""
    prices = {"BOTH": 700.0, "STANDARD": 500.0, "UBER_ONLY": 600.0, "LYFT_ONLY": 650.0}
    rows = [
        {"make_key": "A", "model_key": "BOTH", "platform": "uber", "product": "Comfort"},
        {"make_key": "A", "model_key": "BOTH", "platform": "lyft", "product": "Extra Comfort"},
        {"make_key": "B", "model_key": "STANDARD", "platform": "uber", "product": "UberX"},
        {"make_key": "C", "model_key": "UBER_ONLY", "platform": "uber", "product": "Comfort"},
        {"make_key": "D", "model_key": "LYFT_ONLY", "platform": "lyft", "product": "Extra Comfort"},
    ]
    return pd.DataFrame([
        {**row, "vendor": vendor, "min_year": 2020, "weekly_price_usd": prices[row["model_key"]]}
        for row in rows
    ])


def _vehicle_pool() -> pd.DataFrame:
    return build_vehicle_pool(_vehicle_master())


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


def test_생성한_기사_ID는_2000개_고유이며_재현된다():
    driver_ids = build_driver_ids()

    assert len(driver_ids) == 2_000
    assert len(set(driver_ids)) == 2_000
    assert driver_ids == sorted(driver_ids)
    assert driver_ids == build_driver_ids()


def test_차량_마스터_컬럼_누락과_복수_업체는_거부한다():
    with pytest.raises(ValueError, match="필수 컬럼 누락"):
        build_vehicle_pool(_vehicle_master().drop(columns=["min_year"]))
    mixed = pd.concat([_vehicle_master(), _vehicle_master("othervendor")], ignore_index=True)
    with pytest.raises(ValueError, match="업체가 둘 이상"):
        build_vehicle_pool(mixed)


def test_기사수와_차량후보가_부족하면_실패한다():
    with pytest.raises(ValueError, match="1명 이상"):
        build_driver_ids(0)
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


def test_월별로_계약을_1퍼센트_해지하고_같은_수의_신규계약을_생성한다():
    previous = build_company_snapshot(_driver_ids(), _vehicle_pool())
    current = evolve_company_snapshot(
        previous, _vehicle_pool(), snapshot_date=date(2026, 9, 12), change_rate=0.01,
    )

    ended = current.lease_contract["lease_ended_on"].notna().sum()
    active = current.lease_contract["lease_ended_on"].isna().sum()
    assert ended == 20
    assert len(current.customer) == len(current.taxi) == len(current.lease_contract) == 2_020
    assert active == 2_000


def test_월별_갱신은_기존관계를_보존하고_신규관계만_추가한다():
    previous = build_company_snapshot(_driver_ids(), _vehicle_pool())
    current = evolve_company_snapshot(
        previous, _vehicle_pool(), snapshot_date=date(2026, 9, 12), change_rate=0.005,
    )

    assert set(previous.customer["customer_id"]).issubset(set(current.customer["customer_id"]))
    assert set(previous.taxi["taxi_id"]).issubset(set(current.taxi["taxi_id"]))
    assert set(previous.lease_contract["lease_id"]).issubset(set(current.lease_contract["lease_id"]))
    assert set(current.lease_contract["customer_id"]).issubset(set(current.customer["customer_id"]))
    assert set(current.lease_contract["taxi_id"]).issubset(set(current.taxi["taxi_id"]))
    active = current.lease_contract[current.lease_contract["lease_ended_on"].isna()]
    assert active["customer_id"].is_unique
    assert active["taxi_id"].is_unique


def test_월별_갱신은_같은_입력과_seed에서_동일하다():
    previous = build_company_snapshot(_driver_ids(), _vehicle_pool())
    first = evolve_company_snapshot(previous, _vehicle_pool(), snapshot_date=date(2026, 9, 12))
    second = evolve_company_snapshot(previous, _vehicle_pool(), snapshot_date=date(2026, 9, 12))

    for name in ("customer", "taxi", "lease_contract"):
        pd.testing.assert_frame_equal(getattr(first, name), getattr(second, name))


def test_저장한_전월_스냅샷을_읽어_다음달로_갱신한다(tmp_path):
    previous = build_company_snapshot(_driver_ids(), _vehicle_pool())
    partition = tmp_path / "snapshot_date=2026-08-12"
    write_snapshot(previous, tmp_path, date(2026, 8, 12))

    current = evolve_company_snapshot(
        read_snapshot(partition), _vehicle_pool(), snapshot_date=date(2026, 9, 12),
    )
    assert set(pd.to_datetime(current.customer["snapshot_date"]).dt.date) == {date(2026, 9, 12)}


@pytest.mark.parametrize("snapshot_date,change_rate,error", [
    (date(2026, 8, 12), 0.005, "늦어야"),
    (date(2026, 9, 12), 0.004, "change_rate"),
    (date(2026, 9, 12), 0.011, "change_rate"),
])
def test_월순서와_변경률이_범위를_벗어나면_실패한다(snapshot_date, change_rate, error):
    previous = build_company_snapshot(_driver_ids(), _vehicle_pool())
    with pytest.raises(ValueError, match=error):
        evolve_company_snapshot(
            previous, _vehicle_pool(), snapshot_date=snapshot_date, change_rate=change_rate,
        )


def test_전월_활성계약의_fk가_깨지면_실패한다():
    previous = build_company_snapshot(_driver_ids(), _vehicle_pool())
    previous.lease_contract.loc[0, "customer_id"] = "missing"

    with pytest.raises(ValueError, match="customer_id"):
        evolve_company_snapshot(previous, _vehicle_pool(), snapshot_date=date(2026, 9, 12))
