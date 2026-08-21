"""합성 회사 고객·택시·리스 원천 스냅샷 생성 시나리오.

1. 기사 2,000명 → 고객·고유 택시·활성 계약 각 2,000건
2. BOTH/STANDARD/SINGLE → 400/1,200/400명과 실제 등급 조건 일치
3. 같은 입력과 seed → ID·차량·리스 시작일 동일
4. 리스 시작일 → conftest 의 `TEST_LEASE_START_MIN` ~ `TEST_SNAPSHOT_DATE` 범위
5. 기사 수 또는 차량 후보 그룹 부족 → 명시적 실패
6. 저장한 세 스냅샷 → PK/FK와 스키마 보존
7. 월별 갱신 → 0.5~1% 계약 종료와 동일 수 신규 계약
8. 월별 갱신 → 이력·PK/FK·활성 계약 수 보존 및 결정적 재실행
9. 잘못된 월·변경률·전월 관계 → 명시적 실패
"""

from datetime import date

import pandas as pd
import pytest

from conftest import TEST_LEASE_START_MIN, TEST_MODEL_YEAR, TEST_SEED, TEST_SNAPSHOT_DATE
from sub.generators.synthetic_company_snapshot.snapshot import (
    build_company_snapshot,
    build_driver_ids,
    build_vehicle_pool,
    evolve_company_snapshot,
    read_snapshot,
    write_snapshot,
)


# 갱신 테스트에서 쓰는 "다음 달" — 기본 스냅샷과 같은 일자로 한 달 뒤.
NEXT_SNAPSHOT_DATE = TEST_SNAPSHOT_DATE.replace(
    year=TEST_SNAPSHOT_DATE.year + (TEST_SNAPSHOT_DATE.month == 12),
    month=TEST_SNAPSHOT_DATE.month % 12 + 1,
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
        {**row, "vendor": vendor, "min_year": 2020, "weekly_lease_fee": prices[row["model_key"]]}
        for row in rows
    ])


def _snapshot_kwargs(**overrides) -> dict:
    """기본값이 사라진 인자를 테스트 리터럴로 채웁니다 (conftest 소유)."""
    return {
        "seed": TEST_SEED,
        "snapshot_date": TEST_SNAPSHOT_DATE,
        "lease_start_min": TEST_LEASE_START_MIN,
        **overrides,
    }


def _vehicle_pool() -> pd.DataFrame:
    return build_vehicle_pool(_vehicle_master(), model_year=TEST_MODEL_YEAR)


def test_기사마다_고객_고유택시_활성계약을_하나씩_생성한다():
    tables = build_company_snapshot(_driver_ids(), _vehicle_pool(), **_snapshot_kwargs())

    assert len(tables.customer) == len(tables.taxi) == len(tables.lease_contract) == 2_000
    assert tables.customer["customer_id"].is_unique
    assert tables.customer["synthetic_driver_id"].is_unique
    assert tables.taxi["taxi_id"].is_unique
    assert tables.lease_contract["lease_id"].is_unique
    assert tables.lease_contract["lease_ended_on"].isna().all()


def test_차량그룹별_배정수와_등급조건이_일치한다():
    taxis = build_company_snapshot(_driver_ids(), _vehicle_pool(), **_snapshot_kwargs()).taxi

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
    first = build_company_snapshot(_driver_ids(), _vehicle_pool(), **_snapshot_kwargs())
    second = build_company_snapshot(_driver_ids(), _vehicle_pool(), **_snapshot_kwargs())

    for name in ("customer", "taxi", "lease_contract"):
        pd.testing.assert_frame_equal(getattr(first, name), getattr(second, name))


def test_리스시작일이_지정한_기간_안에_있다():
    started = pd.to_datetime(
        build_company_snapshot(_driver_ids(), _vehicle_pool(), **_snapshot_kwargs()).lease_contract["lease_started_on"]
    )

    # 리터럴을 다시 적지 않습니다 — 기본값을 바꾸면 이 테스트가 조용히 무의미해집니다.
    assert started.min().date() >= TEST_LEASE_START_MIN
    assert started.max().date() <= TEST_SNAPSHOT_DATE


def test_생성한_기사_ID는_2000개_고유이며_재현된다():
    driver_ids = build_driver_ids(2_000)

    assert len(driver_ids) == 2_000
    assert len(set(driver_ids)) == 2_000
    assert driver_ids == sorted(driver_ids)
    assert driver_ids == build_driver_ids(2_000)


def test_차량_마스터_컬럼_누락과_복수_업체는_거부한다():
    with pytest.raises(ValueError, match="필수 컬럼 누락"):
        build_vehicle_pool(_vehicle_master().drop(columns=["min_year"]), model_year=TEST_MODEL_YEAR)
    mixed = pd.concat([_vehicle_master(), _vehicle_master("othervendor")], ignore_index=True)
    with pytest.raises(ValueError, match="업체가 둘 이상"):
        build_vehicle_pool(mixed, model_year=TEST_MODEL_YEAR)


def test_기사수와_차량후보가_부족하면_실패한다():
    with pytest.raises(ValueError, match="1명 이상"):
        build_driver_ids(0)
    with pytest.raises(ValueError, match="차량 후보가 없는 그룹"):
        build_company_snapshot(
            _driver_ids(),
            _vehicle_pool().query("vehicle_group != 'BOTH'"),
            **_snapshot_kwargs(),
        )


def test_기사수와_그룹구성이_어긋나면_두_출처를_지목하고_실패한다():
    """총원은 config, 구성비는 GROUP_COUNTS 가 소유해서 둘이 갈릴 수 있습니다.

    갈리면 zip(strict=True) 가 원인을 알기 어려운 메시지로 죽으므로, 그 전에
    두 출처를 함께 지목하며 멈춰야 합니다.
    """
    with pytest.raises(ValueError, match="driver.initial_count.*GROUP_COUNTS 합"):
        build_company_snapshot(_driver_ids()[:1_999], _vehicle_pool(), **_snapshot_kwargs())


def test_중복된_기사_ID는_거부한다():
    duplicated = _driver_ids()[:-1] + [_driver_ids()[0]]
    with pytest.raises(ValueError, match="중복 없는"):
        build_company_snapshot(duplicated, _vehicle_pool(), **_snapshot_kwargs())


def test_저장한_세_스냅샷의_pk_fk와_스키마가_보존된다(tmp_path):
    tables = build_company_snapshot(_driver_ids(), _vehicle_pool(), **_snapshot_kwargs())
    paths = write_snapshot(tables, tmp_path, TEST_SNAPSHOT_DATE)
    written = {path.stem: pd.read_parquet(path) for path in paths}

    assert set(written) == {"customer", "taxi", "lease_contract"}
    assert set(written["lease_contract"]["customer_id"]) == set(written["customer"]["customer_id"])
    assert set(written["lease_contract"]["taxi_id"]) == set(written["taxi"]["taxi_id"])
    assert set(written["taxi"].columns) >= {
        "taxi_id", "make_key", "model_key", "model_year", "vehicle_group", "snapshot_date"
    }


def test_월별로_계약을_1퍼센트_해지하고_같은_수의_신규계약을_생성한다():
    previous = build_company_snapshot(_driver_ids(), _vehicle_pool(), **_snapshot_kwargs())
    current = evolve_company_snapshot(
        previous, _vehicle_pool(), seed=TEST_SEED, snapshot_date=NEXT_SNAPSHOT_DATE, change_rate=0.01,
    )

    ended = current.lease_contract["lease_ended_on"].notna().sum()
    active = current.lease_contract["lease_ended_on"].isna().sum()
    assert ended == 20
    assert len(current.customer) == len(current.taxi) == len(current.lease_contract) == 2_020
    assert active == 2_000


def test_월별_갱신은_기존관계를_보존하고_신규관계만_추가한다():
    previous = build_company_snapshot(_driver_ids(), _vehicle_pool(), **_snapshot_kwargs())
    current = evolve_company_snapshot(
        previous, _vehicle_pool(), seed=TEST_SEED, snapshot_date=NEXT_SNAPSHOT_DATE, change_rate=0.005,
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
    previous = build_company_snapshot(_driver_ids(), _vehicle_pool(), **_snapshot_kwargs())
    first = evolve_company_snapshot(previous, _vehicle_pool(), seed=TEST_SEED, snapshot_date=NEXT_SNAPSHOT_DATE)
    second = evolve_company_snapshot(previous, _vehicle_pool(), seed=TEST_SEED, snapshot_date=NEXT_SNAPSHOT_DATE)

    for name in ("customer", "taxi", "lease_contract"):
        pd.testing.assert_frame_equal(getattr(first, name), getattr(second, name))


def test_저장한_전월_스냅샷을_읽어_다음달로_갱신한다(tmp_path):
    previous = build_company_snapshot(_driver_ids(), _vehicle_pool(), **_snapshot_kwargs())
    partition = tmp_path / f"snapshot_date={TEST_SNAPSHOT_DATE.isoformat()}"
    write_snapshot(previous, tmp_path, TEST_SNAPSHOT_DATE)

    current = evolve_company_snapshot(
        read_snapshot(partition), _vehicle_pool(), seed=TEST_SEED, snapshot_date=NEXT_SNAPSHOT_DATE,
    )
    assert set(pd.to_datetime(current.customer["snapshot_date"]).dt.date) == {NEXT_SNAPSHOT_DATE}


@pytest.mark.parametrize("snapshot_date,change_rate,error", [
    (TEST_SNAPSHOT_DATE, 0.005, "늦어야"),
    (NEXT_SNAPSHOT_DATE, 0.004, "change_rate"),
    (NEXT_SNAPSHOT_DATE, 0.011, "change_rate"),
])
def test_월순서와_변경률이_범위를_벗어나면_실패한다(snapshot_date, change_rate, error):
    previous = build_company_snapshot(_driver_ids(), _vehicle_pool(), **_snapshot_kwargs())
    with pytest.raises(ValueError, match=error):
        evolve_company_snapshot(
            previous, _vehicle_pool(), seed=TEST_SEED, snapshot_date=snapshot_date, change_rate=change_rate,
        )


def test_전월_활성계약의_fk가_깨지면_실패한다():
    previous = build_company_snapshot(_driver_ids(), _vehicle_pool(), **_snapshot_kwargs())
    previous.lease_contract.loc[0, "customer_id"] = "missing"

    with pytest.raises(ValueError, match="customer_id"):
        evolve_company_snapshot(previous, _vehicle_pool(), seed=TEST_SEED, snapshot_date=NEXT_SNAPSHOT_DATE)


# --- 저장 타입 고정 (#353) -------------------------------------------------
#
# 초기 스냅샷은 모든 계약이 진행 중이라 `lease_ended_on` 이 전량 결측입니다.
# pandas 에 추론을 맡기면 Parquet 타입이 `null` 이 되고, Spark 가 날짜로 읽지
# 못해 기사 배정이 분석 단계에서 죽습니다. 게다가 계약이 종료되기 시작하면
# 타입이 달라져, 어느 달 스냅샷을 읽느냐에 따라 되기도 하고 안 되기도 합니다.


def _written_schemas(tmp_path, tables, snapshot_date):
    import pyarrow.parquet as pq

    paths = write_snapshot(tables, tmp_path, snapshot_date)
    return {path.stem: pq.ParquetFile(path).schema_arrow for path in paths}


def test_전량_결측이어도_lease_ended_on_은_날짜로_저장된다(tmp_path):
    import pyarrow as pa

    tables = build_company_snapshot(_driver_ids(), _vehicle_pool(), **_snapshot_kwargs())
    assert tables.lease_contract["lease_ended_on"].isna().all()  # 전제 확인

    schema = _written_schemas(tmp_path, tables, TEST_SNAPSHOT_DATE)["lease_contract"]

    assert schema.field("lease_ended_on").type == pa.date32()
    assert schema.field("lease_started_on").type == pa.date32()


def test_초기_스냅샷과_월별_갱신의_스키마가_같다(tmp_path):
    """계약이 종료되기 시작해도 타입이 바뀌면 안 됩니다."""
    previous = build_company_snapshot(_driver_ids(), _vehicle_pool(), **_snapshot_kwargs())
    current = evolve_company_snapshot(
        previous, _vehicle_pool(), seed=TEST_SEED, snapshot_date=NEXT_SNAPSHOT_DATE, change_rate=0.01,
    )
    assert current.lease_contract["lease_ended_on"].notna().any()  # 전제 확인

    initial = _written_schemas(tmp_path / "initial", previous, TEST_SNAPSHOT_DATE)
    evolved = _written_schemas(tmp_path / "evolved", current, NEXT_SNAPSHOT_DATE)

    assert initial == evolved


def test_스키마에_없는_컬럼이_빠지면_실패한다(tmp_path):
    tables = build_company_snapshot(_driver_ids(), _vehicle_pool(), **_snapshot_kwargs())
    tables.lease_contract.drop(columns=["lease_ended_on"], inplace=True)

    with pytest.raises(ValueError, match="lease_ended_on"):
        write_snapshot(tables, tmp_path, TEST_SNAPSHOT_DATE)
