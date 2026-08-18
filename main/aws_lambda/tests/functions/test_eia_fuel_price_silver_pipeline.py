"""휘발유·전력 CLEAN Silver 를 통합 연료비 Silver 로 붙이는 시나리오. 이슈 #518.

원본 파싱과 일별 확장은 여기서 검증하지 않습니다 — 각 CLEAN 파이프라인의 테스트가
이미 봅니다(`test_eia_gas_price_pipeline.py`, `test_eia_electricity_price_pipeline.py`).
이 파일이 보는 것은 **붙이는 일** 뿐입니다.

1. 날짜로 붙여 통합 스키마 그대로 산출
2. 계보는 두 수집분 중 **이른 쪽**. 두 원천은 다른 DAG 가 받아 수집일이 다를 수 있음
3. 한쪽 CLEAN 에 날짜가 빠지면 어느 데이터셋인지 알려주며 실패 — 그냥 붙이면 그 날이
   조용히 사라지고 Gold 집계가 줄어듦
4. CLEAN 안에 날짜가 중복되면 실패
5. 단가가 허용 범위 밖이면 실패
6. 입력 CLEAN 이 없으면 돌려야 할 DAG 를 알려주며 실패
"""

from datetime import date

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from main.aws_lambda.functions.eia_fuel_price_silver.extractor import (
    EiaFuelPriceCleanExtractor,
    clean_silver_file,
)
from main.aws_lambda.functions.eia_fuel_price_silver.handler import lambda_handler
from main.aws_lambda.functions.eia_fuel_price_silver.transformer import combine_daily_prices
from schema.silver.ev_charging_price import SCHEMA as EV_SCHEMA
from schema.silver.gas_ev_price import EIA, SCHEMA
from schema.silver.gas_price import SCHEMA as GAS_SCHEMA

GAS_COLLECTED = date(2026, 8, 10)
EV_COLLECTED = date(2026, 8, 17)


def _gas_rows(days=31, price=3.4, collected=GAS_COLLECTED):
    return [
        {"date": date(2025, 5, d), "gas_price": price, "bronze_collected_date": collected}
        for d in range(1, days + 1)
    ]


def _ev_rows(days=31, price=0.4, collected=EV_COLLECTED, status="Final"):
    return [
        {
            "date": date(2025, 5, d),
            "ev_price": price,
            "bronze_collected_date": collected,
            "ev_price_status": status,
        }
        for d in range(1, days + 1)
    ]


def _write_clean(silver, gas_rows, ev_rows, year_month="2025-05"):
    for dataset, rows, schema in (
        ("eia_gas_price", gas_rows, GAS_SCHEMA),
        ("eia_electricity_price", ev_rows, EV_SCHEMA),
    ):
        path = clean_silver_file(str(silver), dataset, year_month)
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_pylist(rows, schema=schema), path)


def test_날짜로_붙여_통합_스키마로_적재한다(tmp_path):
    _write_clean(tmp_path, _gas_rows(), _ev_rows())

    result = lambda_handler({"year_month": "2025-05", "silver_dir": str(tmp_path)})

    assert result["row_count"] == 31
    table = pq.ParquetFile(result["locations"][0]).read()
    assert table.schema.names == SCHEMA.names
    assert set(table["price_source"].to_pylist()) == {EIA}


def test_계보는_두_수집분_중_이른_쪽을_쓴다():
    rows = combine_daily_prices("2025-05", _gas_rows(), _ev_rows())

    assert {row["bronze_collected_date"] for row in rows} == {GAS_COLLECTED}
    assert {row["ev_price_status"] for row in rows} == {"Final"}


def test_전력_확정상태는_전력_CLEAN_의_값을_그대로_싣는다():
    rows = combine_daily_prices("2025-05", _gas_rows(), _ev_rows(status="Preliminary"))

    assert {row["ev_price_status"] for row in rows} == {"Preliminary"}


@pytest.mark.parametrize("side", ["gas", "electricity"])
def test_한쪽_CLEAN_에_날짜가_빠지면_어느_데이터셋인지_알려주며_실패한다(side):
    gas = _gas_rows(30) if side == "gas" else _gas_rows()
    ev = _ev_rows(30) if side == "electricity" else _ev_rows()

    with pytest.raises(ValueError, match=f"eia_{'gas' if side == 'gas' else 'electricity'}_price"):
        combine_daily_prices("2025-05", gas, ev)


def test_CLEAN_안에_날짜가_중복되면_실패한다():
    gas = _gas_rows()
    gas.append(gas[0])

    with pytest.raises(ValueError, match="날짜가 중복"):
        combine_daily_prices("2025-05", gas, _ev_rows())


@pytest.mark.parametrize(
    ("gas_price", "ev_price"), [(99.0, 0.4), (3.4, 9.9)]
)
def test_단가가_허용범위_밖이면_실패한다(gas_price, ev_price):
    with pytest.raises(ValueError, match="허용 범위"):
        combine_daily_prices("2025-05", _gas_rows(price=gas_price), _ev_rows(price=ev_price))


@pytest.mark.parametrize(
    ("missing", "expected_dag"),
    [
        ("eia_gas_price", "eia_gas_price_bronze_to_silver_pipeline"),
        ("eia_electricity_price", "eia_electricity_price_bronze_to_silver_pipeline"),
    ],
)
def test_입력_CLEAN_이_없으면_돌려야_할_DAG_를_알려주며_실패한다(tmp_path, missing, expected_dag):
    _write_clean(tmp_path, _gas_rows(), _ev_rows())
    clean_silver_file(str(tmp_path), missing, "2025-05").unlink()

    with pytest.raises(FileNotFoundError, match=expected_dag):
        EiaFuelPriceCleanExtractor(str(tmp_path), "2025-05").extract()


def test_산출물_경로는_데이터의_달을_쓴다(tmp_path):
    _write_clean(tmp_path, _gas_rows(), _ev_rows())

    result = lambda_handler({"year_month": "2025-05", "silver_dir": str(tmp_path)})

    assert "gas_ev_price/year_month=2025-05/gas_ev_price.parquet" in result["locations"][0]
