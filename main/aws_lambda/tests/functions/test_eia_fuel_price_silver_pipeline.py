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
7. 지역 경로와 옛 경로가 모두 있으면 지역 경로를 읽는다 (#843/#851 — 탐색 순서가
   뒤집히면 옛 경로의 낡은 값을 조용히 집음)
8. 지역 경로가 없으면 옛 경로로 폴백한다 (아직 안 옮긴 데이터셋과의 하위호환)
9. service_area를 TX로 주면 읽기·쓰기 모두 그 경로로 나간다 — 두 CLEAN을 읽는 지역과
   결합 결과를 쓰는 지역이 같음을 함께 증명 (#845)
"""

from datetime import date

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from main.aws_lambda.functions.eia_fuel_price_silver.extractor import (
    EiaFuelPriceCleanExtractor,
    _read,
    clean_silver_file,
    clean_silver_key,
)
from main.aws_lambda.functions.eia_fuel_price_silver.handler import lambda_handler
from main.aws_lambda.functions.eia_fuel_price_silver.loader import silver_key
from main.aws_lambda.functions.eia_fuel_price_silver.transformer import combine_daily_prices
from schema.silver import (
    CLEAN_EV_CHARGING_PRICE_SCHEMA as EV_SCHEMA,
    CLEAN_FUEL_PRICE_SCHEMA as SCHEMA,
    CLEAN_GAS_PRICE_SCHEMA as GAS_SCHEMA,
    EIA,
)

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


def _write_clean(silver, gas_rows, ev_rows, year_month="2025-05", service_area=None):
    for dataset, rows, schema in (
        ("eia_gas_price", gas_rows, GAS_SCHEMA),
        ("eia_electricity_price", ev_rows, EV_SCHEMA),
    ):
        path = clean_silver_file(str(silver), dataset, year_month, service_area)
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

    assert (
        "gas_ev_price/service_area=NYC/year_month=2025-05/gas_ev_price.parquet"
        in result["locations"][0]
    )


# --- S3 배포 (#577) — 키 포맷 --------------------------------------------
# 이 단계는 gas(#557)·electricity(#558) 처럼 최신 파티션을 찾지 않습니다. 대상 월이
# 정해지면 CLEAN 위치가 고정되므로, 키 포맷만 로컬 파일 경로 규칙과 대응되는지 봅니다.


def test_CLEAN_S3_키가_로컬_경로_규칙과_대응된다():
    assert clean_silver_key("eia_gas_price", "2025-05") == (
        "silver/eia_gas_price/year_month=2025-05/eia_gas_price.parquet"
    )
    assert clean_silver_key("eia_electricity_price", "2025-05") == (
        "silver/eia_electricity_price/year_month=2025-05/eia_electricity_price.parquet"
    )


def test_산출물_S3_키도_데이터의_달을_쓴다():
    assert silver_key("2025-05") == "silver/gas_ev_price/year_month=2025-05/gas_ev_price.parquet"


# --- 지역(service_area) 이중 탐색 (#843/#851) -------------------------------


def _write_gas(silver, year_month, price, service_area=None):
    path = clean_silver_file(str(silver), "eia_gas_price", year_month, service_area)
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.Table.from_pylist(_gas_rows(price=price), schema=GAS_SCHEMA), path
    )


def test_지역_경로와_옛_경로가_모두_있으면_지역_경로를_읽는다(tmp_path):
    """탐색 순서가 뒤집히면 옛 경로의 낡은 값을 조용히 집습니다."""
    _write_gas(tmp_path, "2025-05", price=1.0, service_area=None)
    _write_gas(tmp_path, "2025-05", price=9.0, service_area="NYC")

    rows = _read(str(tmp_path), "eia_gas_price", "2025-05", "NYC")

    assert {row["gas_price"] for row in rows} == {9.0}


def test_지역_경로가_없으면_옛_경로로_폴백한다(tmp_path):
    _write_gas(tmp_path, "2025-05", price=3.4, service_area=None)

    rows = _read(str(tmp_path), "eia_gas_price", "2025-05", "NYC")

    assert {row["gas_price"] for row in rows} == {3.4}


def test_service_area를_TX로_주면_읽기_쓰기_모두_그_경로로_나간다(tmp_path):
    """이슈 완료 조건 — 읽는 두 CLEAN Silver와 쓰는 결합 결과가 같은 지역을 써야
    합니다. 어긋나면 다른 지역의 유가로 이 지역 Gold를 계산하는, 조용히 틀린 값이
    나오는 가장 위험한 사고입니다(#845)."""
    _write_clean(tmp_path, _gas_rows(), _ev_rows(), service_area="TX")

    result = lambda_handler(
        {"year_month": "2025-05", "silver_dir": str(tmp_path), "service_area": "TX"}
    )

    assert (
        "gas_ev_price/service_area=TX/year_month=2025-05/gas_ev_price.parquet"
        in result["locations"][0]
    )
