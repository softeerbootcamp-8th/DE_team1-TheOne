"""EIA 연료비 원본 -> Bronze -> 통합 Silver 파이프라인 시나리오.

 1. 주간 휘발유 이력 파싱 — 관측일·가격을 오름차순으로
 2. 월간 전력요금 파싱 — 뉴욕 교통 부문만
 3. 일별 전개 — 그 날 이하 가장 최근 주간 관측치를 복제
 4. 전기 단가 = EIA 전력요금(¢) / 100 * 공공 충전 마진 배수
 5. 대상 월 전 일수를 채움 (하루라도 비면 하류 조인이 통째로 실패)
 6. 원본 시작일보다 이른 달 -> ValueError
 7. 전력 이력에 없는 달 -> 보유 기간을 알려주며 ValueError
 8. 가격이 허용 범위 밖 -> ValueError
 9. Bronze 는 수집일 파티션에 원본을 그대로 보관
10. 변환은 **가장 최신** 수집분을 씀 — 전력 3개월 지연 때문에 대상 월 이하로 고르면
    구조적으로 그 달이 없는 파일을 집게 됨
11. 산출물에 계보(어느 수집분·전력 확정상태)가 실림
12. 같은 내용을 다시 받으면 새 수집일 파티션을 만들지 않음

원본 xls/xlsx 는 네트워크 없이 만들 수 있도록 테스트에서 직접 씁니다.
"""

from datetime import date
from io import BytesIO

import openpyxl
import pytest
import xlrd  # noqa: F401  (구형 xls 파서 존재 확인 — 파싱 대상이 BIFF 라 필수)
import xlwt
from openpyxl import Workbook

from functions.common import eia_fuel_price_layout as layout
from functions.eia_fuel_price_bronze_to_silver.handler import lambda_handler as to_silver
from functions.eia_fuel_price_bronze_to_silver.transformer import (
    PUBLIC_CHARGING_MARKUP,
    build_daily_prices,
    parse_electricity_monthly,
    parse_gas_weekly,
)


def _gas_xls(observations: list[tuple[date, float]]) -> bytes:
    """EIA 휘발유 파일 모양 — 0번 목차 시트, 1번 주간 계열, 헤더 3행."""
    book = xlwt.Workbook()
    book.add_sheet("Contents")
    sheet = book.add_sheet("Data 1")
    for row in range(3):
        sheet.write(row, 0, "header")
    style = xlwt.XFStyle()
    style.num_format_str = "M/D/YYYY"
    for index, (observed, price) in enumerate(observations, start=3):
        sheet.write(index, 0, observed, style)
        sheet.write(index, 1, price)
    buffer = BytesIO()
    book.save(buffer)
    return buffer.getvalue()


def _electricity_xlsx(rows: list[tuple[int, int, str, float]]) -> bytes:
    """EIA-861M 모양 — 2단 헤더(부문/항목) 뒤에 Year/Month/State 행."""
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Monthly-States"
    sheet.append([None, None, None, None, "TRANSPORTATION", None])
    sheet.append([None, None, None, None, "Price", "Sales"])
    sheet.append(["Year", "Month", "State", "Data Status", "Cents/kWh", "MWh"])
    for year, month, state, price in rows:
        sheet.append([year, month, state, "Final", price, 1.0])
    buffer = BytesIO()
    workbook.save(buffer)
    return buffer.getvalue()


GAS = [(date(2025, 4, 28), 3.0), (date(2025, 5, 5), 3.1), (date(2025, 5, 26), 3.2)]
COLLECTED = date(2026, 8, 17)
ELECTRICITY = [(2025, 5, "NY", 20.0), (2025, 5, "CA", 99.0), (2025, 6, "NY", 21.0)]


def test_주간_휘발유_이력을_오름차순으로_읽는다():
    parsed = parse_gas_weekly(_gas_xls(list(reversed(GAS))))

    assert parsed == GAS


def test_월간_전력요금은_뉴욕_교통부문만_읽는다():
    parsed = parse_electricity_monthly(_electricity_xlsx(ELECTRICITY))

    # CA(99.0)는 걸러지고 NY 두 달만 남습니다. 확정 상태도 함께 옵니다 — 같은 달을
    # 다시 만들 때 숫자가 달라지는 유일한 원인이라 결과에 남겨야 합니다.
    assert parsed == {"2025-05": (20.0, "Final"), "2025-06": (21.0, "Final")}


def test_일별_전개는_그날_이하_최근_관측치를_복제한다():
    rows = build_daily_prices("2025-05", _gas_xls(GAS), _electricity_xlsx(ELECTRICITY), COLLECTED)
    prices = {row["date"]: row["gas_price"] for row in rows}

    # 5/1~5/4 는 4/28 관측치, 5/5~5/25 는 5/5, 5/26 부터 5/26 관측치.
    assert prices[date(2025, 5, 1)] == pytest.approx(3.0)
    assert prices[date(2025, 5, 4)] == pytest.approx(3.0)
    assert prices[date(2025, 5, 5)] == pytest.approx(3.1)
    assert prices[date(2025, 5, 25)] == pytest.approx(3.1)
    assert prices[date(2025, 5, 31)] == pytest.approx(3.2)


@pytest.mark.parametrize("markup", [1.0, PUBLIC_CHARGING_MARKUP, 2.5])
def test_전기_단가는_전력요금에_마진_배수를_곱한다(markup):
    rows = build_daily_prices(
        "2025-05", _gas_xls(GAS), _electricity_xlsx(ELECTRICITY), COLLECTED, markup=markup
    )

    # EIA 는 ¢/kWh 로 오므로 100 으로 나눈 뒤 배수를 곱합니다.
    assert rows[0]["ev_price"] == pytest.approx(20.0 / 100 * markup)
    # 월간 값이라 그 달 안에서는 모든 날이 같은 단가여야 합니다.
    assert len({row["ev_price"] for row in rows}) == 1


def test_대상월_전_일수를_채운다():
    rows = build_daily_prices("2025-05", _gas_xls(GAS), _electricity_xlsx(ELECTRICITY), COLLECTED)

    # 하루라도 비면 Gold 의 일자 조인에서 그 날 운행이 통째로 매칭 실패합니다.
    assert [row["date"] for row in rows] == [date(2025, 5, day) for day in range(1, 32)]
    assert {row["price_source"] for row in rows} == {"eia"}


def test_원본_시작일보다_이른_달은_실패한다():
    with pytest.raises(ValueError, match="이전의 휘발유 관측치가 없습니다"):
        build_daily_prices("2025-03", _gas_xls(GAS), _electricity_xlsx(ELECTRICITY), COLLECTED)


def test_전력_이력에_없는_달은_보유기간을_알려주며_실패한다():
    with pytest.raises(ValueError, match="보유 2025-05 ~ 2025-06"):
        build_daily_prices("2025-07", _gas_xls(GAS), _electricity_xlsx(ELECTRICITY), COLLECTED)


@pytest.mark.parametrize(
    ("gas_price", "electricity_cents", "expected"),
    [(999.0, 20.0, "휘발유 가격이 허용 범위"), (3.0, 9999.0, "전기 단가가 허용 범위")],
)
def test_가격이_허용범위_밖이면_실패한다(gas_price, electricity_cents, expected):
    gas = _gas_xls([(date(2025, 4, 28), gas_price)])
    electricity = _electricity_xlsx([(2025, 5, "NY", electricity_cents)])

    with pytest.raises(ValueError, match=expected):
        build_daily_prices("2025-05", gas, electricity, COLLECTED)


def _write_bronze(base_dir, collected_date: date, gas: bytes, electricity: bytes) -> None:
    for path, body in [
        (layout.gas_bronze_file(str(base_dir), collected_date), gas),
        (layout.electricity_bronze_file(str(base_dir), collected_date), electricity),
    ]:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)


def test_bronze에서_silver까지_통합_스키마로_적재한다(tmp_path):
    import pyarrow.parquet as pq

    bronze, silver = tmp_path / "bronze", tmp_path / "silver"
    _write_bronze(bronze, date(2026, 8, 17), _gas_xls(GAS), _electricity_xlsx(ELECTRICITY))

    result = to_silver(
        {"year_month": "2025-05", "bronze_dir": str(bronze), "silver_dir": str(silver)}
    )

    assert result["row_count"] == 31
    assert result["year_month"] == "2025-05"
    # `pq.read_table` 은 경로의 `year_month=` 를 파티션 컬럼으로 덧붙이므로
    # 파일에 실제로 쓰인 스키마를 직접 봅니다.
    assert pq.read_schema(result["locations"][0]).names == [
        "date", "gas_price", "ev_price", "price_source",
        "bronze_collected_date", "ev_price_status",
    ]
    assert set(pq.read_table(result["locations"][0])["price_source"].to_pylist()) == {"eia"}


def test_산출물_경로는_데이터의_달을_쓴다(tmp_path):
    # 전에는 `collected_month=` 였는데 값은 데이터의 달인데 이름은 "수집" 이라 구조를
    # 오해하게 만들었습니다. 2025-05 데이터를 2026-08 수집분으로 만들어도 경로는 2025-05.
    bronze, silver = tmp_path / "bronze", tmp_path / "silver"
    _write_bronze(bronze, COLLECTED, _gas_xls(GAS), _electricity_xlsx(ELECTRICITY))

    result = to_silver(
        {"year_month": "2025-05", "bronze_dir": str(bronze), "silver_dir": str(silver)}
    )

    assert "year_month=2025-05" in result["locations"][0]
    assert "collected_month" not in result["locations"][0]


def test_계보에_사용한_수집분과_전력_확정상태가_실린다(tmp_path):
    """같은 달을 다시 만들면 숫자가 달라질 수 있어서, 무엇으로 만들었는지 남깁니다."""
    import pyarrow.parquet as pq

    bronze, silver = tmp_path / "bronze", tmp_path / "silver"
    _write_bronze(bronze, COLLECTED, _gas_xls(GAS), _electricity_xlsx(ELECTRICITY))

    result = to_silver(
        {"year_month": "2025-05", "bronze_dir": str(bronze), "silver_dir": str(silver)}
    )
    table = pq.ParquetFile(result["locations"][0]).read()

    assert set(table["bronze_collected_date"].to_pylist()) == {COLLECTED}
    assert set(table["ev_price_status"].to_pylist()) == {"Final"}


def test_변환은_가장_최신_수집분을_쓴다(tmp_path):
    """대상 월 이하로 고르면 안 되는 이유를 고정합니다.

    전력 통계는 약 3개월 늦게 공개됩니다. 2025-05 값은 2025-05 에 받은 파일에는 아직
    없고 2026-08 에 받은 파일에 있습니다. "대상 월 이하" 로 고르면 구조적으로 없는
    파일을 집습니다.
    """
    bronze = tmp_path / "bronze"
    old = _electricity_xlsx([(2025, 1, "NY", 20.0)])  # 2025-05 가 아직 없는 파일
    _write_bronze(bronze, date(2025, 5, 10), _gas_xls(GAS), old)
    _write_bronze(bronze, COLLECTED, _gas_xls(GAS), _electricity_xlsx(ELECTRICITY))

    collected_date, partition = layout.newest_bronze_partition(
        str(bronze), layout.ELECTRICITY_DATASET
    )

    assert collected_date == COLLECTED
    assert partition.name == f"collected_date={COLLECTED.isoformat()}"


def test_같은_내용을_다시_받으면_새_파티션을_만들지_않는다(tmp_path):
    """전력은 3개월에 한 번만 갱신되므로 월 1회 수집분 대부분이 바이트까지 같습니다.

    같은 것을 쌓지 않으면 파티션 개수가 "언제 실제로 바뀌었는지" 를 말해줍니다.
    """
    from functions.eia_electricity_price_raw_to_bronze.loader import (
        EiaElectricityPriceBronzeLoader,
    )

    bronze = tmp_path / "bronze"
    body = _electricity_xlsx(ELECTRICITY)
    first = EiaElectricityPriceBronzeLoader(str(bronze), date(2026, 8, 1)).write({"body": body})
    same = EiaElectricityPriceBronzeLoader(str(bronze), date(2026, 9, 1)).write({"body": body})

    assert same.location == first.location
    assert len(layout.bronze_partitions(str(bronze), layout.ELECTRICITY_DATASET)) == 1

    # 내용이 바뀌면 새 파티션이 생깁니다.
    changed = EiaElectricityPriceBronzeLoader(str(bronze), date(2026, 10, 1)).write(
        {"body": _electricity_xlsx([*ELECTRICITY, (2025, 7, "NY", 22.0)])}
    )
    assert changed.location != first.location
    assert len(layout.bronze_partitions(str(bronze), layout.ELECTRICITY_DATASET)) == 2
