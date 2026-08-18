"""EIA 전력요금 Bronze→Silver 변환 시나리오. 이슈 #512.

1. 월간 이력에서 대상 주(NY)·부문(TRANSPORTATION)만 골라 읽음 — 다른 주가 섞이면
   엉뚱한 단가가 그대로 통과함
2. 월값을 그 달 **전 일수**로 펼침. 하루라도 비면 하류 일자 조인에서 그 날이 통째로
   빠지는데, 실패가 아니라 조용히 줄어든 집계로 나타남
3. 공공 충전 배수를 곱해 ¢/kWh → $/kWh 로 변환
4. 대상 월이 이력에 없으면 보유 구간을 알려주며 실패 — 전력 통계는 약 3개월 지연
5. 단가가 허용 범위 밖이면 실패
6. Loader 가 CLEAN 스키마 그대로, 대상 월 파티션에 씀
"""

from datetime import date
from io import BytesIO

import pyarrow.parquet as pq
import pytest
from openpyxl import Workbook

from main.aws_lambda.functions.eia_electricity_price_bronze_to_silver.loader import (
    EiaElectricityPriceSilverLoader,
    silver_file,
)
from main.aws_lambda.functions.eia_electricity_price_bronze_to_silver.transformer import (
    CENTS_PER_DOLLAR,
    PUBLIC_CHARGING_MARKUP,
    build_daily_prices,
    parse_electricity_monthly,
)
from schema.silver.ev_charging_price import SCHEMA

COLLECTED = date(2026, 8, 17)
ROWS = [(2025, 5, "NY", 20.0), (2025, 5, "CA", 99.0), (2025, 6, "NY", 21.0)]


def _xlsx(rows: list[tuple[int, int, str, float]], status: str = "Final") -> bytes:
    """EIA-861M 모양 — 2단 헤더(부문/항목) 뒤에 Year/Month/State 행."""
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Monthly-States"
    sheet.append([None, None, None, None, "TRANSPORTATION", None])
    sheet.append([None, None, None, None, "Price", "Sales"])
    sheet.append(["Year", "Month", "State", "Data Status", "Cents/kWh", "MWh"])
    for year, month, state, price in rows:
        sheet.append([year, month, state, status, price, 1.0])
    buffer = BytesIO()
    workbook.save(buffer)
    return buffer.getvalue()


def test_뉴욕_교통부문만_골라_월별로_읽는다():
    parsed = parse_electricity_monthly(_xlsx(ROWS))

    assert parsed["2025-05"] == (20.0, "Final")
    assert parsed["2025-06"] == (21.0, "Final")
    # CA 행이 섞이면 5월 단가가 99.0 으로 덮여 있었을 것입니다.
    assert parsed["2025-05"][0] != 99.0


def test_월값을_그달_전_일수로_펼치고_배수를_곱한다():
    rows = build_daily_prices("2025-05", _xlsx(ROWS), COLLECTED)

    assert len(rows) == 31
    assert [row["date"] for row in rows] == [date(2025, 5, day) for day in range(1, 32)]
    expected = 20.0 / CENTS_PER_DOLLAR * PUBLIC_CHARGING_MARKUP
    assert {row["ev_price"] for row in rows} == {expected}


def test_배수를_바꾸면_단가가_따라_바뀐다():
    rows = build_daily_prices("2025-05", _xlsx(ROWS), COLLECTED, markup=1.0)

    assert rows[0]["ev_price"] == pytest.approx(0.20)


@pytest.mark.parametrize("status", ["Final", "Preliminary"])
def test_수집분과_확정상태가_모든_행에_남는다(status):
    """EIA 가 최근 약 17개월을 Preliminary 로 두고 나중에 Final 로 바꿉니다. 같은 달을
    다시 만들었을 때 숫자가 달라지는 원인이라 결과에 남깁니다 (#518)."""
    rows = build_daily_prices("2025-05", _xlsx(ROWS, status=status), COLLECTED)

    assert {row["bronze_collected_date"] for row in rows} == {COLLECTED}
    assert {row["ev_price_status"] for row in rows} == {status}


def test_대상_월이_이력에_없으면_보유구간을_알려주며_실패한다():
    with pytest.raises(ValueError, match="2025-07 이 없습니다"):
        build_daily_prices("2025-07", _xlsx(ROWS), COLLECTED)


def test_단가가_허용범위_밖이면_실패한다():
    # 0.5¢/kWh × 배수 2 = $0.01 — 하한(0.05) 아래입니다.
    with pytest.raises(ValueError, match="허용 범위"):
        build_daily_prices("2025-05", _xlsx([(2025, 5, "NY", 0.5)]), COLLECTED)


def test_시트가_다르면_실패한다():
    workbook = Workbook()
    workbook.active.title = "Other"
    buffer = BytesIO()
    workbook.save(buffer)

    with pytest.raises(ValueError, match="Monthly-States"):
        parse_electricity_monthly(buffer.getvalue())


def test_적재는_CLEAN_스키마로_대상월_파티션에_쓴다(tmp_path):
    rows = build_daily_prices("2025-05", _xlsx(ROWS), COLLECTED)

    result = EiaElectricityPriceSilverLoader(str(tmp_path), "2025-05").write(rows)

    assert result.location == str(silver_file(str(tmp_path), "2025-05"))
    assert result.row_count == 31
    table = pq.ParquetFile(result.location).read()
    assert table.schema.names == SCHEMA.names
    assert table.num_rows == 31


def test_빈_결과는_적재를_거부한다(tmp_path):
    with pytest.raises(ValueError, match="적재할"):
        EiaElectricityPriceSilverLoader(str(tmp_path), "2025-05").write([])


# --- Bronze 파티션 선택 규칙 ------------------------------------------------
#
# 통합 단계가 Bronze 를 직접 읽던 시절 그쪽 테스트에 있던 것들입니다 (#518 로 통합이
# CLEAN Silver 만 읽게 되면서 여기로 옮겼습니다). 이 규칙을 실제로 쓰는 건 이제
# 전력 CLEAN 파이프라인의 extractor 입니다.

from main.aws_lambda.common import eia_fuel_price_layout as layout  # noqa: E402


def _write_bronze(base, collected: date, body: bytes) -> None:
    path = layout.electricity_bronze_file(str(base), collected)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(body)


def test_가장_최신_수집분을_고른다(tmp_path):
    """대상 월 이하로 고르면 안 되는 이유를 고정합니다.

    전력 통계는 약 3개월 늦게 공개됩니다. 2025-05 값은 2025-05 에 받은 파일에는 아직
    없고 2026-08 에 받은 파일에 있습니다. "대상 월 이하" 로 고르면 구조적으로 없는
    파일을 집습니다.
    """
    bronze = tmp_path / "bronze"
    _write_bronze(bronze, date(2025, 5, 10), _xlsx([(2025, 1, "NY", 20.0)]))
    _write_bronze(bronze, COLLECTED, _xlsx(ROWS))

    collected_date, partition = layout.newest_bronze_partition(
        str(bronze), layout.ELECTRICITY_DATASET
    )

    assert collected_date == COLLECTED
    assert partition.name == f"collected_date={COLLECTED.isoformat()}"


def test_같은_내용을_다시_받으면_새_파티션을_만들지_않는다(tmp_path):
    """전력은 3개월에 한 번만 갱신되므로 월 1회 수집분 대부분이 바이트까지 같습니다.

    같은 것을 쌓지 않으면 파티션 개수가 "언제 실제로 바뀌었는지" 를 말해줍니다.
    """
    from main.aws_lambda.functions.eia_electricity_price_raw_to_bronze.loader import (
        EiaElectricityPriceBronzeLoader,
    )

    bronze = tmp_path / "bronze"
    body = _xlsx(ROWS)
    first = EiaElectricityPriceBronzeLoader(str(bronze), date(2026, 8, 1)).write({"body": body})
    same = EiaElectricityPriceBronzeLoader(str(bronze), date(2026, 9, 1)).write({"body": body})

    assert same.location == first.location
    assert len(layout.bronze_partitions(str(bronze), layout.ELECTRICITY_DATASET)) == 1
