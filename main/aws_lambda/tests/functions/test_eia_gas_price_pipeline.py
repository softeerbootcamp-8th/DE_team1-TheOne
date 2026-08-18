"""EIA 휘발유 요금 Bronze→Silver 변환 시나리오. 이슈 #517.

1. 주간 이력을 오름차순으로 읽음 — 원본이 최신순이라 정렬을 빠뜨리면 아래 "그 날 이하
   가장 최근" 규칙이 엉뚱한 값을 집음
2. 각 날짜에 그 날 이하 **가장 최근** 관측치를 복제. 선형 보간하지 않음
3. 그 달 **전 일수**로 펼침. 하루라도 비면 하류 일자 조인에서 그 날이 통째로 빠지는데,
   실패가 아니라 조용히 줄어든 집계로 나타남
4. 대상 월 첫날 이전 관측치가 없으면 실패
5. 단가가 허용 범위 밖이면 실패
6. Loader 가 CLEAN 스키마 그대로, 대상 월 파티션에 씀
"""

from datetime import date
from io import BytesIO

import pyarrow.parquet as pq
import pytest
import xlrd  # noqa: F401  (구형 xls 파서 존재 확인 — 파싱 대상이 BIFF 라 필수)
import xlwt

from main.aws_lambda.functions.eia_gas_price_bronze_to_silver.loader import (
    EiaGasPriceSilverLoader,
    silver_file,
)
from main.aws_lambda.functions.eia_gas_price_bronze_to_silver.transformer import (
    build_daily_prices,
    gas_price_for,
    parse_gas_weekly,
)
from schema.silver.gas_price import SCHEMA

COLLECTED = date(2026, 8, 17)
WEEKLY = [
    (date(2025, 4, 28), 3.0),
    (date(2025, 5, 5), 3.1),
    (date(2025, 5, 26), 3.2),
]


def _xls(observations: list[tuple[date, float]]) -> bytes:
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


def test_주간_이력을_오름차순으로_읽는다():
    assert parse_gas_weekly(_xls(list(reversed(WEEKLY)))) == WEEKLY


def test_각_날짜에_그날_이하_가장_최근_관측치를_복제한다():
    days = [date(2025, 5, 4), date(2025, 5, 5), date(2025, 5, 25), date(2025, 5, 26)]

    prices = gas_price_for(days, WEEKLY)

    # 5-04 는 아직 4-28 관측치, 5-05 부터 3.1, 5-26 부터 3.2
    assert [prices[day] for day in days] == [3.0, 3.1, 3.1, 3.2]


def test_월_전체를_일별로_펼친다():
    rows = build_daily_prices("2025-05", _xls(WEEKLY), COLLECTED)

    assert len(rows) == 31
    assert [row["date"] for row in rows] == [date(2025, 5, d) for d in range(1, 32)]
    # 5-01~04 는 4-28 관측치가 이어집니다.
    assert rows[0]["gas_price"] == 3.0
    assert rows[-1]["gas_price"] == 3.2


def test_어느_수집분으로_만들었는지_모든_행에_남는다():
    """같은 달을 다시 만들면 숫자가 달라질 수 있습니다 — EIA 가 과거 값을 개정하기
    때문입니다. 수집분을 남겨두지 않으면 그 차이를 설명할 수 없습니다 (#518)."""
    rows = build_daily_prices("2025-05", _xls(WEEKLY), COLLECTED)

    assert {row["bronze_collected_date"] for row in rows} == {COLLECTED}


def test_대상월_이전_관측치가_없으면_실패한다():
    with pytest.raises(ValueError, match="이전의 휘발유 관측치가 없습니다"):
        build_daily_prices("2025-03", _xls(WEEKLY), COLLECTED)


def test_단가가_허용범위_밖이면_실패한다():
    with pytest.raises(ValueError, match="허용 범위"):
        build_daily_prices("2025-05", _xls([(date(2025, 4, 28), 99.0)]), COLLECTED)


def test_주간_계열_시트가_없으면_실패한다():
    book = xlwt.Workbook()
    book.add_sheet("Contents")
    buffer = BytesIO()
    book.save(buffer)

    with pytest.raises(ValueError, match="주간 계열 시트"):
        parse_gas_weekly(buffer.getvalue())


def test_적재는_CLEAN_스키마로_대상월_파티션에_쓴다(tmp_path):
    rows = build_daily_prices("2025-05", _xls(WEEKLY), COLLECTED)

    result = EiaGasPriceSilverLoader(str(tmp_path), "2025-05").write(rows)

    assert result.location == str(silver_file(str(tmp_path), "2025-05"))
    assert result.row_count == 31
    table = pq.ParquetFile(result.location).read()
    assert table.schema.names == SCHEMA.names


def test_빈_결과는_적재를_거부한다(tmp_path):
    with pytest.raises(ValueError, match="적재할"):
        EiaGasPriceSilverLoader(str(tmp_path), "2025-05").write([])
