"""EIA 주간 휘발유 이력에서 대상 월을 뽑아 일별 단가로 펼칩니다.

원본은 **주간 관측치** 인데 출력은 **일별** 입니다(`schema/silver/gas_price.py`).
하류가 운행 날짜로 조인하므로 그 달 전 일수가 빠짐없이 있어야 하고, 하루라도 비면
그 날 운행이 통째로 매칭에 실패합니다 — 에러가 아니라 조용히 줄어든 집계로 나타납니다.
"""

import calendar
import logging
from datetime import date, datetime

import xlrd

logger = logging.getLogger(__name__)

GAS_SHEET_INDEX = 1
GAS_HEADER_ROWS = 3
GAS_USD_RANGE = (1.0, 15.0)


def month_days(year_month: str) -> list[date]:
    year, month = (int(part) for part in year_month.split("-"))
    last = calendar.monthrange(year, month)[1]
    return [date(year, month, day) for day in range(1, last + 1)]


def parse_gas_weekly(body: bytes) -> list[tuple[date, float]]:
    """주간 휘발유 이력 → (관측일, USD/gal) 오름차순."""
    book = xlrd.open_workbook(file_contents=body)
    if book.nsheets <= GAS_SHEET_INDEX:
        raise ValueError("EIA 휘발유 파일에 주간 계열 시트가 없습니다")
    sheet = book.sheet_by_index(GAS_SHEET_INDEX)

    observations: list[tuple[date, float]] = []
    for index in range(GAS_HEADER_ROWS, sheet.nrows):
        raw_date, raw_price = sheet.cell_value(index, 0), sheet.cell_value(index, 1)
        if not isinstance(raw_date, float) or not isinstance(raw_price, float):
            continue
        parts = xlrd.xldate_as_tuple(raw_date, book.datemode)
        observations.append((date(*parts[:3]), float(raw_price)))

    if not observations:
        raise ValueError("EIA 휘발유 이력이 비어 있습니다")
    return sorted(observations)


def gas_price_for(days: list[date], weekly: list[tuple[date, float]]) -> dict[date, float]:
    """각 날짜에 **그 날 이하 가장 최근 주간 관측치**를 복제합니다.

    선형 보간하지 않는 이유는, EIA 주간값이 "그 주의 관측 평균"이라 다음 관측까지
    유효한 값으로 보는 편이 원 데이터에 가깝기 때문입니다.
    """
    prices: dict[date, float] = {}
    for day in days:
        earlier = [price for observed, price in weekly if observed <= day]
        if not earlier:
            raise ValueError(
                f"{day} 이전의 휘발유 관측치가 없습니다 (원본 시작일 {weekly[0][0]} 이후여야 함)"
            )
        prices[day] = earlier[-1]
    return prices


def validate(rows: list[dict], year_month: str) -> None:
    """그 달 전 일수가 빠짐없이 있고 단가가 허용 범위인지 봅니다."""
    expected = month_days(year_month)
    if [row["date"] for row in rows] != expected:
        raise ValueError(
            f"{year_month} 일자가 빠짐없이 있어야 합니다: "
            f"{len(rows)}행 (기대 {len(expected)}행)"
        )
    for row in rows:
        if not GAS_USD_RANGE[0] < row["gas_price"] < GAS_USD_RANGE[1]:
            raise ValueError(f"휘발유 가격이 허용 범위 밖입니다: {row['gas_price']}")


def build_daily_prices(
    year_month: str,
    gas_body: bytes,
    bronze_collected_date: date,
) -> list[dict]:
    """대상 월의 일별 휘발유 단가."""
    datetime.strptime(year_month, "%Y-%m")

    days = month_days(year_month)
    prices = gas_price_for(days, parse_gas_weekly(gas_body))
    rows = [
        {
            "date": day,
            "gas_price": prices[day],
            "bronze_collected_date": bronze_collected_date,
        }
        for day in days
    ]
    validate(rows, year_month)

    logger.info(
        "EIA 일별 휘발유 단가 생성: %s %d일 gas=%.3f~%.3f 수집분=%s",
        year_month, len(rows),
        min(row["gas_price"] for row in rows),
        max(row["gas_price"] for row in rows),
        bronze_collected_date,
    )
    return rows
