"""EIA 주간 휘발유 이력에서 대상 월을 뽑아 일별 단가로 펼칩니다.

원본은 **주간 관측치** 인데 출력은 **일별** 입니다(`schema/silver/gas_price.py`).
하류가 운행 날짜로 조인하므로 그 달 전 일수가 빠짐없이 있어야 하고, 하루라도 비면
그 날 운행이 통째로 매칭에 실패합니다 — 에러가 아니라 조용히 줄어든 집계로 나타납니다.
"""

import calendar
import logging
from bisect import bisect_left
from datetime import date, datetime

import xlrd

logger = logging.getLogger(__name__)

GAS_SHEET_INDEX = 1
GAS_HEADER_ROWS = 3
GAS_USD_RANGE = (1.0, 15.0)
# 대상 월 마지막 관측일이 월말에서 이만큼 넘게 떨어져 있으면 원본이 낡은 것으로 봅니다.
# 주간 계열이라 정상이면 최대 6일(마지막 관측이 25일, 월말이 31일)입니다. 공개 지연과
# 공휴일을 감안해 두 배 남짓 잡았습니다.
MAX_OBSERVATION_GAP_DAYS = 13


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


def require_fresh_observations(year_month: str, weekly: list[tuple[date, float]]) -> None:
    """대상 월을 덮을 만큼 최근 관측이 있는지 봅니다.

    `gas_price_for` 가 이력 끝을 가장 가까운 값으로 채우므로, 관측이 몇 주 전에
    끊겨도 한 달이 **같은 값 하나로** 채워지고 예외 없이 통과합니다.
    수집이 실패해 직전 수집분이 그대로 쓰이는 경우가 여기에 해당합니다(#544).

    수집일(`bronze_collected_date`)로 재지 않는 이유는, `is_duplicate_of_newest` 가
    내용이 같으면 파티션을 안 쌓아서 그 값이 "언제 받았나"가 아니라 "언제 바뀌었나"를
    뜻하기 때문입니다. 실제로 낡았는지는 관측일이 말해 줍니다.
    """
    month_end = month_days(year_month)[-1]
    in_scope = [observed for observed, _ in weekly if observed <= month_end]
    if not in_scope:
        return  # 관측 자체가 없는 경우는 `gas_price_for` 가 시작일을 알려주며 실패합니다.

    gap = (month_end - max(in_scope)).days
    if gap > MAX_OBSERVATION_GAP_DAYS:
        raise ValueError(
            f"{year_month} 를 덮는 휘발유 관측이 낡았습니다: 마지막 관측 {max(in_scope)}, "
            f"월말까지 {gap}일 (허용 {MAX_OBSERVATION_GAP_DAYS}일). "
            "eia_gas_price_raw_to_bronze_pipeline 을 먼저 돌리세요."
        )


def gas_price_for(days: list[date], weekly: list[tuple[date, float]]) -> dict[date, float]:
    """관측값 사이는 선형 보간하고 이력 양 끝은 가장 가까운 값으로 채웁니다."""
    if not weekly:
        raise ValueError("휘발유 관측치가 없습니다")

    weekly = sorted(weekly)
    observed_days = [observed for observed, _ in weekly]
    prices: dict[date, float] = {}
    for day in days:
        right = bisect_left(observed_days, day)
        if right == 0:
            prices[day] = weekly[0][1]
        elif right == len(weekly):
            prices[day] = weekly[-1][1]
        elif weekly[right][0] == day:
            prices[day] = weekly[right][1]
        else:
            left_day, left_price = weekly[right - 1]
            right_day, right_price = weekly[right]
            elapsed = (day - left_day).days
            span = (right_day - left_day).days
            prices[day] = left_price + (right_price - left_price) * elapsed / span
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
    weekly = parse_gas_weekly(gas_body)
    require_fresh_observations(year_month, weekly)
    prices = gas_price_for(days, weekly)
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
