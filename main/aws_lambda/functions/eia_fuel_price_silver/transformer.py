"""휘발유·전력 CLEAN Silver 를 날짜로 붙여 통합 연료비 Silver 를 만듭니다.

원본 파싱(주간 xls·월간 xlsx)과 일별 확장은 여기 없습니다 — 각 원천의
`*_bronze_to_silver` 가 이미 했습니다(#512, #517). 이 모듈은 **붙이는 일**만 합니다.

계보를 합치는 방식
----------------
두 CLEAN 이 각자 `bronze_collected_date` 를 싣고 옵니다. 통합 결과에는 **더 이른 쪽**을
남깁니다 — 결과가 반영하는 정보의 하한이라서요. 두 원천은 서로 다른 DAG 가 받아서
수집일이 다를 수 있습니다.
"""

import calendar
import logging
from datetime import date, datetime

from schema.silver.gas_ev_price import EIA, FINAL

logger = logging.getLogger(__name__)

GAS_USD_RANGE = (1.0, 15.0)
EV_USD_RANGE = (0.05, 3.0)


def month_days(year_month: str) -> list[date]:
    year, month = (int(part) for part in year_month.split("-"))
    last = calendar.monthrange(year, month)[1]
    return [date(year, month, day) for day in range(1, last + 1)]


def _by_date(rows: list[dict], dataset: str) -> dict[date, dict]:
    indexed: dict[date, dict] = {}
    for row in rows:
        if row["date"] in indexed:
            raise ValueError(f"{dataset} CLEAN Silver 에 날짜가 중복됩니다: {row['date']}")
        indexed[row["date"]] = row
    return indexed


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
        if not EV_USD_RANGE[0] < row["ev_price"] < EV_USD_RANGE[1]:
            raise ValueError(f"충전 단가가 허용 범위 밖입니다: {row['ev_price']}")


def combine_daily_prices(
    year_month: str,
    gas_rows: list[dict],
    electricity_rows: list[dict],
) -> list[dict]:
    """두 CLEAN 을 날짜로 붙인 일별 연료비."""
    datetime.strptime(year_month, "%Y-%m")

    gas = _by_date(gas_rows, "eia_gas_price")
    electricity = _by_date(electricity_rows, "eia_electricity_price")

    days = month_days(year_month)
    for dataset, indexed in (("eia_gas_price", gas), ("eia_electricity_price", electricity)):
        missing = [day for day in days if day not in indexed]
        if missing:
            raise ValueError(
                f"{dataset} CLEAN Silver 에 {year_month} 일자가 빠졌습니다: "
                f"{missing[0]} 외 {len(missing) - 1}건"
            )

    rows = []
    for day in days:
        gas_row, ev_row = gas[day], electricity[day]
        rows.append(
            {
                "date": day,
                "gas_price": gas_row["gas_price"],
                "ev_price": ev_row["ev_price"],
                "price_source": EIA,
                # 두 수집분 중 이른 쪽 — 결과가 반영하는 정보의 하한입니다.
                "bronze_collected_date": min(
                    gas_row["bronze_collected_date"], ev_row["bronze_collected_date"]
                ),
                "ev_price_status": ev_row["ev_price_status"],
            }
        )
    validate(rows, year_month)

    statuses = {row["ev_price_status"] for row in rows}
    collected = {row["bronze_collected_date"] for row in rows}
    logger.info(
        "EIA 연료비 통합: %s %d일 gas=%.3f~%.3f ev=%.4f 수집분=%s 전력상태=%s",
        year_month, len(rows),
        min(row["gas_price"] for row in rows), max(row["gas_price"] for row in rows),
        rows[0]["ev_price"], sorted(collected), sorted(statuses),
    )
    if statuses != {FINAL}:
        # 잠정값이면 나중에 다시 만들 때 숫자가 바뀝니다. 조용히 넘기지 않습니다.
        logger.warning(
            "%s 전력값이 확정(%s) 이 아닙니다 (%s). 나중에 다시 만들면 값이 바뀝니다.",
            year_month, FINAL, sorted(statuses),
        )
    return rows
