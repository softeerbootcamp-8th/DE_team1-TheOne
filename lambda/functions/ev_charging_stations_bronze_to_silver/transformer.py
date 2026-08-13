"""뉴욕시 충전소의 kWh 단위 요금을 일별 평균으로 정제합니다.

Free와 시간당·세션당·정액 요금은 kWh 단위 평균에서 제외합니다.
"""

import math
import re
from collections.abc import Iterable
from datetime import datetime, timezone
from statistics import fmean

from pipeline_core.transformer import Transformer

PRICE_PER_KWH_RE = re.compile(
    r"(?<![-\d])(-?)\$\s*(\d+(?:\.\d+)?)\s*(?:/|per\s+)kwh\b",
    re.IGNORECASE,
)
# kWh 단위로 파싱된 비정상 고액이 평균을 왜곡하지 않도록 차단합니다.
MAX_USD_PER_KWH = 5.0


def _borough(zip_code: object) -> str | None:
    """NYC 우편번호를 5개 borough로 변환합니다."""
    zip5 = str(zip_code or "").strip()[:5]
    if len(zip5) != 5 or not zip5.isdigit():
        return None

    number = int(zip5)
    if 10001 <= number <= 10282:
        return "Manhattan"
    if 10301 <= number <= 10314:
        return "Staten Island"
    if 10451 <= number <= 10475:
        return "Bronx"
    if 11201 <= number <= 11256:
        return "Brooklyn"
    if (
        number in {11004, 11005}
        or 11101 <= number <= 11109
        or 11351 <= number <= 11385
        or 11411 <= number <= 11436
        or 11691 <= number <= 11697
    ):
        return "Queens"
    return None


def _as_utc(value: object) -> datetime:
    parsed = (
        value
        if isinstance(value, datetime)
        else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    )
    if parsed.tzinfo is None:
        raise ValueError("collected_at에 시간대가 없습니다")
    return parsed.astimezone(timezone.utc)


def _price_per_kwh(value: object) -> tuple[float | None, str]:
    """USD/kWh만 서로 비교 가능한 가격으로 변환합니다."""
    text = str(value or "").strip()
    if not text:
        return None, "missing"
    if text.casefold() == "free":
        return None, "free"

    matches = PRICE_PER_KWH_RE.findall(text)
    if not matches:
        return None, "unsupported"

    prices = [float(f"{sign}{number}") for sign, number in matches]
    if any(
        not math.isfinite(price) or not 0 < price <= MAX_USD_PER_KWH
        for price in prices
    ):
        raise ValueError("kWh 단위 요금이 허용 범위를 벗어났습니다")
    return fmean(prices), "normalized"


class EvChargingSilverTransformer(Transformer):
    """각 일별 스냅샷의 NYC kWh 요금을 평균 행으로 정제합니다."""

    def transform(self, data: Iterable[dict]) -> list[dict]:
        rows = [self._transform_snapshot(snapshot) for snapshot in data]
        if not rows:
            raise ValueError("변환할 EV Charging Bronze 데이터가 없습니다.")
        return sorted(rows, key=lambda row: row["date"])

    @staticmethod
    def _transform_snapshot(snapshot: dict) -> dict:
        bronze_path = str(snapshot.get("bronze_path") or "<unknown>")
        try:
            stations = snapshot["fuel_stations"]
            collected_at = _as_utc(snapshot["collected_at"])
            source_url = str(snapshot.get("source_url") or "").strip()
            if not isinstance(stations, list) or not stations:
                raise ValueError("충전소 데이터가 비어 있습니다")
            if not source_url:
                raise ValueError("source_url이 비어 있습니다")
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(f"{bronze_path}: {exc}") from exc

        errors: list[str] = []
        station_ids: set[int] = set()
        prices: list[float] = []
        nyc_station_count = 0

        for row in stations:
            station_label = row.get("id", "<unknown>")
            try:
                state = str(row.get("state") or "").strip().upper()
                fuel_type = str(row.get("fuel_type_code") or "").strip().upper()
                if state != "NY" or fuel_type != "ELEC":
                    raise ValueError("뉴욕주 전기차 충전소가 아닙니다")

                borough = _borough(row.get("zip"))
                if borough is None:
                    continue

                station_id = int(row["id"])
                if station_id <= 0:
                    raise ValueError("station_id는 양수여야 합니다")
                if station_id in station_ids:
                    raise ValueError("station_id가 중복됩니다")

                price, status = _price_per_kwh(row.get("ev_pricing"))
                station_ids.add(station_id)
                nyc_station_count += 1

                if status == "normalized":
                    if price is None:
                        raise ValueError("표준화된 요금이 비어 있습니다")
                    prices.append(price)
            except (KeyError, TypeError, ValueError) as exc:
                errors.append(f"{bronze_path} id={station_label}: {exc}")

        if errors:
            raise ValueError("EV Charging Silver 변환 실패:\n- " + "\n- ".join(errors))
        if not nyc_station_count:
            raise ValueError("뉴욕시 충전소 데이터가 없습니다.")
        if not prices:
            raise ValueError("표준화 가능한 kWh 단위 요금이 없습니다.")
        return {
            "date": collected_at.date(),
            "ev_price": round(fmean(prices), 6),
        }
