"""EIA-861M 월간 주별 전력 판매·요금 원본 수집.

NLR 충전소 API(`ev_charging_stations_raw_to_bronze`)가 **오늘 스냅샷만** 주는 것과
달리, 이 파일에는 2010년부터의 주별·월별 요금이 들어 있습니다.

주의 — 이 값은 **전력 소매요금**이지 공공 충전소 요금이 아닙니다. 둘은 서로 다른
것을 재고 실측상 2배쯤 차이 납니다(EIA $0.207 vs NLR $0.417). 충전 단가로 쓰려면
마진 배수 보정이 필요하고, 그 처리는 `eia_fuel_price_bronze_to_silver` 에 있습니다.

`Electric Power Monthly` 의 Table 5.6.B 는 **연초 누적(YTD)** 이라 이력이 아닙니다.
이력이 필요하면 반드시 이 EIA-861M 파일을 써야 합니다.
"""

import logging

import requests
from pipeline_core.extractor import Extractor

logger = logging.getLogger(__name__)

# EIA-861M (Monthly Electric Power Industry Report) — Sales and Revenue
FILE_URL = "https://www.eia.gov/electricity/data/eia861m/xls/sales_revenue.xlsx"
# xlsx 는 zip 컨테이너입니다.
XLSX_MAGIC = b"PK\x03\x04"
MIN_BYTES = 100_000


def fetch(timeout: int = 120) -> bytes:
    """원본 bytes 를 변형 없이 반환합니다."""
    response = requests.get(FILE_URL, timeout=timeout)
    response.raise_for_status()

    body = response.content
    if len(body) < MIN_BYTES:
        raise ValueError(f"EIA 전력 파일이 너무 작습니다: {len(body)} bytes")
    if not body.startswith(XLSX_MAGIC):
        raise ValueError("EIA 전력 응답이 xlsx 형식이 아닙니다")
    logger.info("EIA 전력 원본 수신: %d bytes", len(body))
    return body


class EiaElectricityPriceExtractor(Extractor):
    """EIA-861M 월간 전력요금 이력 파일을 원본 bytes 로 수집합니다."""

    name = f"eia_electricity_price:{FILE_URL}"

    def __init__(self, timeout: int = 120):
        self._timeout = timeout

    def extract(self) -> dict:
        return {"body": fetch(self._timeout), "source_url": FILE_URL}
