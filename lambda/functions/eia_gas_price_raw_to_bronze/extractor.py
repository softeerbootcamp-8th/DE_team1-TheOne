"""EIA 주간 뉴욕 휘발유 소매가 원본 수집.

AAA(`gas_price_raw_to_bronze`)가 **오늘 가격만** 주는 것과 달리, 이 파일 하나에
2000년부터의 주간 이력이 통째로 들어 있습니다. 그래서 한 번 받으면 어느 과거 달이든
Silver 를 만들 수 있습니다.

원본 그대로 저장합니다 — 파싱은 `eia_fuel_price_bronze_to_silver` 의 몫입니다.
Bronze 는 "EIA 가 이렇게 줬다"를 보관하는 계층이고, EIA 는 과거 값을 개정하므로
그때 받은 파일이 남아 있어야 결과 차이를 되짚을 수 있습니다.
"""

import logging

import requests
from pipeline_core.extractor import Extractor

from ..common.eia_fuel_price_layout import GAS_MIN_BYTES as MIN_BYTES

logger = logging.getLogger(__name__)

# EIA Petroleum & Other Liquids — Weekly New York Regular Conventional Retail Gasoline Prices
FILE_URL = "https://www.eia.gov/dnav/pet/hist_xls/EMM_EPMR_PTE_SNY_DPGw.xls"
# 구형 BIFF(.xls) 파일입니다. 시그니처가 바뀌면 다른 것을 받은 것이므로 즉시 실패시킵니다.
XLS_MAGIC = b"\xd0\xcf\x11\xe0"


def fetch(timeout: int = 60) -> bytes:
    """원본 bytes 를 변형 없이 반환합니다."""
    response = requests.get(FILE_URL, timeout=timeout)
    response.raise_for_status()

    body = response.content
    if len(body) < MIN_BYTES:
        raise ValueError(f"EIA 휘발유 파일이 너무 작습니다: {len(body)} bytes")
    if not body.startswith(XLS_MAGIC):
        raise ValueError("EIA 휘발유 응답이 xls 형식이 아닙니다")
    logger.info("EIA 휘발유 원본 수신: %d bytes", len(body))
    return body


class EiaGasPriceExtractor(Extractor):
    """EIA 주간 휘발유 이력 파일을 원본 bytes 로 수집합니다."""

    name = f"eia_gas_price:{FILE_URL}"

    def __init__(self, timeout: int = 60):
        self._timeout = timeout

    def extract(self) -> dict:
        return {"body": fetch(self._timeout), "source_url": FILE_URL}
