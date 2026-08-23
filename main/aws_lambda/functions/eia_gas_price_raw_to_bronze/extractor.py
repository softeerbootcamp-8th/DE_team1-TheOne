"""EIA 주간 뉴욕 휘발유 소매가 원본 수집.

이 파일 하나에
2000년부터의 주간 이력이 통째로 들어 있습니다. 그래서 한 번 받으면 어느 과거 달이든
Silver 를 만들 수 있습니다.

원본 그대로 저장합니다 — 파싱은 `eia_gas_price_bronze_to_silver` 의 몫입니다.
Bronze 는 "EIA 가 이렇게 줬다"를 보관하는 계층이고, EIA 는 과거 값을 개정하므로
그때 받은 파일이 남아 있어야 결과 차이를 되짚을 수 있습니다.
"""

import logging

import requests
from pipeline_core.extractor import Extractor

from main.aws_lambda.common.eia_fuel_price_layout import GAS_MIN_BYTES as MIN_BYTES

logger = logging.getLogger(__name__)

# EIA Petroleum & Other Liquids — Weekly Regular Conventional Retail Gasoline Prices,
# 주(state)별 시리즈. 키는 이 프로젝트의 service_area 코드입니다.
FILE_URL_DICT = {
    "NYC": "https://www.eia.gov/dnav/pet/hist_xls/EMM_EPMR_PTE_SNY_DPGw.xls",
    "TX": "https://www.eia.gov/dnav/pet/hist_xls/EMM_EPMR_PTE_STX_DPGw.xls",
    "CA": "https://www.eia.gov/dnav/pet/hist_xls/EMM_EPMR_PTE_SCA_DPGw.xls",
    "FL": "https://www.eia.gov/dnav/pet/hist_xls/EMM_EPMR_PTE_SFL_DPGw.xls",
    "MA": "https://www.eia.gov/dnav/pet/hist_xls/EMM_EPMR_PTE_SMA_DPGw.xls",
}
# 구형 BIFF(.xls) 파일입니다. 시그니처가 바뀌면 다른 것을 받은 것이므로 즉시 실패시킵니다.
XLS_MAGIC = b"\xd0\xcf\x11\xe0"


def file_url(service_area: str) -> str:
    try:
        return FILE_URL_DICT[service_area]
    except KeyError:
        raise ValueError(
            f"EIA 휘발유 원본 URL이 등록되지 않은 지역입니다: {service_area!r} "
            f"(등록된 지역: {sorted(FILE_URL_DICT)})"
        ) from None


def fetch(service_area: str, timeout: int = 60) -> bytes:
    """원본 bytes 를 변형 없이 반환합니다."""
    url = file_url(service_area)
    response = requests.get(url, timeout=timeout)
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

    def __init__(self, service_area: str, timeout: int = 60):
        self._service_area = service_area
        self._timeout = timeout
        self.name = f"eia_gas_price:{file_url(service_area)}"

    def extract(self) -> dict:
        url = file_url(self._service_area)
        return {"body": fetch(self._service_area, self._timeout), "source_url": url}
