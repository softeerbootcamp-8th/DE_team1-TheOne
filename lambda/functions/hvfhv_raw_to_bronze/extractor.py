"""NYC FHVHV Trip Record 수집(extract).

수집 대상 URL: https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_{year}-{month}.parquet
"""

import logging

import requests
from pipeline_core.extractor import Extractor

logger = logging.getLogger(__name__)

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_{year_str}-{month_str}.parquet"


def is_available(year_str: str, month_str: str, timeout: int = 30) -> bool:
    """해당 연월 원본이 TLC 에 올라와 있는지 확인합니다.

    TLC 는 두 달쯤 늦게 공개하고 그 지연 폭이 일정하지 않습니다. 달력으로 대상
    연월을 정하면 아직 없는 파일을 받으려다 매번 실패합니다(#345). 받기 전에
    있는지부터 봅니다 — 본문을 받지 않는 HEAD 라 수백 MB 를 끌지 않습니다.

    네트워크 오류는 "없음" 과 구분해야 합니다. 여기서 삼켜 버리면 일시적인
    장애가 "아직 공개 안 됨" 으로 둔갑해 조용히 아무것도 안 하게 됩니다.
    """
    url = BASE_URL.format(year_str=year_str, month_str=month_str)
    response = requests.head(url, timeout=timeout, allow_redirects=True)
    if response.status_code == 200:
        return True
    # CloudFront 는 없는 키에 403 을 돌려줍니다. 404 와 함께 "아직 없음" 으로 봅니다.
    if response.status_code in (403, 404):
        logger.info("아직 공개되지 않았습니다: %s (%d)", url, response.status_code)
        return False
    response.raise_for_status()
    raise RuntimeError(f"예상하지 못한 응답: {url} ({response.status_code})")


def fetch(year_str: str, month_str: str, timeout: int = 180) -> bytes:
    """원천 Parquet 파일을 HTTP GET 요청으로 다운로드합니다.
    
    데이터가 존재하지 않거나 네트워크 오류가 발생하면 람다 실패 처리 됩니다.
    """
    url = BASE_URL.format(year_str=year_str, month_str=month_str)
    logger.info("데이터 다운로드 시작: %s", url)
    
    response = requests.get(url, timeout=timeout)
    
    # RESPONSE 가 200번대가 아닐 경우 오류 처리
    response.raise_for_status()
    
    logger.info("데이터 다운로드 성공: %d bytes", len(response.content))
    return response.content


class HvfhvExtractor(Extractor):
    """대상 연월의 FHVHV Trip Record Parquet 을 원본 그대로 받아옵니다."""

    name = "hvfhv"

    def __init__(self, year_str: str, month_str: str, timeout: int = 180):
        self._year_str = year_str
        self._month_str = month_str
        self._timeout = timeout

    def extract(self) -> bytes:
        logger.info("수집 시작: 연도=%s, 월=%s", self._year_str, self._month_str)
        return fetch(self._year_str, self._month_str, self._timeout)
