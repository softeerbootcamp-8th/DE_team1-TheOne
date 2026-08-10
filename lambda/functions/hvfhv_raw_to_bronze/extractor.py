"""NYC FHVHV Trip Record 수집(extract).

수집 대상 URL: https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_{year}-{month}.parquet
"""

import logging

import requests
from pipeline_core.extractor import Extractor

logger = logging.getLogger(__name__)

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_{year_str}-{month_str}.parquet"


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
