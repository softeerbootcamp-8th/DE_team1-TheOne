"""NYC FHVHV Trip Record 수집(extract).

수집 대상 URL: https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_{year}-{month}.parquet
"""

import logging
import requests

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


def extract(year_str: str, month_str: str) -> bytes:
    """수집 진입점 — URL 데이터 fetch 결과를 반환합니다."""
    logger.info("수집 시작: 연도=%s, 월=%s", year_str, month_str)
    return fetch(year_str, month_str)
