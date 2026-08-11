"""NLR API의 뉴욕주 전기차 충전소 응답 원문을 수집합니다."""

import json
import logging

import requests
from pipeline_core.extractor import Extractor

logger = logging.getLogger(__name__)

API_URL = "https://developer.nlr.gov/api/alt-fuel-stations/v1.json"
PARAMS = {
    "fuel_type": "ELEC",
    "state": "NY",
    "country": "US",
    "status": "all",
    "access": "all",
    "limit": "all",
}
STATE = "NY"
FUEL_TYPE_CODE = "ELEC"


def fetch(api_key: str, timeout: int = 60) -> bytes:
    """응답을 검증하되 저장할 원문 bytes는 변경하지 않고 반환합니다."""
    if not api_key.strip():
        raise ValueError("NLR API key가 비어 있습니다.")

    response = requests.get(
        API_URL,
        headers={"X-Api-Key": api_key},
        params=PARAMS,
        timeout=timeout,
    )
    response.raise_for_status()
    content = response.content
    if not content.strip():
        raise RuntimeError("NLR API 응답이 비어 있습니다.")

    try:
        payload = json.loads(content)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise RuntimeError("NLR API 응답이 올바른 JSON이 아닙니다.") from exc
    if not isinstance(payload, dict):
        raise RuntimeError("NLR API 응답이 JSON 객체 형식이 아닙니다.")

    stations = payload.get("fuel_stations")

    if not isinstance(stations, list) or not stations:
        raise RuntimeError("응답에 충전소 데이터가 비어 있습니다.")
    if payload.get("total_results") != len(stations):
        raise RuntimeError("API 결과가 일부만 반환되었습니다.")
    if any(
        station.get("state") != STATE
        or station.get("fuel_type_code") != FUEL_TYPE_CODE
        for station in stations
    ):
        raise RuntimeError("뉴욕주 전기차 충전소가 아닌 데이터가 포함되었습니다.")

    logger.info(
        "station_extract done rows=%d bytes=%d", len(stations), len(content)
    )
    return content


class EvChargingStationExtractor(Extractor):
    """NLR API에서 뉴욕주 전기차 충전소 스냅샷을 수집합니다."""

    name = "ev_charging_stations"

    def __init__(self, api_key: str, timeout: int = 60):
        self._api_key = api_key
        self._timeout = timeout

    def extract(self) -> bytes:
        logger.info("뉴욕주 전기차 충전소 수집 시작")
        return fetch(self._api_key, self._timeout)
