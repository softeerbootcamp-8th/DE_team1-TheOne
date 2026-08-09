"""NLR API에서 뉴욕주 전기차 충전소 가격 정보를 수집합니다.

`ev_pricing`은 단가, 기본요금, 유휴요금 등이 섞인 자유 형식 문자열이므로
Extract 단계에서는 파싱하지 않고 원문 그대로 반환합니다.
"""

import logging
from datetime import datetime

import requests

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


def fetch(api_key: str, timeout: int = 60) -> list[dict]:
    """뉴욕주 전기차 충전소 전체를 NLR API에서 받아옵니다."""
    if not api_key.strip():
        raise ValueError("NLR API key가 비어 있습니다.")

    response = requests.get(
        API_URL,
        headers={"X-Api-Key": api_key},
        params=PARAMS,
        timeout=timeout,
    )
    response.raise_for_status()
    payload = response.json()
    stations = payload.get("fuel_stations")

    if not isinstance(stations, list) or not stations:
        raise RuntimeError("응답에 충전소 데이터가 비어 있습니다.")
    if payload.get("total_results") != len(stations):
        raise RuntimeError("API 결과가 일부만 반환되었습니다.")
    if any(
        station.get("state") != "NY" or station.get("fuel_type_code") != "ELEC"
        for station in stations
    ):
        raise RuntimeError("뉴욕주 전기차 충전소가 아닌 데이터가 포함되었습니다.")

    return stations


def parse(stations: list[dict], collected_at: datetime) -> list[dict]:
    """원본 응답에서 가격 분석과 적재에 필요한 필드만 선택합니다."""
    return [
        {
            "station_id": station["id"],
            "station_name": station.get("station_name"),
            "fuel_type_code": station.get("fuel_type_code"),
            "status_code": station.get("status_code"),
            "access_code": station.get("access_code"),
            "restricted_access": station.get("restricted_access"),
            "street_address": station.get("street_address"),
            "city": station.get("city"),
            "state": station.get("state"),
            "zip": station.get("zip"),
            "latitude": station.get("latitude"),
            "longitude": station.get("longitude"),
            "ev_network": station.get("ev_network"),
            "ev_network_web": station.get("ev_network_web"),
            "ev_connector_types": station.get("ev_connector_types"),
            "ev_level1_evse_num": station.get("ev_level1_evse_num"),
            "ev_level2_evse_num": station.get("ev_level2_evse_num"),
            "ev_dc_fast_num": station.get("ev_dc_fast_num"),
            "ev_pricing": station.get("ev_pricing"),
            "cards_accepted": station.get("cards_accepted"),
            "date_last_confirmed": station.get("date_last_confirmed"),
            "updated_at": station.get("updated_at"),
            "source_url": API_URL,
            "collected_at": collected_at,
        }
        for station in sorted(stations, key=lambda station: station["id"])
    ]


def extract(api_key: str, collected_at: datetime, timeout: int = 60) -> list[dict]:
    """수집 진입점 — API 호출과 Bronze용 필드 선택을 수행합니다."""
    logger.info("뉴욕주 전기차 충전소 수집 시작")
    rows = parse(fetch(api_key, timeout), collected_at)
    priced = sum(bool((row["ev_pricing"] or "").strip()) for row in rows)
    logger.info("수집 완료: %d개소 (가격 정보 %d개소)", len(rows), priced)
    return rows
