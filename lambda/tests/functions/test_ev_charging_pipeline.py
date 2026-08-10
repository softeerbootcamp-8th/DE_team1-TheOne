"""EV Charging Raw -> Bronze -> Silver 배선 검증 (네트워크 없이 Loader부터 실행)."""

from datetime import datetime, timezone
from pathlib import Path

import pytest

from functions.common import ev_charging_layout as layout
from functions.ev_charging_stations.loader import EvChargingBronzeLoader
from functions.ev_charging_stations_bronze_to_silver.handler import (
    lambda_handler as to_silver,
)

COLLECTED_AT = datetime(2026, 8, 9, 12, 0, tzinfo=timezone.utc)
COLLECTED_DATE = f"{COLLECTED_AT:%Y-%m-%d}"


def station(station_id: int, zip_code: str, pricing: str | None) -> dict:
    """SCHEMA 를 만족하는 최소 충전소 행."""
    return {
        "station_id": station_id,
        "station_name": f"Station {station_id}",
        "fuel_type_code": "ELEC",
        "status_code": "E",
        "access_code": "public",
        "restricted_access": False,
        "street_address": "1 Test St",
        "city": "New York",
        "state": "NY",
        "zip": zip_code,
        "latitude": 40.7,
        "longitude": -74.0,
        "ev_network": "TestNet",
        "ev_network_web": "https://example.com",
        "ev_connector_types": ["J1772"],
        "ev_level1_evse_num": 0,
        "ev_level2_evse_num": 2,
        "ev_dc_fast_num": 0,
        "ev_pricing": pricing,
        "cards_accepted": None,
        "date_last_confirmed": "2026-08-01",
        "updated_at": "2026-08-01T00:00:00Z",
        "source_url": "https://developer.nlr.gov/api/alt-fuel-stations/v1.json",
        "collected_at": COLLECTED_AT,
    }


ROWS = [
    station(1, "10001", "$0.30/kWh"),  # Manhattan
    station(2, "11201", "$0.50 per kWh"),  # Brooklyn
    station(3, "10451", "Free"),  # Bronx, 평균에서 제외
    station(4, "90210", "$9.99/kWh"),  # NYC 밖이라 아예 제외
]


def write_bronze(bronze_dir: Path, rows: list[dict], collected_at: datetime) -> str:
    return EvChargingBronzeLoader(str(bronze_dir), collected_at).write(rows).location


def run_silver(bronze_dir: Path, silver_dir: Path, collected_date: str) -> dict:
    return to_silver(
        event={
            "collected_date": collected_date,
            "bronze_dir": str(bronze_dir),
            "silver_dir": str(silver_dir),
        }
    )


def test_bronze_to_silver(tmp_path):
    bronze_dir, silver_dir = tmp_path / "bronze", tmp_path / "silver"

    location = write_bronze(bronze_dir, ROWS, COLLECTED_AT)
    # Bronze 를 쓰는 쪽과 Silver 가 읽는 쪽이 같은 파티션을 봐야 합니다.
    assert Path(location).parent == layout.bronze_partition(
        str(bronze_dir), COLLECTED_DATE
    )

    result = run_silver(bronze_dir, silver_dir, COLLECTED_DATE)

    assert result["written_count"] == 1
    # NYC 의 kWh 요금만 평균에 들어갑니다 (Free 제외, NYC 밖 제외).
    assert result["average_price_usd_per_kwh"] == pytest.approx(0.40)
    assert result["nyc_station_count"] == 3
    assert result["normalized_price_count"] == 2
    assert result["free_station_count"] == 1
    assert Path(result["path"]).exists()


def test_같은_스냅샷을_다시_처리해도_덮어쓰지_않는다(tmp_path):
    bronze_dir, silver_dir = tmp_path / "bronze", tmp_path / "silver"
    write_bronze(bronze_dir, ROWS, COLLECTED_AT)

    first = run_silver(bronze_dir, silver_dir, COLLECTED_DATE)
    second = run_silver(bronze_dir, silver_dir, COLLECTED_DATE)

    assert first["written_count"] == 1
    assert second["written_count"] == 0  # 동일 collected_at 이라 그대로 유지
    assert first["path"] == second["path"]


def test_요청한_수집일과_정제된_날짜가_다르면_실패한다(tmp_path):
    """엉뚱한 price_date 파티션을 덮어쓰지 않아야 합니다."""
    bronze_dir, silver_dir = tmp_path / "bronze", tmp_path / "silver"

    # 파티션 이름은 8월 9일인데 내용물의 collected_at 은 8월 8일인 상황
    stale_at = datetime(2026, 8, 8, 12, 0, tzinfo=timezone.utc)
    rows = [{**row, "collected_at": stale_at} for row in ROWS]
    write_bronze(bronze_dir, rows, COLLECTED_AT)

    with pytest.raises(ValueError, match="정제된 price_date가 다릅니다"):
        run_silver(bronze_dir, silver_dir, COLLECTED_DATE)


def test_Bronze_파티션이_없으면_실패한다(tmp_path):
    with pytest.raises(FileNotFoundError):
        run_silver(tmp_path / "bronze", tmp_path / "silver", COLLECTED_DATE)


def test_collected_date_형식을_검증한다(tmp_path):
    with pytest.raises(ValueError, match="YYYY-MM-DD"):
        run_silver(tmp_path / "bronze", tmp_path / "silver", "2026/08/09")
