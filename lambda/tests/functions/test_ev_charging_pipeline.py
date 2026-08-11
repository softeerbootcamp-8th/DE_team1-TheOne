"""EV Charging 원본 Bronze와 월별 Silver 파이프라인을 검증합니다."""

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from functions.common import ev_charging_layout as layout
from functions.ev_charging_stations_bronze_to_silver.handler import (
    lambda_handler as to_silver,
)
from functions.ev_charging_stations_bronze_to_silver.loader import SCHEMA
from functions.ev_charging_stations_raw_to_bronze import extractor as raw_extractor
from functions.ev_charging_stations_raw_to_bronze.extractor import API_URL
from functions.ev_charging_stations_raw_to_bronze.loader import (
    EvChargingBronzeLoader,
)

COLLECTED_AT = datetime(2026, 8, 9, 12, 0, tzinfo=timezone.utc)


def station(station_id: int, zip_code: str, pricing: str | None) -> dict:
    return {
        "id": station_id,
        "state": "NY",
        "fuel_type_code": "ELEC",
        "zip": zip_code,
        "ev_pricing": pricing,
        "field_not_used_by_silver": {"keep": True},
    }


def response(stations: list[dict]) -> bytes:
    return json.dumps(
        {
            "total_results": len(stations),
            "station_counts": {"total": len(stations)},
            "fuel_stations": stations,
        },
        ensure_ascii=False,
    ).encode()


DAY_ONE_STATIONS = [
    station(1, "10001", "$0.30/kWh"),
    station(2, "11201", "$0.50 per kWh"),
    station(3, "10451", "Free"),
    station(4, "90210", "$9.99/kWh"),
]
RAW_RESPONSE = response(DAY_ONE_STATIONS)


def write_bronze(bronze_dir: Path, stations: list[dict], collected_at: datetime) -> Path:
    result = EvChargingBronzeLoader(str(bronze_dir), collected_at).write(
        response(stations)
    )
    return Path(result.location)


def run_silver(bronze_dir: Path, silver_dir: Path, month: str = "2026-08") -> dict:
    return to_silver(
        {
            "collected_month": month,
            "bronze_dir": str(bronze_dir),
            "silver_dir": str(silver_dir),
        }
    )


def read_silver(path: Path) -> list[dict]:
    return pq.ParquetFile(path).read().to_pylist()


def test_extractor가_API_응답_bytes를_그대로_반환한다(monkeypatch):
    class Response:
        content = RAW_RESPONSE

        @staticmethod
        def raise_for_status():
            return None

    monkeypatch.setattr(
        raw_extractor.requests,
        "get",
        lambda *args, **kwargs: Response(),
    )

    result = raw_extractor.EvChargingStationExtractor("test-key").extract()

    assert result == RAW_RESPONSE


def test_raw_to_bronze가_전체_JSON_원문을_그대로_저장한다(tmp_path):
    result = EvChargingBronzeLoader(str(tmp_path), COLLECTED_AT).write(RAW_RESPONSE)
    path = Path(result.location)

    assert path == layout.bronze_file(str(tmp_path), COLLECTED_AT)
    assert path.read_bytes() == RAW_RESPONSE
    assert list(path.parent.iterdir()) == [path]
    assert result.row_count == 1


def test_bronze_원문_여러날짜를_월별_silver_parquet으로_변환한다(tmp_path):
    bronze_dir, silver_dir = tmp_path / "bronze", tmp_path / "silver"
    first_path = write_bronze(bronze_dir, DAY_ONE_STATIONS, COLLECTED_AT)
    second_at = COLLECTED_AT + timedelta(days=1)
    second_path = write_bronze(
        bronze_dir,
        [
            station(1, "10001", "$0.60/kWh"),
            station(2, "11201", "$0.80 per kWh"),
            station(3, "10451", "Free"),
        ],
        second_at,
    )
    write_bronze(
        bronze_dir,
        [station(9, "10001", "$9.00/kWh")],
        datetime(2026, 9, 1, 12, 0, tzinfo=timezone.utc),
    )

    result = run_silver(bronze_dir, silver_dir)
    silver_path = layout.silver_file(str(silver_dir), "2026-08")
    table = pq.ParquetFile(silver_path).read()
    rows = table.to_pylist()

    assert result == {
        "row_count": 2,
        "locations": [str(silver_path)],
        "collected_month": "2026-08",
    }
    assert table.schema == SCHEMA
    assert [row["price_date"] for row in rows] == [
        date(2026, 8, 9),
        date(2026, 8, 10),
    ]
    assert [row["average_price_usd_per_kwh"] for row in rows] == pytest.approx(
        [0.4, 0.7]
    )
    assert [row["collected_at"] for row in rows] == [COLLECTED_AT, second_at]
    assert rows[0]["nyc_station_count"] == 3
    assert rows[0]["normalized_price_count"] == 2
    assert rows[0]["free_station_count"] == 1
    assert rows[0]["source_url"] == API_URL
    assert rows[0]["bronze_path"] == str(first_path)
    assert rows[1]["bronze_path"] == str(second_path)


def test_같은_날짜는_최신_스냅샷으로_월파일을_교체한다(tmp_path):
    bronze_dir, silver_dir = tmp_path / "bronze", tmp_path / "silver"
    write_bronze(bronze_dir, DAY_ONE_STATIONS, COLLECTED_AT)
    first = run_silver(bronze_dir, silver_dir)

    latest_at = COLLECTED_AT + timedelta(hours=6)
    latest_path = write_bronze(
        bronze_dir,
        [
            station(1, "10001", "$0.80/kWh"),
            station(2, "11201", "$1.00 per kWh"),
        ],
        latest_at,
    )
    second = run_silver(bronze_dir, silver_dir)

    silver_path = Path(second["locations"][0])
    rows = read_silver(silver_path)
    assert first["row_count"] == second["row_count"] == 1
    assert first["locations"] == second["locations"]
    assert len(list(silver_path.parent.glob("*.parquet"))) == 1
    assert len(rows) == 1
    assert rows[0]["average_price_usd_per_kwh"] == pytest.approx(0.9)
    assert rows[0]["collected_at"] == latest_at
    assert rows[0]["bronze_path"] == str(latest_path)


def test_collected_month_형식이_잘못되면_실패한다(tmp_path):
    with pytest.raises(ValueError, match="YYYY-MM"):
        run_silver(tmp_path / "bronze", tmp_path / "silver", "2026-8")


def test_대상_월의_bronze_JSON이_없으면_실패한다(tmp_path):
    with pytest.raises(FileNotFoundError, match="Bronze JSON 파일이 없습니다"):
        run_silver(tmp_path / "bronze", tmp_path / "silver")
