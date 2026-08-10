"""Lyft Eligible Vehicles Raw/Bronze Loader 계약 검증."""

import json
from datetime import datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq

from functions.lyft_eligible_vehicles.loader import (
    SCHEMA,
    LyftEligibleVehiclesLoader,
)

COLLECTED_AT = datetime(2026, 8, 10, 3, 0, tzinfo=timezone.utc)
RAW_MDX = "2018 (Extra Comfort) / 2021 (Black)<sup>2</sup>"


def test_loader가_raw_json과_bronze_parquet을_일별_적재한다(tmp_path):
    common = {
        "city_slug": "new-york",
        "source_url": "https://www.lyft.com/driver/eligible-premium-vehicles",
        "collected_at": COLLECTED_AT,
    }
    rows = [
        {
            **common,
            "make": "Acura",
            "model": "MDX",
            "min_year": 2018,
            "ride_types": ["Extra Comfort"],
            "raw_eligibility": RAW_MDX,
        },
        {
            **common,
            "make": "Acura",
            "model": "MDX",
            "min_year": 2021,
            "ride_types": ["Black"],
            "raw_eligibility": RAW_MDX,
        },
        {
            **common,
            "make": "Cadillac",
            "model": "ESCALADE ESV",
            "min_year": None,
            "ride_types": ["XXL"],
            "raw_eligibility": "2018 (Extra Comfort) / (XXL)",
        },
    ]

    result = LyftEligibleVehiclesLoader(
        str(tmp_path / "raw"),
        str(tmp_path / "bronze"),
        COLLECTED_AT,
    ).write(rows)

    path = Path(result.location)
    raw_path = next((tmp_path / "raw").rglob("*.json"))
    assert path.parent.name == "city=new-york"
    assert path.parent.parent.name == "collected_date=2026-08-10"
    assert raw_path.parent.name == "city=new-york"
    assert result.row_count == 3

    raw = json.loads(raw_path.read_text(encoding="utf-8"))
    assert len(raw["vehicles"]) == 2
    assert raw["vehicles"][0]["raw_eligibility"] == RAW_MDX

    table = pq.ParquetFile(path).read()
    assert table.schema.equals(SCHEMA)
    assert table.to_pylist()[2]["min_year"] is None
