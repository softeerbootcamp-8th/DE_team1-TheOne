"""Lyft Eligible Vehicles Source -> Raw 적재 계약 검증."""

from datetime import datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq

from sub.aws_lambda.functions.lyft_eligible_vehicles_source_to_raw.loader import (
    SCHEMA,
    LyftEligibleVehiclesRawLoader,
)

COLLECTED_AT = datetime(2026, 8, 10, 3, 0, tzinfo=timezone.utc)


def row(model, min_year, products, raw_vehicle) -> dict:
    return {
        "city_slug": "new-york",
        "make": "Cadillac",
        "model": model,
        "min_year": min_year,
        "products": products,
        "raw_eligibility": None if model is None else raw_vehicle.split(" - ", 1)[-1],
        "raw_vehicle": raw_vehicle,
        "source_url": "https://www.lyft.com/driver/eligible-premium-vehicles",
        "collected_at": COLLECTED_AT,
    }


def test_loader가_파싱_결과를_선별하지_않고_Raw에_저장한다(tmp_path):
    rows = [
        row("ESCALADE ESV", 2019, ["Black", "Black SUV"], "ESCALADE raw"),
        row("LYRIQ", 2024, ["Future Select"], "LYRIQ raw"),
    ]

    result = LyftEligibleVehiclesRawLoader(
        str(tmp_path),
        "new-york",
        COLLECTED_AT,
    ).write(rows)

    path = Path(result.location)
    table = pq.ParquetFile(path).read()
    assert path.parent.name == "city=new-york"
    assert path.parent.parent.name == "collected_date=2026-08-10"
    assert table.schema.equals(SCHEMA)
    assert table.to_pylist() == rows
    assert result.row_count == 2
