"""Lyft Eligible Vehicles Bronze -> Silver 전체 Pipeline 검증."""

from datetime import datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from shared.lambda_runtime.common import lyft_eligible_vehicles_layout as layout
from sub.lambda_runtime.functions.lyft_eligible_vehicles_bronze_to_silver.handler import (
    lambda_handler as to_silver,
)
from sub.lambda_runtime.functions.lyft_eligible_vehicles_bronze_to_silver.loader import (
    SCHEMA,
)
from sub.lambda_runtime.functions.lyft_eligible_vehicles_raw_to_bronze.loader import (
    LyftEligibleVehiclesBronzeLoader,
)
from sub.lambda_runtime.functions.uber_eligible_vehicles_bronze_to_silver.loader import (
    SCHEMA as UBER_SCHEMA,
)

COLLECTED_AT = datetime(2026, 8, 10, 3, 0, tzinfo=timezone.utc)
COLLECTED_DATE = f"{COLLECTED_AT:%Y-%m-%d}"
CITY = "new-york"


def vehicle(min_year: int, products: list[str], collected_at=COLLECTED_AT) -> dict:
    raw_eligibility = f"{min_year} ({', '.join(products)})"
    return {
        "city_slug": CITY,
        "make": "Cadillac",
        "model": "ESCALADE ESV",
        "min_year": min_year,
        "products": products,
        "raw_eligibility": raw_eligibility,
        "raw_vehicle": f"__ESCALADE ESV__ - {raw_eligibility}",
        "source_url": "https://www.lyft.com/driver/eligible-premium-vehicles",
        "collected_at": collected_at,
    }


def write_bronze(bronze_dir: Path, rows: list[dict]) -> str:
    return LyftEligibleVehiclesBronzeLoader(
        str(bronze_dir), CITY, COLLECTED_AT
    ).write(rows).location


def run_silver(bronze_dir: Path, silver_dir: Path) -> dict:
    return to_silver(
        event={
            "collected_date": COLLECTED_DATE,
            "bronze_dir": str(bronze_dir),
            "silver_dir": str(silver_dir),
        }
    )


def test_Handler가_도시별_Silver_Parquet을_적재한다(tmp_path):
    bronze_dir, silver_dir = tmp_path / "bronze", tmp_path / "silver"
    write_bronze(
        bronze_dir,
        [vehicle(2019, ["Black", "Black SUV only in select regions"])],
    )

    result = run_silver(bronze_dir, silver_dir)

    expected = (
        layout.date_partition(str(silver_dir), COLLECTED_DATE)
        / f"{layout.CITY_PARTITION_KEY}={CITY}"
        / f"{layout.DATASET}.parquet"
    )
    assert result == {
        "row_count": 2,
        "locations": [str(expected)],
        "collected_date": COLLECTED_DATE,
    }

    table = pq.ParquetFile(expected).read()
    assert SCHEMA.equals(UBER_SCHEMA)
    assert table.schema.equals(SCHEMA)
    assert [row["product"] for row in table.to_pylist()] == ["Black", "Black SUV"]


def test_같은_날짜를_재실행하면_같은_파일을_덮어쓴다(tmp_path):
    bronze_dir, silver_dir = tmp_path / "bronze", tmp_path / "silver"
    write_bronze(bronze_dir, [vehicle(2019, ["Black"])])

    first = run_silver(bronze_dir, silver_dir)
    second = run_silver(bronze_dir, silver_dir)

    assert first["locations"] == second["locations"]
    partition = Path(first["locations"][0]).parent
    assert len(list(partition.glob("*.parquet"))) == 1


def test_요청일과_Bronze_수집일이_다르면_적재하지_않는다(tmp_path):
    bronze_dir, silver_dir = tmp_path / "bronze", tmp_path / "silver"
    stale = COLLECTED_AT.replace(day=9)
    write_bronze(bronze_dir, [vehicle(2019, ["Black"], stale)])

    with pytest.raises(ValueError, match="변환된 수집일이 다릅니다"):
        run_silver(bronze_dir, silver_dir)
