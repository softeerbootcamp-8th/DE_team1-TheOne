"""Uber 배차 가능 목록 Bronze -> Silver 배선 검증 (네트워크 없이 Loader부터 실행)."""

from datetime import datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from functions.common import uber_eligible_vehicles_layout as layout
from functions.uber_eligible_vehicles_bronze_to_silver.handler import (
    lambda_handler as to_silver,
)
from functions.uber_eligible_vehicles_raw_to_bronze.loader import (
    UberEligibleVehiclesBronzeLoader,
)

COLLECTED_AT = datetime(2026, 8, 10, 3, 0, tzinfo=timezone.utc)
COLLECTED_DATE = f"{COLLECTED_AT:%Y-%m-%d}"
CITY = "new-york"


def vehicle(make, model, min_year, products, collected_at=COLLECTED_AT) -> dict:
    """Bronze SCHEMA 를 만족하는 연식 묶음 한 줄."""
    return {
        "city_slug": CITY,
        "make": make,
        "model": model,
        "min_year": min_year,
        "products": products,
        "raw_eligibility": f"{min_year} ({', '.join(products)})",
        "collected_at": collected_at,
    }


def write_bronze(bronze_dir: Path, rows: list[dict], city=CITY) -> str:
    loader = UberEligibleVehiclesBronzeLoader(str(bronze_dir), city, COLLECTED_AT)
    return loader.write(rows).location


def run_silver(bronze_dir: Path, silver_dir: Path, collected_date: str) -> dict:
    return to_silver(
        event={
            "collected_date": collected_date,
            "bronze_dir": str(bronze_dir),
            "silver_dir": str(silver_dir),
        }
    )


def test_상품별로_한_행씩_펼친다(tmp_path):
    bronze_dir, silver_dir = tmp_path / "bronze", tmp_path / "silver"
    write_bronze(
        bronze_dir,
        [
            vehicle("Acura", "ZDX", 2010, ["UberX", "Comfort"]),
            vehicle("Acura", "ZDX", 2018, ["Comfort Electric"]),
        ],
    )

    result = run_silver(bronze_dir, silver_dir, COLLECTED_DATE)

    assert result["row_count"] == 3
    assert len(result["locations"]) == 1

    silver_path = Path(result["locations"][0])
    assert silver_path == layout.silver_file(
        str(silver_dir), COLLECTED_AT.date(), CITY
    )

    written = pq.ParquetFile(silver_path).read().to_pylist()
    assert [(row["product"], row["min_year"]) for row in written] == [
        ("Comfort", 2010),
        ("Comfort Electric", 2018),
        ("UberX", 2010),
    ]


def test_같은_상품이_여러_연식에_나오면_낮은_쪽을_남긴다(tmp_path):
    """min_year 는 '허용되는 가장 오래된 연식' 이라 낮은 쪽이 실제 기준입니다."""
    bronze_dir, silver_dir = tmp_path / "bronze", tmp_path / "silver"
    write_bronze(
        bronze_dir,
        [
            vehicle("Kia", "NIRO", 2019, ["UberX"]),
            vehicle("Kia", "NIRO", 2015, ["UberX"]),
        ],
    )

    result = run_silver(bronze_dir, silver_dir, COLLECTED_DATE)

    written = pq.ParquetFile(result["locations"][0]).read().to_pylist()
    assert len(written) == 1
    assert written[0]["min_year"] == 2015


def test_조인_키가_차량_대장과_같은_규칙으로_만들어진다(tmp_path):
    """대장은 OCR 이라 대문자, uber 는 일반 표기입니다. 키가 같아야 붙습니다."""
    from functions.vehicle_catalog_bronze_to_silver.transformer import (
        VehicleCatalogSilverTransformer,
    )

    bronze_dir, silver_dir = tmp_path / "bronze", tmp_path / "silver"
    write_bronze(
        bronze_dir, [vehicle("Mitsubishi", "Outlander  Sport", 2018, ["UberX"])]
    )
    result = run_silver(bronze_dir, silver_dir, COLLECTED_DATE)
    uber_row = pq.ParquetFile(result["locations"][0]).read().to_pylist()[0]

    catalog_row = VehicleCatalogSilverTransformer().transform(
        [
            {
                "vendor": "fasttrack",
                "make": "MITSUBISHI",
                "model": "OUTLANDER SPORT",
                "raw_name": "MITSUBISHI OUTLANDER SPORT",
                "price_usd": 554.0,
                "price_period": "week",
                "source_url": "https://example.com",
                "collected_at": COLLECTED_AT,
            }
        ]
    )[0]

    assert (uber_row["make_key"], uber_row["model_key"]) == (
        catalog_row["make_key"],
        catalog_row["model_key"],
    )


def test_도시가_여럿이면_파티션도_나뉜다(tmp_path):
    bronze_dir, silver_dir = tmp_path / "bronze", tmp_path / "silver"
    rows = [vehicle("Kia", "NIRO", 2019, ["UberX"])]
    write_bronze(bronze_dir, rows)
    write_bronze(bronze_dir, [{**rows[0], "city_slug": "chicago"}], city="chicago")

    result = run_silver(bronze_dir, silver_dir, COLLECTED_DATE)

    assert len(result["locations"]) == 2
    assert result["row_count"] == 2


def test_같은_수집일을_다시_변환하면_덮어쓴다(tmp_path):
    bronze_dir, silver_dir = tmp_path / "bronze", tmp_path / "silver"
    write_bronze(bronze_dir, [vehicle("Kia", "NIRO", 2019, ["UberX"])])

    first = run_silver(bronze_dir, silver_dir, COLLECTED_DATE)
    second = run_silver(bronze_dir, silver_dir, COLLECTED_DATE)

    assert first["locations"] == second["locations"]
    partition = Path(first["locations"][0]).parent
    assert len(list(partition.glob("*.parquet"))) == 1


def test_요청한_수집일과_변환된_날짜가_다르면_실패한다(tmp_path):
    bronze_dir, silver_dir = tmp_path / "bronze", tmp_path / "silver"
    stale = COLLECTED_AT.replace(day=9)
    write_bronze(bronze_dir, [vehicle("Kia", "NIRO", 2019, ["UberX"], stale)])

    with pytest.raises(ValueError, match="변환된 수집일이 다릅니다"):
        run_silver(bronze_dir, silver_dir, COLLECTED_DATE)


def test_Bronze_파티션이_없으면_실패한다(tmp_path):
    with pytest.raises(FileNotFoundError):
        run_silver(tmp_path / "bronze", tmp_path / "silver", COLLECTED_DATE)


def test_collected_date_형식을_검증한다(tmp_path):
    with pytest.raises(ValueError, match="YYYY-MM-DD"):
        run_silver(tmp_path / "bronze", tmp_path / "silver", "2026/08/10")
