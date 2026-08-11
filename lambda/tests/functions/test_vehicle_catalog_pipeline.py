"""차량 대장 Raw -> Bronze -> Silver 배선 검증 (네트워크/OCR 없이 Loader부터 실행)."""

from datetime import datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from functions.common import vehicle_catalog_layout as layout
from functions.vehicle_catalog_bronze_to_silver.handler import (
    lambda_handler as to_silver,
)
from functions.vehicle_catalog_raw_to_bronze.loader import VehicleCatalogBronzeLoader

COLLECTED_AT = datetime(2026, 8, 10, 3, 0, tzinfo=timezone.utc)
COLLECTED_DATE = f"{COLLECTED_AT:%Y-%m-%d}"


def vehicle(make: str, model: str, price: float, collected_at=COLLECTED_AT) -> dict:
    """Bronze SCHEMA 를 만족하는 차량 한 대."""
    return {
        "vendor": "fasttrack",  # 파티션 키 (파일 안에는 안 들어갑니다)
        "make": make,
        "model": model,
        "raw_name": f"{make} {model}",
        "price_usd": price,
        "price_period": "week",
        "image_url": "https://example.com/card.png",
        "booking_url": "https://example.com/book",
        "source_url": "https://fasttrackleasingllc.com/vehicles-pricing/",
        "collected_at": collected_at,
    }


ROWS = [
    vehicle("Mitsubishi", "OUTLANDER SPORT", 554.0),
    vehicle("Kia", "RAV4", 649.0),
]


def write_bronze(bronze_dir: Path, rows: list[dict], collected_at=COLLECTED_AT) -> str:
    return VehicleCatalogBronzeLoader(str(bronze_dir), collected_at).write(rows).location


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

    location = write_bronze(bronze_dir, ROWS)
    # Bronze 를 쓰는 쪽과 Silver 가 읽는 쪽이 같은 업체 파티션을 봐야 합니다.
    assert Path(location).parent == layout.vendor_partition(
        str(bronze_dir), COLLECTED_DATE, "fasttrack"
    )

    result = run_silver(bronze_dir, silver_dir, COLLECTED_DATE)

    assert result["row_count"] == 2
    assert len(result["locations"]) == 1

    silver_path = Path(result["locations"][0])
    assert silver_path == layout.silver_file(
        str(silver_dir), COLLECTED_AT.date(), "fasttrack"
    )
    # 조인 키는 대문자로 정규화됩니다.
    written = pq.ParquetFile(silver_path).read().to_pylist()
    assert {row["make_key"] for row in written} == {"MITSUBISHI", "KIA"}
    assert {row["model_key"] for row in written} == {"OUTLANDER SPORT", "RAV4"}


def test_업체가_여럿이면_파티션도_나뉜다(tmp_path):
    bronze_dir, silver_dir = tmp_path / "bronze", tmp_path / "silver"

    write_bronze(bronze_dir, ROWS)
    other = [{**row, "vendor": "othervendor"} for row in ROWS]
    write_bronze(bronze_dir, other)

    result = run_silver(bronze_dir, silver_dir, COLLECTED_DATE)

    assert len(result["locations"]) == 2
    assert result["row_count"] == 4


def test_같은_수집일을_다시_변환하면_덮어쓴다(tmp_path):
    """재실행 결과가 여러 개 남으면 읽는 쪽에서 무엇이 맞는지 알 수 없습니다."""
    bronze_dir, silver_dir = tmp_path / "bronze", tmp_path / "silver"
    write_bronze(bronze_dir, ROWS)

    first = run_silver(bronze_dir, silver_dir, COLLECTED_DATE)
    second = run_silver(bronze_dir, silver_dir, COLLECTED_DATE)

    assert first["locations"] == second["locations"]
    partition = Path(first["locations"][0]).parent
    assert len(list(partition.glob("*.parquet"))) == 1


def test_요청한_수집일과_변환된_날짜가_다르면_실패한다(tmp_path):
    """엉뚱한 collected_date 파티션을 덮어쓰지 않아야 합니다."""
    bronze_dir, silver_dir = tmp_path / "bronze", tmp_path / "silver"

    # 파티션은 8월 10일인데 파일 안의 collected_at 은 8월 9일인 상황
    stale_at = datetime(2026, 8, 9, 3, 0, tzinfo=timezone.utc)
    write_bronze(bronze_dir, [vehicle("Kia", "RAV4", 649.0, stale_at)])

    with pytest.raises(ValueError, match="변환된 수집일이 다릅니다"):
        run_silver(bronze_dir, silver_dir, COLLECTED_DATE)


def test_Bronze_파티션이_없으면_실패한다(tmp_path):
    with pytest.raises(FileNotFoundError):
        run_silver(tmp_path / "bronze", tmp_path / "silver", COLLECTED_DATE)


def test_collected_date_형식을_검증한다(tmp_path):
    with pytest.raises(ValueError, match="YYYY-MM-DD"):
        run_silver(tmp_path / "bronze", tmp_path / "silver", "2026/08/10")
