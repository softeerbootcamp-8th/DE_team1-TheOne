"""Lyft Eligible Vehicles Bronze 추출과 Silver 변환 계약 검증."""

from datetime import datetime, timezone

import pytest

from functions.lyft_eligible_vehicles_bronze_to_silver.extractor import (
    LyftEligibleVehiclesBronzeExtractor,
)
from functions.lyft_eligible_vehicles_bronze_to_silver.transformer import (
    LyftEligibleVehiclesSilverTransformer,
)
from functions.lyft_eligible_vehicles_raw_to_bronze.loader import (
    LyftEligibleVehiclesBronzeLoader,
)

CITY = "new-york"
COLLECTED_AT = datetime(2026, 8, 10, 3, 0, tzinfo=timezone.utc)
COLLECTED_DATE = f"{COLLECTED_AT:%Y-%m-%d}"


def vehicle(min_year: int, products: list[str]) -> dict:
    raw_eligibility = f"{min_year} ({', '.join(products)})"
    return {
        "city_slug": CITY,
        "make": "Cadillac",
        "model": "ESCALADE  ESV",
        "min_year": min_year,
        "products": products,
        "raw_eligibility": raw_eligibility,
        "raw_vehicle": f"__ESCALADE ESV__ - {raw_eligibility}",
        "source_url": "https://www.lyft.com/driver/eligible-premium-vehicles",
        "collected_at": COLLECTED_AT,
    }


def write_bronze(base_dir, rows: list[dict], collected_at=COLLECTED_AT) -> str:
    return LyftEligibleVehiclesBronzeLoader(
        str(base_dir), CITY, collected_at
    ).write(rows).location


def test_최신_Bronze를_읽고_Lyft_상품을_표준화한다(tmp_path):
    early = COLLECTED_AT.replace(hour=1)
    write_bronze(tmp_path, [vehicle(2020, ["XL"])], early)
    latest_path = write_bronze(
        tmp_path,
        [
            vehicle(2019, ["Black", "Black SUV only in select regions"]),
            vehicle(2020, ["Black only in select regions"]),
            vehicle(2018, ["Extra Comfort"]),
        ],
    )

    bronze = LyftEligibleVehiclesBronzeExtractor(
        str(tmp_path), COLLECTED_DATE
    ).extract()
    silver = LyftEligibleVehiclesSilverTransformer().transform(bronze)

    assert {row["bronze_path"] for row in silver} == {latest_path}
    assert {row["make_key"] for row in silver} == {"CADILLAC"}
    assert {row["model_key"] for row in silver} == {"ESCALADE ESV"}
    assert [(row["product"], row["min_year"]) for row in silver] == [
        ("Black", 2019),
        ("Black SUV", 2019),
        ("Extra Comfort", 2018),
    ]


def test_알_수_없는_상품명은_Silver_변환에서_실패한다(tmp_path):
    write_bronze(tmp_path, [vehicle(2024, ["Future Select"])])
    bronze = LyftEligibleVehiclesBronzeExtractor(
        str(tmp_path), COLLECTED_DATE
    ).extract()

    with pytest.raises(ValueError, match="알 수 없는 Lyft 상품명.*Future Select"):
        LyftEligibleVehiclesSilverTransformer().transform(bronze)


def test_Bronze_파티션이_없으면_실패한다(tmp_path):
    with pytest.raises(FileNotFoundError, match="Bronze 파티션이 없습니다"):
        LyftEligibleVehiclesBronzeExtractor(str(tmp_path), COLLECTED_DATE).extract()


def test_collected_date_형식을_검증한다(tmp_path):
    with pytest.raises(ValueError, match="YYYY-MM-DD"):
        LyftEligibleVehiclesBronzeExtractor(str(tmp_path), "2026/08/10")
