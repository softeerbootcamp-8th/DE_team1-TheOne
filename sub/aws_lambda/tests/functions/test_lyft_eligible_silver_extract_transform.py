"""Lyft Eligible Vehicles Raw 추출과 Curated 변환 계약 검증."""

from datetime import datetime, timezone

import pytest

from sub.aws_lambda.functions.lyft_eligible_vehicles_raw_to_curated.extractor import (
    LyftEligibleVehiclesRawExtractor,
)
from sub.aws_lambda.functions.lyft_eligible_vehicles_raw_to_curated.transformer import (
    LyftEligibleVehiclesCuratedTransformer,
)
from sub.aws_lambda.functions.lyft_eligible_vehicles_source_to_raw.loader import (
    LyftEligibleVehiclesRawLoader,
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


def write_raw(base_dir, rows: list[dict], collected_at=COLLECTED_AT) -> str:
    return LyftEligibleVehiclesRawLoader(
        str(base_dir), CITY, collected_at
    ).write(rows).location


def test_최신_Raw를_읽고_Lyft_상품을_표준화한다(tmp_path):
    early = COLLECTED_AT.replace(hour=1)
    write_raw(tmp_path, [vehicle(2020, ["XL"])], early)
    latest_path = write_raw(
        tmp_path,
        [
            vehicle(2019, ["Black", "Black SUV only in select regions"]),
            vehicle(2020, ["Black only in select regions"]),
            vehicle(2018, ["Extra Comfort"]),
        ],
    )

    raw = LyftEligibleVehiclesRawExtractor(
        str(tmp_path), COLLECTED_DATE
    ).extract()
    curated = LyftEligibleVehiclesCuratedTransformer().transform(raw)

    assert {row["bronze_path"] for row in curated} == {latest_path}
    assert {row["make_key"] for row in curated} == {"CADILLAC"}
    assert {row["model_key"] for row in curated} == {"ESCALADE ESV"}
    assert [(row["product"], row["min_year"]) for row in curated] == [
        ("Black", 2019),
        ("Black SUV", 2019),
        ("Extra Comfort", 2018),
    ]


def test_알_수_없는_상품명은_Curated_변환에서_실패한다(tmp_path):
    write_raw(tmp_path, [vehicle(2024, ["Future Select"])])
    raw = LyftEligibleVehiclesRawExtractor(
        str(tmp_path), COLLECTED_DATE
    ).extract()

    with pytest.raises(ValueError, match="알 수 없는 Lyft 상품명.*Future Select"):
        LyftEligibleVehiclesCuratedTransformer().transform(raw)


def test_Raw_파티션이_없으면_실패한다(tmp_path):
    with pytest.raises(FileNotFoundError, match="Raw 파티션이 없습니다"):
        LyftEligibleVehiclesRawExtractor(str(tmp_path), COLLECTED_DATE).extract()


def test_collected_date_형식을_검증한다(tmp_path):
    with pytest.raises(ValueError, match="YYYY-MM-DD"):
        LyftEligibleVehiclesRawExtractor(str(tmp_path), "2026/08/10")
