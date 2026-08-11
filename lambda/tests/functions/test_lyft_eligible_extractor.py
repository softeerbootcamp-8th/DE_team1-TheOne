"""Lyft Eligible Vehicles 원문 추출 계약 검증."""

from datetime import datetime, timezone

from functions.lyft_eligible_vehicles_raw_to_bronze import extractor


def test_연식이_없는_묶음은_건너뛰고_상품명은_선별하지_않는다(monkeypatch):
    page_data = {
        "componentType": "FAQ",
        "displayName": extractor.VEHICLE_FAQ_NAME,
        "entries": [
            {
                "componentType": "FAQEntry",
                "question": "Cadillac",
                "answer": (
                    "__ESCALADE ESV__ - 2018 (Extra Comfort) / "
                    "2019 (Black, Black SUV) / (XXL)\n"
                    "__LYRIQ__ - 2024 (Future Select)\n"
                    "*Black exterior is required"
                ),
            }
        ],
    }
    collected_at = datetime(2026, 8, 10, tzinfo=timezone.utc)
    monkeypatch.setattr(extractor, "fetch", lambda timeout: page_data)

    rows = extractor.LyftEligibleVehiclesExtractor(
        "new-york",
        collected_at,
    ).extract()

    assert [(row["min_year"], row["products"]) for row in rows] == [
        (2018, ["Extra Comfort"]),
        (2019, ["Black", "Black SUV"]),
        (2024, ["Future Select"]),
    ]
    assert all(row["min_year"] is not None for row in rows)
    assert rows[0]["raw_vehicle"].startswith("__ESCALADE ESV__")
