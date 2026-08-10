"""Lyft Eligible Vehicles Extractor 계약 검증."""

from datetime import datetime, timezone

from functions.lyft_eligible_vehicles import extractor


def test_extractor가_연식과_등급_묶음을_행으로_펼친다(monkeypatch):
    page_data = {
        "componentType": "FAQ",
        "displayName": extractor.VEHICLE_FAQ_NAME,
        "entries": [
            {
                "componentType": "FAQEntry",
                "question": "Cadillac",
                "answer": (
                    "__ESCALADE ESV__ - 2018 (Extra Comfort) / "
                    "2019 (Black, Black SUV) / (XXL)"
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

    assert [(row["min_year"], row["ride_types"]) for row in rows] == [
        (2018, ["Extra Comfort"]),
        (2019, ["Black", "Black SUV"]),
        (None, ["XXL"]),
    ]
