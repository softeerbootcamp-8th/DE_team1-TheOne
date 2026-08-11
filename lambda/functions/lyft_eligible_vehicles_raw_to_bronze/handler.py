"""Lyft Premium Eligible Vehicles 수집과 Raw/Bronze 적재 핸들러."""

import os
from datetime import datetime, timezone

from pipeline_core.pipeline import Pipeline

from ..common.logging_setup import configure_lambda_logging
from .extractor import CITY_SLUG, LyftEligibleVehiclesExtractor
from .loader import LyftEligibleVehiclesLoader, raw_file

configure_lambda_logging()


def lambda_handler(event: dict | None = None, context=None) -> dict:
    event = event or {}
    city_slug = event.get("city_slug") or os.getenv("CITY_SLUG", CITY_SLUG)
    raw_dir = event.get("raw_dir") or os.getenv("RAW_DIR", "data/raw")
    bronze_dir = (
        event.get("bronze_dir")
        or event.get("base_dir")
        or os.getenv("BRONZE_DIR", "data/bronze")
    )
    collected_at = datetime.now(timezone.utc)

    result = Pipeline(
        LyftEligibleVehiclesExtractor(city_slug, collected_at),
        LyftEligibleVehiclesLoader(raw_dir, bronze_dir, collected_at),
    ).run()

    return {
        "row_count": result.write_result.row_count,
        "locations": [result.write_result.location],
        "city_slug": city_slug,
        "collected_date": f"{collected_at:%Y-%m-%d}",
        # Bronze 산출물이 아니라 함께 남긴 Raw 스냅샷이라 locations 에 넣지 않습니다.
        "raw_path": str(raw_file(raw_dir, city_slug, collected_at)),
    }
