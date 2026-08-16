"""Lyft Premium Eligible Vehicles 수집과 Bronze 적재 핸들러."""

import os
from datetime import datetime, timezone

from pipeline_core.pipeline import Pipeline

from ..common.logging_setup import configure_lambda_logging
from .extractor import CITY_SLUG, LyftEligibleVehiclesExtractor
from .loader import build_bronze_loader

configure_lambda_logging()


def lambda_handler(event: dict | None = None, context=None) -> dict:
    event = event or {}
    city_slug = event.get("city_slug") or os.getenv("CITY_SLUG", CITY_SLUG)
    base_dir = event.get("base_dir") or os.getenv("BRONZE_DIR", "data/bronze")
    storage = event.get("storage") or os.getenv("BRONZE_STORAGE", "local")
    bucket = event.get("bucket") or os.getenv("DATA_LAKE_S3_BUCKET")
    collected_at = datetime.now(timezone.utc)

    result = Pipeline(
        LyftEligibleVehiclesExtractor(city_slug, collected_at),
        build_bronze_loader(storage, base_dir, city_slug, collected_at, bucket=bucket),
    ).run()

    return {
        "row_count": result.write_result.row_count,
        "locations": [result.write_result.location],
        "city_slug": city_slug,
        "collected_date": f"{collected_at:%Y-%m-%d}",
    }
