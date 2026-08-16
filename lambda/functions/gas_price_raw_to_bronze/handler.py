"""AAA New York 휘발유 가격 Raw 수집과 Bronze 적재를 실행합니다."""

import os
from datetime import datetime, timezone

from pipeline_core.pipeline import Pipeline

from ..common.logging_setup import configure_lambda_logging
from .extractor import GasPriceHtmlExtractor, GasPriceSnapshotExtractor
from .loader import build_bronze_loader
from .snapshot import GasPriceSnapshotLoader

configure_lambda_logging()


def lambda_handler(event: dict | None = None, context=None) -> dict:
    event = event or {}
    base_dir = event.get("base_dir") or os.getenv("BRONZE_DIR", "data/bronze")
    storage = event.get("storage") or os.getenv("BRONZE_STORAGE", "local")
    bucket = event.get("bucket") or os.getenv("DATA_LAKE_S3_BUCKET")
    collected_at = datetime.now(timezone.utc)

    snapshot_result = Pipeline(
        GasPriceHtmlExtractor(),
        GasPriceSnapshotLoader(base_dir, collected_at),
    ).run()
    snapshot_location = snapshot_result.write_result.location

    record = GasPriceSnapshotExtractor(snapshot_location).extract()
    write_result = build_bronze_loader(
        storage, base_dir, collected_at, bucket=bucket
    ).write(record)
    price_date = datetime.strptime(record["price_date_raw"], "%m/%d/%y").date()

    return {
        "row_count": write_result.row_count,
        "locations": [write_result.location],
        "state": record["state"],
        "fuel_type": record["fuel_type"],
        "price_date": price_date.isoformat(),
        "collected_date": f"{collected_at:%Y-%m-%d}",
    }
