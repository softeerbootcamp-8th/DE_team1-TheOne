"""AAA New York 휘발유 가격 Raw 수집과 Bronze 적재를 실행합니다."""

import json
import os
from datetime import datetime, timezone
from pathlib import Path

from pipeline_core.pipeline import Pipeline

from ..common.logging_setup import configure_lambda_logging
from .extractor import GasPriceExtractor
from .loader import GasPriceBronzeLoader

configure_lambda_logging()


def lambda_handler(event: dict | None = None, context=None) -> dict:
    event = event or {}
    base_dir = event.get("base_dir") or os.getenv("BRONZE_DIR", "data/bronze")
    collected_at = datetime.now(timezone.utc)

    result = Pipeline(
        GasPriceExtractor(),
        GasPriceBronzeLoader(base_dir, collected_at),
    ).run()
    location = result.write_result.location
    record = json.loads(Path(location).read_text(encoding="utf-8"))
    price_date = datetime.strptime(record["price_date_raw"], "%m/%d/%y").date()

    return {
        "row_count": result.write_result.row_count,
        "locations": [location],
        "state": record["state"],
        "fuel_type": record["fuel_type"],
        "price_date": price_date.isoformat(),
        "collected_date": f"{collected_at:%Y-%m-%d}",
    }
