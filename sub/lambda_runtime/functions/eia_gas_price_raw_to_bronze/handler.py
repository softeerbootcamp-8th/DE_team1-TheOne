"""EIA 주간 휘발유 원본 수집과 Bronze 적재를 실행합니다."""

import os
from datetime import datetime, timezone

from pipeline_core.pipeline import Pipeline

from shared.lambda_runtime.common.logging_setup import configure_lambda_logging
from .extractor import EiaGasPriceExtractor
from .loader import build_bronze_loader

configure_lambda_logging()


def lambda_handler(event: dict | None = None, context=None) -> dict:
    event = event or {}
    base_dir = event.get("base_dir") or os.getenv("BRONZE_DIR", "data/bronze")
    storage = event.get("storage") or os.getenv("BRONZE_STORAGE", "local")
    bucket = event.get("bucket") or os.getenv("DATA_LAKE_S3_BUCKET")
    collected_date = datetime.now(timezone.utc).date()

    result = Pipeline(
        EiaGasPriceExtractor(),
        build_bronze_loader(storage, base_dir, collected_date, bucket=bucket),
    ).run()

    return {
        "row_count": result.write_result.row_count,
        "locations": [result.write_result.location],
        "collected_date": collected_date.isoformat(),
        "source_url": EiaGasPriceExtractor.name.split(":", 1)[1],
    }
