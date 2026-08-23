"""EIA 월간 전력요금 원본 수집과 Bronze 적재를 실행합니다."""

import os
from datetime import datetime, timezone

from pipeline_core.pipeline import Pipeline

from shared.aws_lambda.common.logging_setup import configure_lambda_logging
from .extractor import EiaElectricityPriceExtractor
from .loader import build_bronze_loader

configure_lambda_logging()


def lambda_handler(event: dict | None = None, context=None) -> dict:
    event = event or {}
    base_dir = event.get("base_dir") or os.getenv("BRONZE_DIR", "data/bronze")
    storage = event.get("storage") or os.getenv("BRONZE_STORAGE", "local")
    bucket = event.get("bucket") or os.getenv("DATA_LAKE_S3_BUCKET")
    service_area = event.get("service_area") or os.getenv("SERVICE_AREA", "NYC")
    collected_date = datetime.now(timezone.utc).date()

    loader = build_bronze_loader(
        storage,
        base_dir,
        collected_date,
        bucket=bucket,
        service_area=service_area,
    )
    result = Pipeline(EiaElectricityPriceExtractor(), loader).run()

    return {
        "row_count": result.write_result.row_count,
        "locations": [result.write_result.location],
        "collected_date": collected_date.isoformat(),
        "source_url": EiaElectricityPriceExtractor.name.split(":", 1)[1],
    }
