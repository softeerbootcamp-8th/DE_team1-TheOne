"""EIA 월간 전력요금 원본 수집과 Bronze 적재를 실행합니다."""

import os
from datetime import datetime, timezone

from pipeline_core.pipeline import Pipeline

from main.aws_lambda.common import eia_fuel_price_layout as layout
from main.aws_lambda.common.monthly_dataset import (
    collected_at_from_token,
    collected_at_token,
)
from shared.aws_lambda.common.logging_setup import configure_lambda_logging
from shared.aws_lambda.common.storage_config import resolve_storage
from .extractor import EiaElectricityPriceExtractor
from .loader import build_bronze_loader

configure_lambda_logging()


def lambda_handler(event: dict | None = None, context=None) -> dict:
    event = event or {}
    base_dir = event.get("base_dir") or os.getenv("BRONZE_DIR", "data/bronze")
    storage = resolve_storage(event)
    bucket = event.get("bucket") or os.getenv("DATA_LAKE_S3_BUCKET")
    service_area = event.get("service_area") or os.getenv("SERVICE_AREA", "NYC")
    collected_at = event.get("collected_at") or (
        datetime.now(timezone.utc)
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )
    collected_at = collected_at_from_token(collected_at_token(collected_at))

    loader = build_bronze_loader(
        storage,
        base_dir,
        collected_at,
        bucket=bucket,
        service_area=service_area,
    )
    result = Pipeline(EiaElectricityPriceExtractor(), loader).run()

    actual_collected_at = layout.bronze_collected_at(result.write_result.location)
    return {
        "row_count": result.write_result.row_count,
        "locations": [result.write_result.location],
        "collected_at": actual_collected_at,
        "collected_date": actual_collected_at[:10],
        "source_url": EiaElectricityPriceExtractor.name.split(":", 1)[1],
    }
