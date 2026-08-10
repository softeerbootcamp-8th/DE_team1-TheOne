"""AAA New York 휘발유 가격 Raw 수집과 Bronze 적재를 실행합니다."""

import os
from datetime import datetime, timezone

from pipeline_core.pipeline import Pipeline

from ..common import gas_price_layout as layout
from ..common.logging_setup import configure_lambda_logging
from .extractor import FUEL_TYPE, STATE, GasPriceExtractor
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

    return {
        "state": STATE,
        "fuel_type": FUEL_TYPE,
        "price_date": layout.price_date_from_bronze_file(result.write_result.location),
        # Silver 배치가 이 하루치 파티션만 처리합니다 (Bronze 파티션 키와 동일).
        "collected_date": f"{collected_at:%Y-%m-%d}",
        "row_count": result.write_result.row_count,
        "path": result.write_result.location,
    }
