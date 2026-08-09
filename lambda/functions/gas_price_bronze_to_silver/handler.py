"""Gas Price Bronze 월별 배치를 일별 Silver JSON으로 변환합니다."""

import os

from pipeline_core.pipeline import Pipeline

from ..common.logging_setup import configure_lambda_logging
from .extractor import GasPriceBronzeExtractor
from .loader import GasPriceSilverLoader
from .transformer import GasPriceSilverTransformer

configure_lambda_logging()


def lambda_handler(event: dict | None = None, context=None) -> dict:
    event = event or {}
    collected_month = event.get("collected_month") or os.getenv("COLLECTED_MONTH")
    if not collected_month:
        raise ValueError("collected_month 또는 COLLECTED_MONTH가 필요합니다.")

    bronze_dir = event.get("bronze_dir") or os.getenv("BRONZE_DIR", "data/bronze")
    silver_dir = event.get("silver_dir") or os.getenv("SILVER_DIR", "data/silver")

    result = Pipeline(
        GasPriceBronzeExtractor(bronze_dir, collected_month),
        GasPriceSilverLoader(silver_dir),
        transformer=GasPriceSilverTransformer(),
    ).run()

    return {
        "collected_month": collected_month,
        "row_count": result.write_result.row_count,
        "location": result.write_result.location,
    }
