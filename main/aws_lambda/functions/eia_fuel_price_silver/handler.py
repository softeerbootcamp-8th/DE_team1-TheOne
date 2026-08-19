"""휘발유·전력 CLEAN Silver 두 개를 대상 월의 통합 연료비 Silver 로 붙입니다."""

import os

from pipeline_core.pipeline import Pipeline
from pipeline_core.transformer import Transformer

from shared.aws_lambda.common.logging_setup import configure_lambda_logging
from .extractor import build_clean_extractor
from .loader import build_silver_loader
from .transformer import combine_daily_prices

configure_lambda_logging()


class EiaFuelPriceCombineTransformer(Transformer):
    def __init__(self, year_month: str):
        self._year_month = year_month

    def transform(self, data: dict) -> list[dict]:
        return combine_daily_prices(
            self._year_month, data["gas_rows"], data["electricity_rows"]
        )


def lambda_handler(event: dict | None = None, context=None) -> dict:
    event = event or {}
    year_month = event.get("year_month") or os.getenv("YEAR_MONTH")
    if not year_month:
        raise ValueError("year_month 또는 YEAR_MONTH가 필요합니다 (YYYY-MM).")

    storage = event.get("storage") or os.getenv("BRONZE_STORAGE", "local")
    bucket = event.get("bucket") or os.getenv("DATA_LAKE_S3_BUCKET")
    silver_dir = event.get("silver_dir") or os.getenv("SILVER_DIR", "data/silver")

    result = Pipeline(
        build_clean_extractor(storage, silver_dir, bucket, year_month),
        build_silver_loader(storage, silver_dir, bucket, year_month),
        EiaFuelPriceCombineTransformer(year_month),
    ).run()

    return {
        "row_count": result.write_result.row_count,
        "locations": [result.write_result.location],
        "year_month": year_month,
    }
