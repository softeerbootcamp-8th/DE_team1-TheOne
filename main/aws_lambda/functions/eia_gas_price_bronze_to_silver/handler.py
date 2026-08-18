"""EIA 휘발유 Bronze 를 대상 월의 일별 단가 Silver 로 변환합니다."""

import os

from pipeline_core.pipeline import Pipeline
from pipeline_core.transformer import Transformer

from shared.aws_lambda.common.logging_setup import configure_lambda_logging
from .extractor import EiaGasPriceBronzeExtractor
from .loader import EiaGasPriceSilverLoader
from .transformer import build_daily_prices

configure_lambda_logging()


class EiaGasPriceTransformer(Transformer):
    def __init__(self, year_month: str):
        self._year_month = year_month

    def transform(self, data: dict) -> list[dict]:
        return build_daily_prices(
            self._year_month, data["gas_body"], data["bronze_collected_date"]
        )


def lambda_handler(event: dict | None = None, context=None) -> dict:
    event = event or {}
    year_month = event.get("year_month") or os.getenv("YEAR_MONTH")
    if not year_month:
        raise ValueError("year_month 또는 YEAR_MONTH가 필요합니다 (YYYY-MM).")

    bronze_dir = event.get("bronze_dir") or os.getenv("BRONZE_DIR", "data/bronze")
    silver_dir = event.get("silver_dir") or os.getenv("SILVER_DIR", "data/silver")

    result = Pipeline(
        EiaGasPriceBronzeExtractor(bronze_dir, year_month),
        EiaGasPriceSilverLoader(silver_dir, year_month),
        EiaGasPriceTransformer(year_month),
    ).run()

    return {
        "row_count": result.write_result.row_count,
        "locations": [result.write_result.location],
        "year_month": year_month,
    }
