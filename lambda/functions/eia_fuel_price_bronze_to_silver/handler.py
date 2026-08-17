"""EIA Bronze 두 개를 대상 월의 통합 연료비 Silver 로 변환합니다."""

import os

from pipeline_core.pipeline import Pipeline
from pipeline_core.transformer import Transformer

from ..common.logging_setup import configure_lambda_logging
from .extractor import EiaFuelPriceBronzeExtractor
from .loader import EiaFuelPriceSilverLoader
from .transformer import PUBLIC_CHARGING_MARKUP, build_daily_prices

configure_lambda_logging()


class EiaFuelPriceTransformer(Transformer):
    def __init__(self, year_month: str, markup: float):
        self._year_month = year_month
        self._markup = markup

    def transform(self, data: dict) -> list[dict]:
        return build_daily_prices(
            self._year_month,
            data["gas_body"],
            data["electricity_body"],
            data["bronze_collected_date"],
            markup=self._markup,
        )


def lambda_handler(event: dict | None = None, context=None) -> dict:
    event = event or {}
    year_month = event.get("year_month") or os.getenv("YEAR_MONTH")
    if not year_month:
        raise ValueError("year_month 또는 YEAR_MONTH가 필요합니다 (YYYY-MM).")

    bronze_dir = event.get("bronze_dir") or os.getenv("BRONZE_DIR", "data/bronze")
    silver_dir = event.get("silver_dir") or os.getenv("SILVER_DIR", "data/silver")
    markup = float(event.get("markup") or os.getenv("MARKUP") or PUBLIC_CHARGING_MARKUP)

    result = Pipeline(
        EiaFuelPriceBronzeExtractor(bronze_dir, year_month),
        EiaFuelPriceSilverLoader(silver_dir, year_month),
        EiaFuelPriceTransformer(year_month, markup),
    ).run()

    return {
        "row_count": result.write_result.row_count,
        "locations": [result.write_result.location],
        "year_month": year_month,
        "markup": markup,
    }
