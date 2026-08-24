"""EIA 휘발유 Bronze 를 대상 월의 일별 단가 Silver 로 변환합니다."""

import os

from pipeline_core.pipeline import Pipeline
from pipeline_core.transformer import Transformer

from main.aws_lambda.common import eia_fuel_price_layout as layout
from shared.aws_lambda.common.logging_setup import configure_lambda_logging
from .extractor import build_bronze_extractor
from .loader import build_silver_loader
from .transformer import build_daily_prices

configure_lambda_logging()


class EiaGasPriceTransformer(Transformer):
    def __init__(self, year_month: str):
        self._year_month = year_month

    def transform(self, data: dict) -> dict:
        return {
            "rows": build_daily_prices(
                self._year_month, data["gas_body"], data["bronze_collected_date"]
            ),
            "source_collected_at": data["source_collected_at"],
        }


def lambda_handler(event: dict | None = None, context=None) -> dict:
    event = event or {}
    year_month = event.get("year_month") or os.getenv("YEAR_MONTH")
    if not year_month:
        raise ValueError("year_month 또는 YEAR_MONTH가 필요합니다 (YYYY-MM).")

    storage = event.get("storage") or os.getenv("BRONZE_STORAGE", "local")
    bucket = event.get("bucket") or os.getenv("DATA_LAKE_S3_BUCKET")
    bronze_dir = event.get("bronze_dir") or os.getenv("BRONZE_DIR", "data/bronze")
    silver_dir = event.get("silver_dir") or os.getenv("SILVER_DIR", "data/silver")
    service_area = event.get("service_area") or os.getenv("SERVICE_AREA", "NYC")

    result = Pipeline(
        build_bronze_extractor(storage, bronze_dir, bucket, year_month, service_area),
        build_silver_loader(
            storage,
            silver_dir,
            bucket,
            year_month,
            service_area,
        ),
        EiaGasPriceTransformer(year_month),
    ).run()

    source_collected_at = layout.silver_source_collected_at(
        result.write_result.location
    )
    return {
        "row_count": result.write_result.row_count,
        "locations": [result.write_result.location],
        "year_month": year_month,
        "source_collected_at": source_collected_at,
    }
