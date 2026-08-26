"""EIA 전력 Bronze 를 대상 월의 일별 충전 단가 Silver 로 변환합니다."""

import os

from pipeline_core.pipeline import Pipeline
from pipeline_core.transformer import Transformer

from main.aws_lambda.common import eia_fuel_price_layout as layout
from shared.aws_lambda.common.logging_setup import configure_lambda_logging
from shared.aws_lambda.common.storage_config import resolve_storage
from .extractor import build_bronze_extractor
from .loader import build_silver_loader
from .transformer import PUBLIC_CHARGING_MARKUP, build_daily_prices

configure_lambda_logging()


class EiaElectricityPriceTransformer(Transformer):
    def __init__(self, year_month: str, markup: float, service_area: str):
        self._year_month = year_month
        self._markup = markup
        self._service_area = service_area

    def transform(self, data: dict) -> dict:
        return {
            "rows": build_daily_prices(
                self._year_month,
                data["electricity_body"],
                data["bronze_collected_date"],
                markup=self._markup,
                service_area=self._service_area,
            ),
            "source_collected_at": data["source_collected_at"],
        }


def lambda_handler(event: dict | None = None, context=None) -> dict:
    event = event or {}
    year_month = event.get("year_month") or os.getenv("YEAR_MONTH")
    if not year_month:
        raise ValueError("year_month 또는 YEAR_MONTH가 필요합니다 (YYYY-MM).")

    storage = resolve_storage(event)
    bucket = event.get("bucket") or os.getenv("DATA_LAKE_S3_BUCKET")
    bronze_dir = event.get("bronze_dir") or os.getenv("BRONZE_DIR", "data/bronze")
    silver_dir = event.get("silver_dir") or os.getenv("SILVER_DIR", "data/silver")
    markup = float(event.get("markup") or os.getenv("MARKUP") or PUBLIC_CHARGING_MARKUP)
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
        EiaElectricityPriceTransformer(year_month, markup, service_area),
    ).run()

    source_collected_at = layout.silver_source_collected_at(
        result.write_result.location
    )
    return {
        "row_count": result.write_result.row_count,
        "locations": [result.write_result.location],
        "year_month": year_month,
        "markup": markup,
        "source_collected_at": source_collected_at,
    }
