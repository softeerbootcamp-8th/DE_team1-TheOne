"""휘발유·전력 CLEAN Silver 두 개를 대상 월의 통합 연료비 Silver 로 붙입니다."""

import os
from pathlib import PurePosixPath

from pipeline_core.pipeline import Pipeline
from pipeline_core.transformer import Transformer

from shared.aws_lambda.common.logging_setup import configure_lambda_logging
from main.common.eia_fuel_version import fuel_source_tokens
from .extractor import build_clean_extractor
from .loader import build_silver_loader
from .transformer import combine_daily_prices

configure_lambda_logging()


class EiaFuelPriceCombineTransformer(Transformer):
    def __init__(self, year_month: str):
        self._year_month = year_month

    def transform(self, data: dict) -> dict:
        return {
            "rows": combine_daily_prices(
                self._year_month, data["gas_rows"], data["electricity_rows"]
            ),
            "gas_source_collected_at": data["gas_source_collected_at"],
            "ev_source_collected_at": data["ev_source_collected_at"],
        }


def lambda_handler(event: dict | None = None, context=None) -> dict:
    event = event or {}
    year_month = event.get("year_month") or os.getenv("YEAR_MONTH")
    if not year_month:
        raise ValueError("year_month 또는 YEAR_MONTH가 필요합니다 (YYYY-MM).")

    storage = event.get("storage") or os.getenv("BRONZE_STORAGE", "local")
    bucket = event.get("bucket") or os.getenv("DATA_LAKE_S3_BUCKET")
    silver_dir = event.get("silver_dir") or os.getenv("SILVER_DIR", "data/silver")
    service_area = event.get("service_area") or os.getenv("SERVICE_AREA", "NYC")
    result = Pipeline(
        build_clean_extractor(storage, silver_dir, bucket, year_month, service_area),
        build_silver_loader(
            storage,
            silver_dir,
            bucket,
            year_month,
            service_area,
        ),
        EiaFuelPriceCombineTransformer(year_month),
    ).run()

    input_version = PurePosixPath(
        result.write_result.location.split("://", 1)[-1]
    ).parent.name
    source_tokens = fuel_source_tokens(input_version)
    if source_tokens is None:
        raise ValueError(f"Fuel Silver 입력 버전 경로가 올바르지 않습니다: {input_version}")
    gas_source, ev_source = source_tokens
    return {
        "row_count": result.write_result.row_count,
        "locations": [result.write_result.location],
        "year_month": year_month,
        "input_version": input_version,
        "gas_source_collected_at": gas_source,
        "ev_source_collected_at": ev_source,
    }
