"""EIA 전력 Bronze 를 대상 월의 일별 충전 단가 Silver 로 변환합니다."""

import os

from pipeline_core.pipeline import Pipeline
from pipeline_core.transformer import Transformer

from shared.aws_lambda.common.logging_setup import configure_lambda_logging
from .extractor import build_bronze_extractor
from .loader import build_silver_loader
from .transformer import PUBLIC_CHARGING_MARKUP, build_daily_prices

configure_lambda_logging()


class EiaElectricityPriceTransformer(Transformer):
    def __init__(self, year_month: str, markup: float):
        self._year_month = year_month
        self._markup = markup

    def transform(self, data: dict) -> list[dict]:
        return build_daily_prices(
            self._year_month,
            data["electricity_body"],
            data["bronze_collected_date"],
            markup=self._markup,
        )


def lambda_handler(event: dict | None = None, context=None) -> dict:
    event = event or {}
    dry_run = event.get("dry_run", False)
    if not isinstance(dry_run, bool):
        raise ValueError("dry_run은 boolean이어야 합니다.")
    year_month = event.get("year_month") or os.getenv("YEAR_MONTH")
    if not year_month:
        raise ValueError("year_month 또는 YEAR_MONTH가 필요합니다 (YYYY-MM).")

    storage = event.get("storage") or os.getenv("BRONZE_STORAGE", "local")
    bucket = event.get("bucket") or os.getenv("DATA_LAKE_S3_BUCKET")
    bronze_dir = event.get("bronze_dir") or os.getenv("BRONZE_DIR", "data/bronze")
    silver_dir = event.get("silver_dir") or os.getenv("SILVER_DIR", "data/silver")
    markup = float(event.get("markup") or os.getenv("MARKUP") or PUBLIC_CHARGING_MARKUP)

    result = Pipeline(
        build_bronze_extractor(storage, bronze_dir, bucket, year_month),
        build_silver_loader(
            storage,
            silver_dir,
            bucket,
            year_month,
            dry_run=dry_run,
        ),
        EiaElectricityPriceTransformer(year_month, markup),
    ).run()

    response = {
        "row_count": result.write_result.row_count,
        "locations": [result.write_result.location],
        "year_month": year_month,
        "markup": markup,
    }
    if dry_run:
        response["dry_run"] = True
    return response
