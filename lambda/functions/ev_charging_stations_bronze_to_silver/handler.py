"""전기차 충전소 Bronze를 뉴욕시 일별 평균 Silver로 변환합니다."""

import os

from pipeline_core.pipeline import Pipeline

from ..common.logging_setup import configure_lambda_logging
from .extractor import EvChargingBronzeExtractor
from .loader import COUNT_FIELDS, EvChargingSilverLoader
from .transformer import EvChargingSilverTransformer

configure_lambda_logging()


def lambda_handler(event: dict | None = None, context=None) -> dict:
    event = event or {}
    collected_date = event.get("collected_date") or os.getenv("COLLECTED_DATE")
    if not collected_date:
        raise ValueError("collected_date 또는 COLLECTED_DATE가 필요합니다.")

    bronze_dir = event.get("bronze_dir") or os.getenv("BRONZE_DIR", "data/bronze")
    silver_dir = event.get("silver_dir") or os.getenv("SILVER_DIR", "data/silver")

    loader = EvChargingSilverLoader(silver_dir, expect_price_date=collected_date)
    result = Pipeline(
        EvChargingBronzeExtractor(bronze_dir, collected_date),
        loader,
        transformer=EvChargingSilverTransformer(),
    ).run()

    row = loader.written_row or {}
    return {
        # 0이면 기존 Silver JSON이 더 최신이라 다시 쓰지 않았다는 뜻입니다.
        "row_count": result.write_result.row_count,
        "locations": [result.write_result.location],
        "collected_date": collected_date,
        "average_price_usd_per_kwh": row.get("average_price_usd_per_kwh"),
        **{field: row.get(field) for field in COUNT_FIELDS},
    }
