"""Gas Price Bronze 월별 배치를 일별 Silver JSON으로 변환합니다."""

import logging
import os

from .extract import extract
from .load import load
from .transform import transform

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def lambda_handler(event: dict | None = None, context=None) -> dict:
    event = event or {}
    collected_month = event.get("collected_month") or os.getenv("COLLECTED_MONTH")
    if not collected_month:
        raise ValueError("collected_month 또는 COLLECTED_MONTH가 필요합니다.")

    bronze_dir = event.get("bronze_dir") or os.getenv("BRONZE_DIR", "data/bronze")
    silver_dir = event.get("silver_dir") or os.getenv("SILVER_DIR", "data/silver")

    bronze_rows = extract(bronze_dir, collected_month)
    silver_rows = transform(bronze_rows)
    paths = load(silver_rows, silver_dir)

    return {
        "collected_month": collected_month,
        "bronze_row_count": len(bronze_rows),
        "silver_row_count": len(silver_rows),
        "paths": paths,
    }
