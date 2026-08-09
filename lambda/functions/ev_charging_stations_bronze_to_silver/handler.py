"""전기차 충전소 Bronze를 뉴욕시 일별 평균 Silver로 변환합니다."""

import logging
import os

from .extract import extract
from .load import load
from .transform import transform

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def lambda_handler(event: dict | None = None, context=None) -> dict:
    event = event or {}
    collected_date = event.get("collected_date") or os.getenv("COLLECTED_DATE")
    if not collected_date:
        raise ValueError("collected_date 또는 COLLECTED_DATE가 필요합니다.")

    bronze_dir = event.get("bronze_dir") or os.getenv("BRONZE_DIR", "data/bronze")
    silver_dir = event.get("silver_dir") or os.getenv("SILVER_DIR", "data/silver")

    bronze_rows = extract(bronze_dir, collected_date)
    silver_row = transform(bronze_rows)
    if silver_row["price_date"].isoformat() != collected_date:
        raise ValueError("collected_date와 변환된 price_date가 다릅니다.")
    path = load(silver_row, silver_dir)

    return {
        "collected_date": collected_date,
        "bronze_row_count": len(bronze_rows),
        "silver_row_count": 1,
        "nyc_station_count": silver_row["nyc_station_count"],
        "normalized_price_count": silver_row["normalized_price_count"],
        "free_station_count": silver_row["free_station_count"],
        "missing_price_count": silver_row["missing_price_count"],
        "unsupported_price_count": silver_row["unsupported_price_count"],
        "average_price_usd_per_kwh": silver_row["average_price_usd_per_kwh"],
        "path": path,
    }
