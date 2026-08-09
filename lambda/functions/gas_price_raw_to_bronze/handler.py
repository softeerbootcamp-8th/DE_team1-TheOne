"""AAA New York 휘발유 가격 Raw 수집과 Bronze 적재를 실행합니다."""

import logging
import os
from datetime import datetime, timezone

from .extract import extract
from .load import load

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def lambda_handler(event: dict | None = None, context=None) -> dict:
    event = event or {}
    base_dir = event.get("base_dir") or os.getenv("OUTPUT_DIR", "data/bronze")
    collected_at = datetime.now(timezone.utc)

    row = extract()
    path = load(row, base_dir, collected_at)

    return {
        "state": row["state"],
        "fuel_type": row["fuel_type"],
        "price_date": row["price_date"].isoformat(),
        "row_count": 1,
        "path": path,
    }
