"""NLR 전기차 충전소 수집과 Bronze 적재를 실행합니다."""

import logging
import os
from datetime import datetime, timezone

from .extract import extract
from .load import load

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def lambda_handler(event: dict | None = None, context=None) -> dict:
    event = event or {}
    api_key = os.getenv("NLR_API_KEY", "").strip()
    if not api_key:
        raise ValueError("NLR_API_KEY 환경변수가 필요합니다.")

    base_dir = event.get("base_dir") or os.getenv("OUTPUT_DIR", "data/bronze")
    collected_at = datetime.now(timezone.utc)

    rows = extract(api_key, collected_at)
    path = load(rows, base_dir, collected_at)

    return {
        "state": "NY",
        "fuel_type_code": "ELEC",
        "row_count": len(rows),
        "priced_count": sum(bool((row["ev_pricing"] or "").strip()) for row in rows),
        "path": path,
    }
