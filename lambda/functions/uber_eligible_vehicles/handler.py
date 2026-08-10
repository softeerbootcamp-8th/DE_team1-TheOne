"""Uber Eligible Vehicles 수집/적재 Lambda 핸들러.

extract(수집) 와 load(적재) 를 이어붙이기만 합니다.
"""

import json
import logging
import os
from datetime import datetime, timezone

from .extract import extract
from .load import load

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def lambda_handler(event: dict | None = None, context=None) -> dict:
    event = event or {}
    city_slug = event.get("city_slug") or os.getenv("CITY_SLUG", "new-york")
    base_dir = event.get("base_dir") or os.getenv("BRONZE_DIR", "data/bronze")
    collected_at = datetime.now(timezone.utc)

    rows = extract(city_slug, collected_at)
    path = load(rows, base_dir, collected_at)

    return {"city_slug": city_slug, "row_count": len(rows), "path": path}


if __name__ == "__main__":
    print(json.dumps(lambda_handler(), ensure_ascii=False, indent=2))
