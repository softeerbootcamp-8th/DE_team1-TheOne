"""리스 업체 보유 차량 대장 Bronze 를 Silver 로 변환합니다.

collected_date 를 event 로 받게 되어 있어, 다음 이슈에서 붙일 DAG 가
논리 실행일을 그대로 넘겨주면 됩니다.
"""

import json
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
    silver_rows = transform(bronze_rows)
    if silver_rows[0]["collected_at"].date().isoformat() != collected_date:
        raise ValueError("collected_date와 변환된 수집일이 다릅니다.")
    paths = load(silver_rows, silver_dir)

    return {
        "collected_date": collected_date,
        "bronze_row_count": len(bronze_rows),
        "silver_row_count": len(silver_rows),
        "vendor_count": len(paths),
        "paths": paths,
    }


if __name__ == "__main__":
    print(json.dumps(lambda_handler(), ensure_ascii=False, indent=2))
