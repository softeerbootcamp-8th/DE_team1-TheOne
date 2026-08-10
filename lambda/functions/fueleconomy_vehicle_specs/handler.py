"""fueleconomy.gov 차종별 제원 수집/적재 Lambda 핸들러.

extract(수집) 와 load(적재) 를 이어붙이기만 합니다.
1년에 한 번 도는 것을 전제로, 매번 전량 스냅샷을 새 파티션에 씁니다.
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
    base_dir = event.get("base_dir") or os.getenv("BRONZE_DIR", "data/bronze")
    collected_at = datetime.now(timezone.utc)

    rows = extract(collected_at)
    path = load(rows, base_dir, collected_at)

    return {"row_count": len(rows), "path": path}


if __name__ == "__main__":
    print(json.dumps(lambda_handler(), ensure_ascii=False, indent=2))
