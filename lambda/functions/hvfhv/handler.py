"""NYC FHVHV Trip Record 수집/적재 Lambda 핸들러.

extract(수집) 모듈과 load(적재) 모듈을 조율합니다.
"""

import json
import logging
import os
from datetime import datetime, timezone

from .extract import extract
from .load import load

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def lambda_handler(event: dict | None = None, context=None) -> dict:
    event = event or {}
    
    year_str = event.get("year") or os.getenv("YEAR")
    month_str = event.get("month") or os.getenv("MONTH")
    
    if not year_str or not month_str:
        raise ValueError("year와 month 파라미터가 누락되었습니다.")
        
    base_dir = event.get("base_dir") or os.getenv("BRONZE_DIR", "data/bronze")
    collected_at = datetime.now(timezone.utc)

    logger.info("ETL 작업 개시: 대상 연월=%s-%s, 출력 폴더=%s", year_str, month_str, base_dir)

    content = extract(year_str, month_str)
    path = load(content, base_dir, collected_at)

    result = {
        "status": "success",
        "year": year_str,
        "month": month_str,
        "file_size_bytes": len(content),
        "path": path,
    }
    
    logger.info("ETL 작업 종료: %s", json.dumps(result, ensure_ascii=False))
    return result


if __name__ == "__main__":
    test_event = {
        "year": os.getenv("YEAR"),
        "month": os.getenv("MONTH"),
    }
    print(json.dumps(lambda_handler(test_event), ensure_ascii=False, indent=2))

