"""AAA New York 정규 휘발유 평균 가격 적재(load).

extract가 만든 일별 행을 월별 폴더의 JSON 파일로 씁니다.
"""

import json
import logging
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

DATASET = "oil"

# 수집 연월로 나눈 Hive 파티션 경로를 만듭니다.
def partition_path(base_dir: str, collected_at: datetime) -> Path:
    return Path(base_dir) / DATASET / f"collected_month={collected_at:%Y-%m}"

# 수집 결과 한 건을 일별 JSON으로 저장하고 파일 경로를 반환합니다.
def load(row: dict, base_dir: str, collected_at: datetime) -> str:
    partition = partition_path(base_dir, collected_at)
    partition.mkdir(parents=True, exist_ok=True)
    path = partition / f"{row['price_date']:%Y-%m-%d}.json"

    record = {
        "state": row["state"],
        "fuel_type": row["fuel_type"],
        "price_usd_per_gallon": row["price_usd_per_gallon"],
        "price_date": row["price_date"].isoformat(),
        "source_url": row["source_url"],
        "collected_at": collected_at.isoformat(),
    }

    temporary_path = path.with_suffix(".tmp")
    temporary_path.write_text(
        json.dumps(record, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    temporary_path.replace(path)

    logger.info("적재 완료: %s (%d bytes)", path, path.stat().st_size)
    return str(path)
