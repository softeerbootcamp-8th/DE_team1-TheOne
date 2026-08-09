"""AAA New York 정규 휘발유 평균 가격 Bronze JSON 적재.

Extractor가 만든 일별 행을 수집일 기준 Hive 파티션에 씁니다.
"""

import json
import logging
from datetime import datetime
from pathlib import Path

from pipeline_core.loader import Loader, WriteResult

logger = logging.getLogger(__name__)

DATASET = "oil"


class GasPriceBronzeLoader(Loader):
    """수집 결과 한 건을 수집일 파티션의 일별 JSON으로 저장합니다."""

    def __init__(self, base_dir: str, collected_at: datetime):
        self._base_dir = base_dir
        self._collected_at = collected_at

    # 수집일로 나눈 Hive 파티션 경로를 만듭니다.
    def partition_path(self) -> Path:
        return (
            Path(self._base_dir)
            / DATASET
            / f"collected_date={self._collected_at:%Y-%m-%d}"
        )

    def write(self, data: dict) -> WriteResult:
        partition = self.partition_path()
        partition.mkdir(parents=True, exist_ok=True)
        # 파일 이름이 곧 가격 기준일입니다 (핸들러가 이 이름에서 price_date를 읽습니다).
        path = partition / f"{data['price_date']:%Y-%m-%d}.json"

        record = {
            "state": data["state"],
            "fuel_type": data["fuel_type"],
            "price_usd_per_gallon": data["price_usd_per_gallon"],
            "price_date": data["price_date"].isoformat(),
            "source_url": data["source_url"],
            "collected_at": self._collected_at.isoformat(),
        }

        temporary_path = path.with_suffix(".tmp")
        temporary_path.write_text(
            json.dumps(record, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        temporary_path.replace(path)

        logger.info("적재 완료: %s (%d bytes)", path, path.stat().st_size)
        return WriteResult(location=str(path), row_count=1)
