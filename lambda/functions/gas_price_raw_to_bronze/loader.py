"""AAA New York 정규 휘발유 평균 가격 원문을 Bronze JSON으로 적재.

Extractor가 수집한 가격과 기준일 문자열을 변환하지 않고 일별 파티션에 씁니다.
"""

import json
import logging
from datetime import datetime

from pipeline_core.loader import Loader, WriteResult

from ..common import gas_price_layout as layout

logger = logging.getLogger(__name__)


class GasPriceBronzeLoader(Loader):
    """수집 결과 한 건을 수집일 파티션의 일별 JSON으로 저장합니다."""

    def __init__(self, base_dir: str, collected_at: datetime):
        self._base_dir = base_dir
        self._collected_at = collected_at

    def write(self, data: dict) -> WriteResult:
        path = layout.bronze_file(self._base_dir, f"{self._collected_at:%Y-%m-%d}")
        path.parent.mkdir(parents=True, exist_ok=True)

        record = {
            **data,
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
