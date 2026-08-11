"""NLR API 응답 원문을 로컬 Bronze JSON으로 적재합니다."""

import logging
from datetime import datetime

from pipeline_core.loader import Loader, WriteResult

from ..common import ev_charging_layout as layout

logger = logging.getLogger(__name__)


class EvChargingBronzeLoader(Loader):
    """API 응답 bytes를 변환 없이 JSON 파일 하나로 저장합니다."""

    def __init__(self, base_dir: str, collected_at: datetime):
        self._base_dir = base_dir
        self._collected_at = collected_at

    def write(self, data: bytes) -> WriteResult:
        if not isinstance(data, bytes) or not data:
            raise ValueError("적재할 NLR API 원문 bytes가 없습니다.")

        path = layout.bronze_file(self._base_dir, self._collected_at)
        path.parent.mkdir(parents=True, exist_ok=True)

        temporary_path = path.with_suffix(".tmp")
        temporary_path.write_bytes(data)
        temporary_path.replace(path)

        logger.info(
            "bronze_load done path=%s files=1 bytes=%d",
            path,
            path.stat().st_size,
        )
        return WriteResult(location=str(path), row_count=1)
