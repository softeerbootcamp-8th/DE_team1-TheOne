"""AAA Gas Price HTML 원문을 불변 스냅샷으로 저장합니다."""

import logging
from datetime import datetime

from pipeline_core.loader import Loader, WriteResult

from ..common import gas_price_layout as layout

logger = logging.getLogger(__name__)


class GasPriceSnapshotLoader(Loader):
    """HTML 원문을 수집시각 경로에 한 번만 저장합니다."""

    def __init__(self, base_dir: str, collected_at: datetime):
        self._base_dir = base_dir
        self._collected_at = collected_at

    def write(self, data: str) -> WriteResult:
        path = layout.snapshot_file(self._base_dir, self._collected_at)
        path.parent.mkdir(parents=True, exist_ok=True)

        with path.open("x", encoding="utf-8") as snapshot:
            snapshot.write(data)

        logger.info("원문 스냅샷 저장 완료: %s (%d bytes)", path, path.stat().st_size)
        return WriteResult(location=str(path), row_count=1)
