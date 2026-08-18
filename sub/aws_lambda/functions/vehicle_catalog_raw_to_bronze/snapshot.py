"""차량 대장 HTML·카드 이미지 원문을 불변 스냅샷으로 저장합니다."""

import logging
from datetime import datetime

from pipeline_core.loader import Loader, WriteResult

from shared.aws_lambda.common import vehicle_catalog_layout as layout

logger = logging.getLogger(__name__)


class VehicleCatalogHtmlSnapshotLoader(Loader):
    def __init__(self, base_dir: str, collected_at: datetime):
        self._base_dir = base_dir
        self._collected_at = collected_at

    def write(self, data: str) -> WriteResult:
        path = layout.html_snapshot_file(self._base_dir, self._collected_at)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("x", encoding="utf-8") as snapshot:
            snapshot.write(data)
        logger.info("HTML 원문 저장 완료: %s (%d bytes)", path, path.stat().st_size)
        return WriteResult(location=str(path), row_count=1)


class VehicleCatalogImageSnapshotLoader(Loader):
    def __init__(self, base_dir: str, collected_at: datetime, source_url: str):
        self._base_dir = base_dir
        self._collected_at = collected_at
        self._source_url = source_url

    def write(self, data: bytes) -> WriteResult:
        path = layout.image_snapshot_file(
            self._base_dir, self._collected_at, self._source_url
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("xb") as snapshot:
            snapshot.write(data)
        logger.info("이미지 원문 저장 완료: %s (%d bytes)", path, path.stat().st_size)
        return WriteResult(location=str(path), row_count=1)
