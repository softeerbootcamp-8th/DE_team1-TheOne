"""NLR API 응답 원문을 로컬 Bronze JSON으로 적재합니다."""

import logging
from datetime import datetime

from pipeline_core.loader import Loader, WriteResult

from ..common import ev_charging_layout as layout
from ..common.atomic_write import atomic_write
from ..common.s3_loader import S3Loader, S3Object

logger = logging.getLogger(__name__)


class EvChargingBronzeLoader(Loader):
    """API 응답 bytes를 변환 없이 JSON 파일 하나로 로컬에 저장합니다."""

    def __init__(self, base_dir: str, collected_at: datetime):
        self._base_dir = base_dir
        self._collected_at = collected_at

    def write(self, data: bytes) -> WriteResult:
        body = _validate_bytes(data)

        path = layout.bronze_file(self._base_dir, self._collected_at)
        path.parent.mkdir(parents=True, exist_ok=True)

        atomic_write(path, lambda temporary: temporary.write_bytes(body))

        logger.info(
            "bronze_load done path=%s files=1 bytes=%d",
            path,
            path.stat().st_size,
        )
        return WriteResult(location=str(path), row_count=1)


class EvChargingS3BronzeLoader(Loader):
    """API 응답 bytes를 변환 없이 JSON 파일 하나로 S3에 저장합니다."""

    def __init__(self, collected_at: datetime, bucket: str | None = None):
        self._collected_at = collected_at
        self._bucket = bucket

    def write(self, data: bytes) -> WriteResult:
        body = _validate_bytes(data)
        key = layout.bronze_key(self._collected_at)

        result = S3Loader(key=key, bucket=self._bucket).write(
            S3Object(body=body, row_count=1)
        )
        logger.info("bronze_load done location=%s bytes=%d", result.location, len(body))
        return result


def _validate_bytes(data: bytes) -> bytes:
    if not isinstance(data, bytes) or not data:
        raise ValueError("적재할 NLR API 원문 bytes가 없습니다.")
    return data


def build_bronze_loader(
    storage: str, base_dir: str, collected_at: datetime, bucket: str | None = None
) -> Loader:
    """storage 파라미터로 로컬/S3 Loader 중 하나를 고릅니다."""
    if storage == "local":
        return EvChargingBronzeLoader(base_dir, collected_at)
    if storage == "s3":
        return EvChargingS3BronzeLoader(collected_at, bucket=bucket)
    raise ValueError(f"알 수 없는 storage: {storage!r} (local 또는 s3)")
