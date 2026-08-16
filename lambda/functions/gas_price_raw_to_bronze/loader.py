"""AAA New York 정규 휘발유 평균 가격 원문을 Bronze JSON으로 적재.

Extractor가 수집한 가격과 기준일 문자열을 변환하지 않고 일별 파티션에 씁니다.
"""

import json
import logging
from datetime import datetime

from pipeline_core.loader import Loader, WriteResult

from ..common import gas_price_layout as layout
from ..common.atomic_write import atomic_write
from ..common.s3_loader import S3Loader, S3Object

logger = logging.getLogger(__name__)


class GasPriceBronzeLoader(Loader):
    """수집 결과 한 건을 수집일 파티션의 일별 JSON으로 로컬에 저장합니다."""

    def __init__(self, base_dir: str, collected_at: datetime):
        self._base_dir = base_dir
        self._collected_at = collected_at

    def write(self, data: dict) -> WriteResult:
        path = layout.bronze_file(self._base_dir, f"{self._collected_at:%Y-%m-%d}")
        path.parent.mkdir(parents=True, exist_ok=True)
        body = _to_json_bytes(data, self._collected_at)

        atomic_write(
            path,
            lambda temporary: temporary.write_bytes(body),
        )

        logger.info("적재 완료: %s (%d bytes)", path, len(body))
        return WriteResult(location=str(path), row_count=1)


class GasPriceS3BronzeLoader(Loader):
    """수집 결과 한 건을 수집일 파티션의 일별 JSON으로 S3에 저장합니다."""

    def __init__(self, collected_at: datetime, bucket: str | None = None):
        self._collected_at = collected_at
        self._bucket = bucket

    def write(self, data: dict) -> WriteResult:
        body = _to_json_bytes(data, self._collected_at)
        key = layout.bronze_key(f"{self._collected_at:%Y-%m-%d}")

        result = S3Loader(key=key, bucket=self._bucket).write(
            S3Object(body=body, row_count=1)
        )
        logger.info("적재 완료: %s (%d bytes)", result.location, len(body))
        return result


def _to_json_bytes(data: dict, collected_at: datetime) -> bytes:
    record = {**data, "collected_at": collected_at.isoformat()}
    return (json.dumps(record, ensure_ascii=False) + "\n").encode("utf-8")


def build_bronze_loader(
    storage: str, base_dir: str, collected_at: datetime, bucket: str | None = None
) -> Loader:
    """storage 파라미터로 로컬/S3 Loader 중 하나를 고릅니다."""
    if storage == "local":
        return GasPriceBronzeLoader(base_dir, collected_at)
    if storage == "s3":
        return GasPriceS3BronzeLoader(collected_at, bucket=bucket)
    raise ValueError(f"알 수 없는 storage: {storage!r} (local 또는 s3)")
