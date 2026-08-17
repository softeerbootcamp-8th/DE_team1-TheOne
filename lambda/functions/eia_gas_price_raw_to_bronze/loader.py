"""EIA 원본 파일을 수집일 파티션에 그대로 적재합니다."""

import logging
from datetime import date

from pipeline_core.loader import Loader, WriteResult

from ..common import eia_fuel_price_layout as layout
from ..common.atomic_write import atomic_write
from ..common.s3_loader import S3Loader, S3Object

logger = logging.getLogger(__name__)


class EiaGasPriceBronzeLoader(Loader):
    """받은 bytes 를 변형 없이 로컬에 씁니다."""

    def __init__(self, base_dir: str, collected_date: date):
        self._base_dir = base_dir
        self._collected_date = collected_date

    def write(self, data: dict) -> WriteResult:
        path = layout.gas_bronze_file(self._base_dir, self._collected_date)
        path.parent.mkdir(parents=True, exist_ok=True)
        body = data["body"]

        atomic_write(path, lambda temporary: temporary.write_bytes(body))

        logger.info("적재 완료: %s (%d bytes)", path, len(body))
        # 이력 파일이라 "행 수"가 대상 월과 무관합니다. 파일 1건으로 셉니다.
        return WriteResult(location=str(path), row_count=1)


class EiaGasPriceS3BronzeLoader(Loader):
    """받은 bytes 를 변형 없이 S3 에 씁니다."""

    def __init__(self, collected_date: date, bucket: str | None = None):
        self._collected_date = collected_date
        self._bucket = bucket

    def write(self, data: dict) -> WriteResult:
        body = data["body"]
        key = layout.gas_bronze_key(self._collected_date)

        result = S3Loader(key=key, bucket=self._bucket).write(
            S3Object(body=body, row_count=1)
        )
        logger.info("적재 완료: %s (%d bytes)", result.location, len(body))
        return result


def build_bronze_loader(
    storage: str, base_dir: str, collected_date: date, bucket: str | None = None
) -> Loader:
    if storage == "local":
        return EiaGasPriceBronzeLoader(base_dir, collected_date)
    if storage == "s3":
        return EiaGasPriceS3BronzeLoader(collected_date, bucket=bucket)
    raise ValueError(f"알 수 없는 storage: {storage!r} (local 또는 s3)")
