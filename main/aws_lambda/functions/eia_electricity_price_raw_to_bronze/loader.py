"""EIA 원본 파일을 수집일 파티션에 그대로 적재합니다."""

import logging
from datetime import date

from pipeline_core.loader import Loader, WriteResult

from main.aws_lambda.common import eia_fuel_price_layout as layout
from shared.aws_lambda.common.atomic_write import atomic_write
from shared.aws_lambda.common.s3_loader import S3Loader, S3Object

logger = logging.getLogger(__name__)


class EiaElectricityPriceBronzeLoader(Loader):
    """받은 bytes 를 변형 없이 로컬에 씁니다."""

    def __init__(self, base_dir: str, collected_date: date):
        self._base_dir = base_dir
        self._collected_date = collected_date

    def write(self, data: dict) -> WriteResult:
        body = data["body"]

        # 전력은 3개월에 한 번만 실제로 갱신되므로 월 1회 수집분 대부분이 바이트까지
        # 같습니다. 같은 것을 새 파티션으로 쌓지 않습니다.
        duplicate = layout.is_duplicate_of_newest(
            self._base_dir, layout.ELECTRICITY_DATASET, layout.ELECTRICITY_FILE_NAME, body
        )
        if duplicate is not None:
            logger.info("최신 수집분과 동일해 건너뜁니다: %s (%d bytes)", duplicate, len(body))
            return WriteResult(location=str(duplicate), row_count=1)

        path = layout.electricity_bronze_file(self._base_dir, self._collected_date)
        path.parent.mkdir(parents=True, exist_ok=True)
        atomic_write(path, lambda temporary: temporary.write_bytes(body))

        logger.info("적재 완료: %s (%d bytes)", path, len(body))
        # 이력 파일이라 "행 수"가 대상 월과 무관합니다. 파일 1건으로 셉니다.
        return WriteResult(location=str(path), row_count=1)


class EiaElectricityPriceS3BronzeLoader(Loader):
    """받은 bytes 를 변형 없이 S3 에 씁니다."""

    def __init__(self, collected_date: date, bucket: str | None = None):
        self._collected_date = collected_date
        self._bucket = bucket

    def write(self, data: dict) -> WriteResult:
        body = data["body"]
        key = layout.electricity_bronze_key(self._collected_date)

        result = S3Loader(key=key, bucket=self._bucket).write(
            S3Object(body=body, row_count=1)
        )
        logger.info("적재 완료: %s (%d bytes)", result.location, len(body))
        return result


def build_bronze_loader(
    storage: str, base_dir: str, collected_date: date, bucket: str | None = None
) -> Loader:
    if storage == "local":
        return EiaElectricityPriceBronzeLoader(base_dir, collected_date)
    if storage == "s3":
        return EiaElectricityPriceS3BronzeLoader(collected_date, bucket=bucket)
    raise ValueError(f"알 수 없는 storage: {storage!r} (local 또는 s3)")
