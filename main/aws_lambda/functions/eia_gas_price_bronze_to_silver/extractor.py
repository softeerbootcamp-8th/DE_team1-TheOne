"""EIA 휘발유 요금 Bronze 원본을 읽습니다.

**가장 최근 수집분**을 씁니다. 파일 하나에 2000년부터의 주간 이력이 통째로 들어 있어
어느 수집분이든 여러 달을 담고 있고, 새 파일일수록 최근 주가 더 채워져 있습니다.
"""

import logging
from datetime import date

from pipeline_core.extractor import Extractor

from main.aws_lambda.common import eia_fuel_price_layout as layout
from shared.common.s3_reader import get_object_bytes, list_keys

logger = logging.getLogger(__name__)


class EiaGasPriceBronzeExtractor(Extractor):
    """휘발유 원본 bytes 를 로컬에서 읽습니다."""

    def __init__(self, base_dir: str, year_month: str, service_area: str):
        self._base_dir = base_dir
        self._year_month = year_month
        self._service_area = service_area
        self.name = f"eia_gas_price_bronze:{base_dir}:{year_month}"

    def extract(self) -> dict:
        collected_at, partition = layout.newest_bronze_partition(
            self._base_dir, layout.GAS_DATASET, self._service_area
        )
        path = partition / layout.GAS_FILE_NAME
        if not path.is_file():
            raise FileNotFoundError(f"EIA 휘발유 Bronze 파일이 없습니다: {path}")
        body = path.read_bytes()
        if not body:
            raise ValueError(f"EIA 휘발유 Bronze 파일이 비어 있습니다: {path}")

        logger.info("bronze_extract done path=%s bytes=%d", path, len(body))
        return {
            "gas_body": body,
            "source_collected_at": collected_at,
            "bronze_collected_date": date.fromisoformat(collected_at[:10]),
        }


class EiaGasPriceS3BronzeExtractor(Extractor):
    """휘발유 원본 bytes 를 S3 에서 읽습니다."""

    def __init__(self, bucket: str, year_month: str, service_area: str):
        self._bucket = bucket
        self._year_month = year_month
        self._service_area = service_area
        self.name = f"eia_gas_price_bronze_s3:{bucket}:{year_month}"

    def extract(self) -> dict:
        prefix = layout.bronze_s3_prefix(layout.GAS_DATASET, self._service_area)
        keys = list_keys(self._bucket, prefix)
        collected_at, key = layout.newest_bronze_s3_key(
            keys, layout.GAS_DATASET, layout.GAS_FILE_NAME, self._service_area
        )
        body = get_object_bytes(self._bucket, key)
        if not body:
            raise ValueError(f"EIA 휘발유 Bronze 객체가 비어 있습니다: s3://{self._bucket}/{key}")

        logger.info("bronze_extract done key=%s bytes=%d", key, len(body))
        return {
            "gas_body": body,
            "source_collected_at": collected_at,
            "bronze_collected_date": date.fromisoformat(collected_at[:10]),
        }


def build_bronze_extractor(
    storage: str,
    base_dir: str,
    bucket: str | None,
    year_month: str,
    service_area: str,
) -> Extractor:
    if storage == "local":
        return EiaGasPriceBronzeExtractor(base_dir, year_month, service_area)
    if storage == "s3":
        return EiaGasPriceS3BronzeExtractor(bucket, year_month, service_area)
    raise ValueError(f"알 수 없는 storage: {storage!r} (local 또는 s3)")
