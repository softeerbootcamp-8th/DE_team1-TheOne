"""EIA 전력요금 Bronze 원본을 읽습니다.

**가장 최근 수집분**을 씁니다. 대상 월 이하로 제한하지 않는 이유가 둘 있습니다.

전력 통계는 약 3개월 늦게 공개됩니다. M월 값은 M+3월쯤 받은 파일에만 들어 있어서,
"대상 월 이하 최신" 으로 고르면 구조적으로 그 달이 없는 파일을 집습니다.

최신을 쓰면 값도 더 정확합니다. EIA 는 최근 약 17개월을 `Preliminary` 로 두고 나중에
`Final` 로 확정하므로, 새 파일일수록 확정분이 많습니다.
"""

import logging

from pipeline_core.extractor import Extractor

from main.aws_lambda.common import eia_fuel_price_layout as layout
from shared.common.s3_reader import get_object_bytes, list_keys

logger = logging.getLogger(__name__)


class EiaElectricityPriceBronzeExtractor(Extractor):
    """전력요금 원본 bytes 를 로컬에서 읽습니다."""

    def __init__(self, base_dir: str, year_month: str):
        self._base_dir = base_dir
        self._year_month = year_month
        self.name = f"eia_electricity_price_bronze:{base_dir}:{year_month}"

    def extract(self) -> dict:
        collected_date, partition = layout.newest_bronze_partition(
            self._base_dir, layout.ELECTRICITY_DATASET
        )
        path = partition / layout.ELECTRICITY_FILE_NAME
        if not path.is_file():
            raise FileNotFoundError(f"EIA 전력 Bronze 파일이 없습니다: {path}")
        body = path.read_bytes()
        if not body:
            raise ValueError(f"EIA 전력 Bronze 파일이 비어 있습니다: {path}")

        logger.info("bronze_extract done path=%s bytes=%d", path, len(body))
        return {"electricity_body": body, "bronze_collected_date": collected_date}


class EiaElectricityPriceS3BronzeExtractor(Extractor):
    """전력요금 원본 bytes 를 S3 에서 읽습니다."""

    def __init__(self, bucket: str, year_month: str):
        self._bucket = bucket
        self._year_month = year_month
        self.name = f"eia_electricity_price_bronze_s3:{bucket}:{year_month}"

    def extract(self) -> dict:
        prefix = layout.bronze_s3_prefix(layout.ELECTRICITY_DATASET)
        keys = list_keys(self._bucket, prefix)
        collected_date, key = layout.newest_bronze_s3_key(
            keys, layout.ELECTRICITY_DATASET, layout.ELECTRICITY_FILE_NAME
        )
        body = get_object_bytes(self._bucket, key)
        if not body:
            raise ValueError(f"EIA 전력 Bronze 객체가 비어 있습니다: s3://{self._bucket}/{key}")

        logger.info("bronze_extract done key=%s bytes=%d", key, len(body))
        return {"electricity_body": body, "bronze_collected_date": collected_date}


def build_bronze_extractor(
    storage: str, base_dir: str, bucket: str | None, year_month: str
) -> Extractor:
    if storage == "local":
        return EiaElectricityPriceBronzeExtractor(base_dir, year_month)
    if storage == "s3":
        return EiaElectricityPriceS3BronzeExtractor(bucket, year_month)
    raise ValueError(f"알 수 없는 storage: {storage!r} (local 또는 s3)")
