"""휘발유·전력 CLEAN Silver 두 개를 읽습니다.

Bronze 원본을 직접 읽지 않습니다 — 정제(주간·월간 원본을 일별로 펼치는 일)는 각 원천의
`*_bronze_to_silver` 파이프라인이 이미 끝냈습니다(#512, #517). 이 단계는 **날짜로 붙이는
일**만 합니다.

두 CLEAN 은 대상 월 파티션 하나씩이고, 각자 자기 계보(`bronze_collected_date`, 전력은
`ev_price_status`)를 싣고 옵니다. 그래서 여기서 원본을 다시 열어 볼 이유가 없습니다.
"""

import io
import logging
from pathlib import Path

from shared.common.service_area_path import join_segments, service_area_segment

import pyarrow.parquet as pq
from botocore.exceptions import ClientError
from pipeline_core.extractor import Extractor

from shared.common.s3_reader import get_object_bytes

logger = logging.getLogger(__name__)

GAS_DATASET = "eia_gas_price"
ELECTRICITY_DATASET = "eia_electricity_price"
PARTITION_KEY = "year_month"

_DAG_ID = {
    GAS_DATASET: "eia_gas_price_bronze_to_silver_pipeline",
    ELECTRICITY_DATASET: "eia_electricity_price_bronze_to_silver_pipeline",
}


def clean_silver_file(
    base_dir: str, dataset: str, year_month: str, service_area: str | None = None
) -> Path:
    dataset_root = Path(base_dir) / dataset
    area = service_area_segment(service_area)
    return (
        (dataset_root / area if area else dataset_root)
        / f"{PARTITION_KEY}={year_month}"
        / f"{dataset}.parquet"
    )


def clean_silver_key(
    dataset: str, year_month: str, service_area: str | None = None
) -> str:
    return join_segments(
        "silver",
        dataset,
        service_area_segment(service_area),
        f"{PARTITION_KEY}={year_month}",
        f"{dataset}.parquet",
    )


def _rows_from_bytes(dataset: str, body: bytes) -> list[dict]:
    rows = pq.ParquetFile(io.BytesIO(body)).read().to_pylist()
    if not rows:
        raise ValueError(f"{dataset} CLEAN Silver 가 비어 있습니다.")
    return rows


def _read(base_dir: str, dataset: str, year_month: str) -> list[dict]:
    path = clean_silver_file(base_dir, dataset, year_month)
    if not path.is_file():
        raise FileNotFoundError(
            f"{dataset} CLEAN Silver 가 없습니다: {path} — {_DAG_ID[dataset]} 을 먼저 돌리세요."
        )
    # `pq.read_table` 은 경로의 `year_month=` 를 컬럼으로 덧붙입니다. 파일에 실제로 쓰인
    # 것만 봐야 하므로 ParquetFile 로 직접 읽습니다.
    rows = _rows_from_bytes(dataset, path.read_bytes())
    logger.info("clean_silver_extract done path=%s rows=%d", path, len(rows))
    return rows


def _read_s3(bucket: str, dataset: str, year_month: str) -> list[dict]:
    key = clean_silver_key(dataset, year_month)
    try:
        body = get_object_bytes(bucket, key)
    except ClientError as error:
        raise FileNotFoundError(
            f"{dataset} CLEAN Silver 가 없습니다: s3://{bucket}/{key} — "
            f"{_DAG_ID[dataset]} 을 먼저 돌리세요."
        ) from error
    rows = _rows_from_bytes(dataset, body)
    logger.info("clean_silver_extract done key=%s rows=%d", key, len(rows))
    return rows


class EiaFuelPriceCleanExtractor(Extractor):
    """휘발유·전력 CLEAN Silver 를 로컬에서 함께 읽습니다."""

    def __init__(self, base_dir: str, year_month: str):
        self._base_dir = base_dir
        self._year_month = year_month
        self.name = f"eia_fuel_price_clean:{base_dir}:{year_month}"

    def extract(self) -> dict:
        return {
            "gas_rows": _read(self._base_dir, GAS_DATASET, self._year_month),
            "electricity_rows": _read(self._base_dir, ELECTRICITY_DATASET, self._year_month),
        }


class EiaFuelPriceCleanS3Extractor(Extractor):
    """휘발유·전력 CLEAN Silver 를 S3 에서 함께 읽습니다.

    각 CLEAN 은 대상 월 파티션 하나뿐이라 위치가 `year_month` 로 고정됩니다 — 파티션
    나열이 필요 없어 raw_to_bronze 쪽의 "최신 파티션 찾기" 와 다릅니다.
    """

    def __init__(self, bucket: str, year_month: str):
        self._bucket = bucket
        self._year_month = year_month
        self.name = f"eia_fuel_price_clean_s3:{bucket}:{year_month}"

    def extract(self) -> dict:
        return {
            "gas_rows": _read_s3(self._bucket, GAS_DATASET, self._year_month),
            "electricity_rows": _read_s3(self._bucket, ELECTRICITY_DATASET, self._year_month),
        }


def build_clean_extractor(
    storage: str, base_dir: str, bucket: str | None, year_month: str
) -> Extractor:
    if storage == "local":
        return EiaFuelPriceCleanExtractor(base_dir, year_month)
    if storage == "s3":
        return EiaFuelPriceCleanS3Extractor(bucket, year_month)
    raise ValueError(f"알 수 없는 storage: {storage!r} (local 또는 s3)")
