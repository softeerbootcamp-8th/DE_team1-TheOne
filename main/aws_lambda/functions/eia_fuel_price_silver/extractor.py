"""휘발유·전력 CLEAN Silver 두 개를 읽습니다.

Bronze 원본을 직접 읽지 않습니다 — 정제(주간·월간 원본을 일별로 펼치는 일)는 각 원천의
`*_bronze_to_silver` 파이프라인이 이미 끝냈습니다(#512, #517). 이 단계는 **날짜로 붙이는
일**만 합니다.

두 CLEAN 은 대상 월 아래 수집 원천별 버전으로 남고, 각자 자기 계보
(`bronze_collected_date`, 전력은 `ev_price_status`)를 싣고 옵니다. 여기서는 완료된 최신
버전을 하나씩 고르고 원본은 다시 열지 않습니다.
"""

import io
import logging
from pathlib import Path

from main.aws_lambda.common.monthly_dataset import (
    join_segments, service_area_prefix, service_area_root, service_area_segment,
)

import pyarrow.parquet as pq
from pipeline_core.extractor import Extractor

from shared.common.eia_fuel_version import source_collected_at_token
from shared.common.s3_reader import get_object_bytes, list_keys
from shared.common.success_marker import data_key_is_complete, marker_path

logger = logging.getLogger(__name__)

GAS_DATASET = "eia_gas_price"
ELECTRICITY_DATASET = "eia_electricity_price"
PARTITION_KEY = "year_month"

_DAG_ID = {
    GAS_DATASET: "eia_gas_price_bronze_to_silver_pipeline",
    ELECTRICITY_DATASET: "eia_electricity_price_bronze_to_silver_pipeline",
}


def clean_silver_file(
    base_dir: str,
    dataset: str,
    year_month: str,
    source_collected_at: str,
    service_area: str,
) -> Path:
    dataset_root = Path(base_dir) / dataset
    area = service_area_segment(service_area)
    return (
        (dataset_root / area)
        / f"{PARTITION_KEY}={year_month}"
        / f"source_collected_at={source_collected_at}"
        / f"{dataset}.parquet"
    )


def clean_silver_key(
    dataset: str, year_month: str, source_collected_at: str, service_area: str
) -> str:
    return join_segments(
        "silver",
        dataset,
        service_area_segment(service_area),
        f"{PARTITION_KEY}={year_month}",
        f"source_collected_at={source_collected_at}",
        f"{dataset}.parquet",
    )


def _rows_from_bytes(dataset: str, body: bytes) -> list[dict]:
    rows = pq.ParquetFile(io.BytesIO(body)).read().to_pylist()
    if not rows:
        raise ValueError(f"{dataset} CLEAN Silver 가 비어 있습니다.")
    return rows


def _read(
    base_dir: str, dataset: str, year_month: str, service_area: str
) -> tuple[list[dict], str]:
    partition = (
        service_area_root(Path(base_dir) / dataset, service_area)
        / f"{PARTITION_KEY}={year_month}"
    )
    candidates = []
    for version in partition.glob("source_collected_at=*"):
        token = source_collected_at_token(version.name)
        path = version / f"{dataset}.parquet"
        if token and path.is_file() and marker_path(version).is_file():
            candidates.append((token, path))
    if candidates:
        token, path = max(candidates)
        # `pq.read_table` 은 경로의 `year_month=` 를 컬럼으로 덧붙입니다. 파일에
        # 실제로 쓰인 것만 봐야 하므로 ParquetFile 로 직접 읽습니다.
        rows = _rows_from_bytes(dataset, path.read_bytes())
        logger.info("clean_silver_extract done path=%s rows=%d", path, len(rows))
        return rows, token
    raise FileNotFoundError(
        f"{dataset} CLEAN Silver 가 없습니다: {partition} — "
        f"{_DAG_ID[dataset]} 을 먼저 돌리세요."
    )


def _read_s3(
    bucket: str, dataset: str, year_month: str, service_area: str
) -> tuple[list[dict], str]:
    area_prefix = service_area_prefix("silver", dataset, service_area=service_area)
    prefix = f"{area_prefix}/{PARTITION_KEY}={year_month}/"
    keys = set(list_keys(bucket, prefix))
    candidates = []
    for key in keys:
        relative = key.removeprefix(prefix)
        parts = relative.split("/")
        if len(parts) != 2 or parts[1] != f"{dataset}.parquet":
            continue
        token = source_collected_at_token(parts[0])
        if token and data_key_is_complete(key, keys):
            candidates.append((token, key))
    if candidates:
        token, key = max(candidates)
        body = get_object_bytes(bucket, key)
        rows = _rows_from_bytes(dataset, body)
        logger.info("clean_silver_extract done key=%s rows=%d", key, len(rows))
        return rows, token
    raise FileNotFoundError(
        f"{dataset} CLEAN Silver 가 없습니다: s3://{bucket}/{prefix} — "
        f"{_DAG_ID[dataset]} 을 먼저 돌리세요."
    )


class EiaFuelPriceCleanExtractor(Extractor):
    """휘발유·전력 CLEAN Silver 를 로컬에서 함께 읽습니다."""

    def __init__(self, base_dir: str, year_month: str, service_area: str):
        self._base_dir = base_dir
        self._year_month = year_month
        self._service_area = service_area
        self.name = f"eia_fuel_price_clean:{base_dir}:{year_month}"

    def extract(self) -> dict:
        gas_rows, gas_source = _read(
            self._base_dir, GAS_DATASET, self._year_month, self._service_area
        )
        electricity_rows, ev_source = _read(
            self._base_dir,
            ELECTRICITY_DATASET,
            self._year_month,
            self._service_area,
        )
        return {
            "gas_rows": gas_rows,
            "electricity_rows": electricity_rows,
            "gas_source_collected_at": gas_source,
            "ev_source_collected_at": ev_source,
        }


class EiaFuelPriceCleanS3Extractor(Extractor):
    """휘발유·전력 CLEAN Silver 를 S3 에서 함께 읽습니다.

    대상 월 아래 완료된 최신 `source_collected_at` 버전을 원천별로 고릅니다.
    """

    def __init__(self, bucket: str, year_month: str, service_area: str):
        self._bucket = bucket
        self._year_month = year_month
        self._service_area = service_area
        self.name = f"eia_fuel_price_clean_s3:{bucket}:{year_month}"

    def extract(self) -> dict:
        gas_rows, gas_source = _read_s3(
            self._bucket, GAS_DATASET, self._year_month, self._service_area
        )
        electricity_rows, ev_source = _read_s3(
            self._bucket,
            ELECTRICITY_DATASET,
            self._year_month,
            self._service_area,
        )
        return {
            "gas_rows": gas_rows,
            "electricity_rows": electricity_rows,
            "gas_source_collected_at": gas_source,
            "ev_source_collected_at": ev_source,
        }


def build_clean_extractor(
    storage: str,
    base_dir: str,
    bucket: str | None,
    year_month: str,
    service_area: str,
) -> Extractor:
    if storage == "local":
        return EiaFuelPriceCleanExtractor(base_dir, year_month, service_area)
    if storage == "s3":
        return EiaFuelPriceCleanS3Extractor(bucket, year_month, service_area)
    raise ValueError(f"알 수 없는 storage: {storage!r} (local 또는 s3)")
