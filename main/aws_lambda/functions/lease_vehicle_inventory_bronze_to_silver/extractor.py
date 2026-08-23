"""보유 차량 Bronze 원본 Parquet 한 파일을 읽습니다."""

import logging
from pathlib import Path, PurePosixPath

import pyarrow as pa
import pyarrow.parquet as pq
from pipeline_core.extractor import Extractor

from shared.common.monthly_bronze import bronze_collection_token
from shared.common.s3_reader import get_object_bytes, list_keys

from .loader import DATASET


logger = logging.getLogger(__name__)


class LeaseVehicleInventoryBronzeExtractor(Extractor):
    """월 파티션의 원본 파일을 그대로 읽습니다. 정제는 Transformer 가 합니다."""

    name = "lease_vehicle_inventory_bronze"

    def __init__(self, bronze_path: str | Path):
        self._path = Path(bronze_path)

    def extract(self) -> pa.Table:
        if not self._path.is_file():
            raise FileNotFoundError(f"보유 차량 Bronze 파일이 없습니다: {self._path}")
        try:
            table = pq.ParquetFile(self._path).read()
        except (OSError, pa.ArrowInvalid) as exc:
            raise ValueError(
                f"보유 차량 Bronze가 읽을 수 있는 Parquet이 아닙니다: {self._path}"
            ) from exc
        logger.info("bronze_extract done path=%s rows=%d", self._path, table.num_rows)
        return table


class LeaseVehicleInventoryS3BronzeExtractor(Extractor):
    """월 파티션의 최신 수집분 원본을 S3 에서 읽습니다."""

    def __init__(self, bucket: str, year_month: str):
        self._bucket = bucket
        self._year_month = year_month
        self.name = f"lease_vehicle_inventory_bronze_s3:{bucket}:{year_month}"

    def extract(self) -> pa.Table:
        prefix = _bronze_s3_prefix(self._year_month)
        key = _newest_key(list_keys(self._bucket, prefix), prefix)
        body = get_object_bytes(self._bucket, key)
        if not body:
            raise ValueError(f"보유 차량 Bronze 객체가 비어 있습니다: s3://{self._bucket}/{key}")
        try:
            table = pq.read_table(pa.BufferReader(body))
        except (OSError, pa.ArrowInvalid) as exc:
            raise ValueError(
                f"보유 차량 Bronze가 읽을 수 있는 Parquet이 아닙니다: s3://{self._bucket}/{key}"
            ) from exc
        logger.info("bronze_extract done key=%s rows=%d", key, table.num_rows)
        return table


def _bronze_s3_prefix(year_month: str) -> str:
    return f"bronze/{DATASET}/year_month={year_month}/"


def _newest_key(keys: list[str], prefix: str) -> str:
    candidates = [
        (key, bronze_collection_token(PurePosixPath(key))) for key in keys
    ]
    candidates = [(key, token) for key, token in candidates if token]
    if not candidates:
        raise FileNotFoundError(f"보유 차량 Bronze S3 파티션이 없습니다: {prefix}")
    return max(candidates, key=lambda item: item[1])[0]


def _newest_bronze_path(base_dir: str, year_month: str) -> Path:
    partition = Path(base_dir) / DATASET / f"year_month={year_month}"
    candidates = [
        *partition.glob("*.parquet"),
        *partition.glob("collected_at=*/data.parquet"),
    ]
    candidates = [path for path in candidates if bronze_collection_token(path)]
    if not candidates:
        raise FileNotFoundError(f"보유 차량 Bronze 파티션이 없습니다: {partition}")
    return max(candidates, key=bronze_collection_token)


def build_bronze_extractor(
    storage: str, base_dir: str, bucket: str | None, year_month: str
) -> Extractor:
    if storage == "local":
        return LeaseVehicleInventoryBronzeExtractor(_newest_bronze_path(base_dir, year_month))
    if storage == "s3":
        return LeaseVehicleInventoryS3BronzeExtractor(bucket, year_month)
    raise ValueError(f"알 수 없는 storage: {storage!r} (local 또는 s3)")
