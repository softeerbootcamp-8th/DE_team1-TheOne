"""Lyft Premium 대상 차량 행을 Bronze Parquet으로 적재합니다."""

import logging
from datetime import datetime

import pyarrow as pa
import pyarrow.parquet as pq
from pipeline_core.loader import Loader, WriteResult

from schema.bronze.lyft_eligible_vehicles import SCHEMA

from shared.lambda_runtime.common import lyft_eligible_vehicles_layout as layout
from shared.lambda_runtime.common.atomic_write import atomic_write
from shared.lambda_runtime.common.s3_loader import S3Loader, S3Object

logger = logging.getLogger(__name__)


class LyftEligibleVehiclesBronzeLoader(Loader):
    """추출 결과를 선별하지 않고 일별 Bronze 파일 하나로 로컬에 저장합니다."""

    def __init__(self, base_dir: str, city_slug: str, collected_at: datetime):
        self._base_dir = base_dir
        self._city_slug = city_slug
        self._collected_at = collected_at

    def write(self, data: list[dict]) -> WriteResult:
        path = layout.bronze_file(self._base_dir, self._city_slug, self._collected_at)
        path.parent.mkdir(parents=True, exist_ok=True)

        table = pa.Table.from_pylist(data, schema=SCHEMA)
        atomic_write(
            path,
            lambda temporary: pq.write_table(
                table, temporary, compression="snappy"
            ),
        )

        logger.info(
            "bronze_load done path=%s rows=%d bytes=%d",
            path,
            table.num_rows,
            path.stat().st_size,
        )
        return WriteResult(location=str(path), row_count=table.num_rows)


class LyftEligibleVehiclesS3BronzeLoader(Loader):
    """추출 결과를 선별하지 않고 일별 Bronze 파일 하나로 S3에 저장합니다."""

    def __init__(self, city_slug: str, collected_at: datetime, bucket: str | None = None):
        self._city_slug = city_slug
        self._collected_at = collected_at
        self._bucket = bucket

    def write(self, data: list[dict]) -> WriteResult:
        key = layout.bronze_key(self._city_slug, self._collected_at)

        table = pa.Table.from_pylist(data, schema=SCHEMA)
        buffer = pa.BufferOutputStream()
        pq.write_table(table, buffer, compression="snappy")
        body = buffer.getvalue().to_pybytes()

        result = S3Loader(key=key, bucket=self._bucket).write(
            S3Object(body=body, row_count=table.num_rows)
        )
        logger.info(
            "bronze_load done location=%s rows=%d bytes=%d",
            result.location,
            table.num_rows,
            len(body),
        )
        return result


def build_bronze_loader(
    storage: str,
    base_dir: str,
    city_slug: str,
    collected_at: datetime,
    bucket: str | None = None,
) -> Loader:
    """storage 파라미터로 로컬/S3 Loader 중 하나를 고릅니다."""
    if storage == "local":
        return LyftEligibleVehiclesBronzeLoader(base_dir, city_slug, collected_at)
    if storage == "s3":
        return LyftEligibleVehiclesS3BronzeLoader(city_slug, collected_at, bucket=bucket)
    raise ValueError(f"알 수 없는 storage: {storage!r} (local 또는 s3)")
