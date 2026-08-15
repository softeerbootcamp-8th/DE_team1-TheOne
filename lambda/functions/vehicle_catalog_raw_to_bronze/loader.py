"""Fast Track Leasing 렌탈 차량 적재(load).

extract 가 만든 행 목록을 parquet 으로 씁니다.
지금은 로컬 경로만 지원하고, S3 적재는 다음 이슈에서 붙입니다.
"""

import logging
from datetime import datetime

import pyarrow as pa
import pyarrow.parquet as pq
from pipeline_core.loader import Loader, WriteResult

from schema.bronze.vehicle_catalog import SCHEMA

from ..common import vehicle_catalog_layout as layout
from ..common.atomic_write import atomic_write

logger = logging.getLogger(__name__)


class VehicleCatalogBronzeLoader(Loader):
    """행 목록을 업체 파티션 하나에 parquet 한 개로 씁니다."""

    def __init__(self, base_dir: str, collected_at: datetime):
        self._base_dir = base_dir
        self._collected_at = collected_at

    def write(self, data: list[dict]) -> WriteResult:
        path = layout.bronze_file(self._base_dir, data[0]["vendor"], self._collected_at)
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
