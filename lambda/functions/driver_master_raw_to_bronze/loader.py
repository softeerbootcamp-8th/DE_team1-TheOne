"""기사 마스터 테이블 월별 스냅샷 적재(load).

extract 가 만든 행 목록(전월 스냅샷 + 이번 달 신규/탈퇴 반영)을 다른 `*_raw_to_bronze`
와 동일한 `year_month=` 파티션 규칙으로 parquet 한 개에 씁니다.
"""

import logging
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pipeline_core.loader import Loader, WriteResult

from .extractor import DATASET

logger = logging.getLogger(__name__)

SCHEMA = pa.schema(
    [
        ("driver_id", pa.string()),
        ("driver_name", pa.string()),
        ("primary_distance_bands", pa.string()),
        ("primary_time_blocks", pa.string()),
        ("active_weekdays", pa.string()),
        ("max_idle_seconds", pa.float64()),
        ("min_idle_seconds", pa.float64()),
        ("max_trip_count", pa.int64()),
        ("min_trip_count", pa.int64()),
        ("min_work_minutes", pa.float64()),
        ("max_work_minutes", pa.float64()),
        ("max_rest_minutes", pa.float64()),
        ("min_rest_minutes", pa.float64()),
        ("churned_at", pa.timestamp("us")),
        ("joined_at", pa.timestamp("us")),
    ]
)


class DriverMasterBronzeLoader(Loader):
    """행 목록을 `year_month=` 파티션 하나에 parquet 한 개로 씁니다."""

    def __init__(self, base_dir: str, year_month: str, collected_at: datetime):
        self._base_dir = base_dir
        self._year_month = year_month
        self._collected_at = collected_at

    def partition_path(self) -> Path:
        return Path(self._base_dir) / DATASET / f"year_month={self._year_month}"

    def write(self, data: list[dict]) -> WriteResult:
        partition = self.partition_path()
        partition.mkdir(parents=True, exist_ok=True)
        path = partition / f"{self._collected_at:%Y%m%dT%H%M%SZ}.parquet"

        table = pa.Table.from_pylist(data, schema=SCHEMA)
        pq.write_table(table, path, compression="snappy")

        logger.info("bronze_load done path=%s rows=%d bytes=%d", path, table.num_rows, path.stat().st_size)
        return WriteResult(location=str(path), row_count=table.num_rows)
