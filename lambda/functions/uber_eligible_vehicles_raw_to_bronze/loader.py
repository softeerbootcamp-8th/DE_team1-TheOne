"""Uber Eligible Vehicles 적재(load).

extract 가 만든 행 목록을 parquet 으로 씁니다.
지금은 로컬 경로만 지원하고, S3 적재는 다음 이슈에서 붙입니다.
"""

import logging
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pipeline_core.loader import Loader, WriteResult

from schema.bronze.uber_eligible_vehicles import SCHEMA

from ..common.atomic_write import atomic_write

logger = logging.getLogger(__name__)

# 데이터셋 고유 명칭
DATASET = "uber_eligible_vehicles"


class UberEligibleVehiclesBronzeLoader(Loader):
    """행 목록을 도시 파티션 하나에 parquet 한 개로 씁니다."""

    def __init__(self, base_dir: str, city_slug: str, collected_at: datetime):
        self._base_dir = base_dir
        self._city_slug = city_slug
        self._collected_at = collected_at

    def partition_path(self) -> Path:
        """collected_date / city 로 나눈 Hive 파티션 경로."""
        return (
            Path(self._base_dir)
            / DATASET
            / f"collected_date={self._collected_at:%Y-%m-%d}"
            / f"city={self._city_slug}"
        )

    def write(self, data: list[dict]) -> WriteResult:
        partition = self.partition_path()
        partition.mkdir(parents=True, exist_ok=True)
        path = partition / f"{self._collected_at:%Y%m%dT%H%M%SZ}.parquet"

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
