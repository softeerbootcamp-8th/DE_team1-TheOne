"""Lyft Premium 대상 차량 행을 Bronze Parquet으로 적재합니다."""

import logging
from datetime import datetime

import pyarrow as pa
import pyarrow.parquet as pq
from pipeline_core.loader import Loader, WriteResult

from ..common import lyft_eligible_vehicles_layout as layout

logger = logging.getLogger(__name__)

SCHEMA = pa.schema(
    [
        ("city_slug", pa.string()),
        ("make", pa.string()),
        ("model", pa.string()),
        ("min_year", pa.int16()),
        ("products", pa.list_(pa.string())),
        ("raw_eligibility", pa.string()),
        ("raw_vehicle", pa.string()),
        ("source_url", pa.string()),
        ("collected_at", pa.timestamp("us", tz="UTC")),
    ]
)


class LyftEligibleVehiclesBronzeLoader(Loader):
    """추출 결과를 선별하지 않고 일별 Bronze 파일 하나로 저장합니다."""

    def __init__(self, base_dir: str, city_slug: str, collected_at: datetime):
        self._base_dir = base_dir
        self._city_slug = city_slug
        self._collected_at = collected_at

    def write(self, data: list[dict]) -> WriteResult:
        path = layout.bronze_file(self._base_dir, self._city_slug, self._collected_at)
        path.parent.mkdir(parents=True, exist_ok=True)

        table = pa.Table.from_pylist(data, schema=SCHEMA)
        temporary = path.with_suffix(".tmp")
        pq.write_table(table, temporary, compression="snappy")
        temporary.replace(path)

        logger.info(
            "bronze_load done path=%s rows=%d bytes=%d",
            path,
            table.num_rows,
            path.stat().st_size,
        )
        return WriteResult(location=str(path), row_count=table.num_rows)
