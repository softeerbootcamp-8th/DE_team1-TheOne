"""정제된 Gas Price 데이터를 월별 Silver Parquet으로 적재합니다."""

import logging
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq

from pipeline_core.loader import Loader, WriteResult

from ..common import gas_price_layout as layout

logger = logging.getLogger(__name__)

SCHEMA = pa.schema(
    [
        ("state", pa.string()),
        ("fuel_type", pa.string()),
        ("price_usd_per_gallon", pa.float64()),
        ("price_date", pa.date32()),
        ("source_url", pa.string()),
        ("collected_at", pa.timestamp("us", tz="UTC")),
        ("bronze_path", pa.string()),
    ]
)


class GasPriceSilverLoader(Loader):
    """정제된 한 달치를 고정 경로의 Parquet 파일 하나로 저장합니다."""

    def __init__(self, base_dir: str, collected_month: str):
        self._base_dir = base_dir
        self._collected_month = collected_month

    def write(self, data: list[dict]) -> WriteResult:
        if not data:
            raise ValueError("적재할 Gas Price Silver 데이터가 없습니다.")

        path = layout.silver_file(self._base_dir, self._collected_month)
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary_path = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
        table = pa.Table.from_pylist(data, schema=SCHEMA)

        try:
            pq.write_table(table, temporary_path, compression="snappy")
            temporary_path.replace(path)
        finally:
            temporary_path.unlink(missing_ok=True)

        logger.info(
            "silver_load done path=%s collected_month=%s rows=%d",
            path,
            self._collected_month,
            table.num_rows,
        )
        return WriteResult(location=str(path), row_count=table.num_rows)
