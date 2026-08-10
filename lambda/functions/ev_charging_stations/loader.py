"""뉴욕주 전기차 충전소 데이터를 로컬 Bronze Parquet으로 적재합니다."""

import logging
from datetime import datetime

import pyarrow as pa
import pyarrow.parquet as pq
from pipeline_core.loader import Loader, WriteResult

from ..common import ev_charging_layout as layout

logger = logging.getLogger(__name__)

SCHEMA = pa.schema(
    [
        ("station_id", pa.int64()),
        ("station_name", pa.string()),
        ("fuel_type_code", pa.string()),
        ("status_code", pa.string()),
        ("access_code", pa.string()),
        ("restricted_access", pa.bool_()),
        ("street_address", pa.string()),
        ("city", pa.string()),
        ("state", pa.string()),
        ("zip", pa.string()),
        ("latitude", pa.float64()),
        ("longitude", pa.float64()),
        ("ev_network", pa.string()),
        ("ev_network_web", pa.string()),
        ("ev_connector_types", pa.list_(pa.string())),
        ("ev_level1_evse_num", pa.int64()),
        ("ev_level2_evse_num", pa.int64()),
        ("ev_dc_fast_num", pa.int64()),
        ("ev_pricing", pa.string()),
        ("cards_accepted", pa.string()),
        ("date_last_confirmed", pa.string()),
        ("updated_at", pa.string()),
        ("source_url", pa.string()),
        ("collected_at", pa.timestamp("us", tz="UTC")),
    ]
)


class EvChargingBronzeLoader(Loader):
    """하루치 충전소 스냅샷을 Parquet 파일 하나로 저장합니다."""

    def __init__(self, base_dir: str, collected_at: datetime):
        self._base_dir = base_dir
        self._collected_at = collected_at

    def write(self, data: list[dict]) -> WriteResult:
        path = layout.bronze_file(self._base_dir, self._collected_at)
        path.parent.mkdir(parents=True, exist_ok=True)

        table = pa.Table.from_pylist(data, schema=SCHEMA)
        pq.write_table(table, path, compression="snappy")

        logger.info(
            "bronze_load done path=%s rows=%d bytes=%d",
            path,
            table.num_rows,
            path.stat().st_size,
        )
        return WriteResult(location=str(path), row_count=table.num_rows)
