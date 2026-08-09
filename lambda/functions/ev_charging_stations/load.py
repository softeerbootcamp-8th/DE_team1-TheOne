"""뉴욕주 전기차 충전소 데이터를 로컬 Bronze Parquet으로 적재합니다."""

import logging
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

DATASET = "ev_charging_stations"

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


def partition_path(base_dir: str, collected_at: datetime) -> Path:
    """수집일 기준 Hive 파티션 경로를 반환합니다."""
    return Path(base_dir) / DATASET / f"collected_date={collected_at:%Y-%m-%d}"


def load(rows: list[dict], base_dir: str, collected_at: datetime) -> str:
    """하루치 충전소 스냅샷을 Parquet 파일 하나로 저장합니다."""
    partition = partition_path(base_dir, collected_at)
    partition.mkdir(parents=True, exist_ok=True)
    path = partition / f"{collected_at:%Y%m%dT%H%M%SZ}.parquet"

    table = pa.Table.from_pylist(rows, schema=SCHEMA)
    pq.write_table(table, path, compression="snappy")

    logger.info("적재 완료: %s (%d행, %d bytes)", path, table.num_rows, path.stat().st_size)
    return str(path)
