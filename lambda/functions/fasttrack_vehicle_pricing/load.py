"""Fast Track Leasing 렌탈 차량 적재(load).

extract 가 만든 행 목록을 parquet 으로 씁니다.
지금은 로컬 경로만 지원하고, S3 적재는 다음 이슈에서 붙입니다.
"""

import logging
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

DATASET = "fasttrack_vehicle_pricing"

# vendor 는 파티션 키(vendor=)로만 남깁니다. 파일 안에 같은 이름의 컬럼을 또 두면
# 읽을 때 파티션 값(dictionary)과 타입이 충돌합니다.
SCHEMA = pa.schema(
    [
        ("make", pa.string()),
        ("model", pa.string()),
        ("raw_name", pa.string()),  # 사이트 표기 원문
        ("price_usd", pa.float64()),  # 이미지 안에만 있어 현재는 항상 null
        ("price_period", pa.string()),
        ("image_url", pa.string()),  # 가격이 바뀌면 이 URL 이 바뀜
        ("booking_url", pa.string()),
        ("source_url", pa.string()),
        ("collected_at", pa.timestamp("us", tz="UTC")),
    ]
)


def partition_path(base_dir: str, vendor: str, collected_at: datetime) -> Path:
    """collected_date / vendor 로 나눈 Hive 파티션 경로."""
    return (
        Path(base_dir)
        / DATASET
        / f"collected_date={collected_at:%Y-%m-%d}"
        / f"vendor={vendor}"
    )


def load(rows: list[dict], base_dir: str, collected_at: datetime) -> str:
    """행 목록을 파티션 하나에 parquet 한 개로 씁니다."""
    partition = partition_path(base_dir, rows[0]["vendor"], collected_at)
    partition.mkdir(parents=True, exist_ok=True)
    path = partition / f"{collected_at:%Y%m%dT%H%M%SZ}.parquet"

    table = pa.Table.from_pylist(rows, schema=SCHEMA)
    pq.write_table(table, path, compression="snappy")

    logger.info("적재 완료: %s (%d행, %d bytes)", path, table.num_rows, path.stat().st_size)
    return str(path)
