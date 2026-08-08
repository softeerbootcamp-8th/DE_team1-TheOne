"""Uber Eligible Vehicles 적재(load).

extract 가 만든 행 목록을 parquet 으로 씁니다.
지금은 로컬 경로만 지원하고, S3 적재는 다음 이슈에서 붙입니다.
"""

import logging
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

DATASET = "uber_eligible_vehicles"

SCHEMA = pa.schema(
    [
        ("city_slug", pa.string()),
        ("make", pa.string()),
        ("model", pa.string()),
        ("min_year", pa.int16()),
        ("products", pa.list_(pa.string())),
        ("raw_eligibility", pa.string()),  # 모델 원문 (파싱 검증/재처리용)
        ("collected_at", pa.timestamp("us", tz="UTC")),
    ]
)


def partition_path(base_dir: str, city_slug: str, collected_at: datetime) -> Path:
    """collected_date / city 로 나눈 Hive 파티션 경로."""
    return (
        Path(base_dir)
        / DATASET
        / f"collected_date={collected_at:%Y-%m-%d}"
        / f"city={city_slug}"
    )


def load(rows: list[dict], base_dir: str, collected_at: datetime) -> str:
    """행 목록을 파티션 하나에 parquet 한 개로 씁니다."""
    partition = partition_path(base_dir, rows[0]["city_slug"], collected_at)
    partition.mkdir(parents=True, exist_ok=True)
    path = partition / f"{collected_at:%Y%m%dT%H%M%SZ}.parquet"

    table = pa.Table.from_pylist(rows, schema=SCHEMA)
    pq.write_table(table, path, compression="snappy")

    logger.info("적재 완료: %s (%d행, %d bytes)", path, table.num_rows, path.stat().st_size)
    return str(path)
