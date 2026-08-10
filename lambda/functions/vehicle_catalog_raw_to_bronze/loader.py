"""Fast Track Leasing 렌탈 차량 적재(load).

extract 가 만든 행 목록을 parquet 으로 씁니다.
지금은 로컬 경로만 지원하고, S3 적재는 다음 이슈에서 붙입니다.
"""

import logging
from datetime import datetime

import pyarrow as pa
import pyarrow.parquet as pq
from pipeline_core.loader import Loader, WriteResult

from ..common import vehicle_catalog_layout as layout

logger = logging.getLogger(__name__)

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


class VehicleCatalogBronzeLoader(Loader):
    """행 목록을 업체 파티션 하나에 parquet 한 개로 씁니다."""

    def __init__(self, base_dir: str, collected_at: datetime):
        self._base_dir = base_dir
        self._collected_at = collected_at

    def write(self, data: list[dict]) -> WriteResult:
        path = layout.bronze_file(self._base_dir, data[0]["vendor"], self._collected_at)
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
