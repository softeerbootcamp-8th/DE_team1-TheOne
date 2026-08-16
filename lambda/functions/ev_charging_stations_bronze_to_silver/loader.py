"""뉴욕시 일별 평균 충전 요금을 월별 Silver Parquet으로 적재합니다."""

import logging

import pyarrow as pa
import pyarrow.parquet as pq
from pipeline_core.loader import Loader, WriteResult

from schema.silver.ev_charging_price import SCHEMA

from ..common.atomic_write import atomic_write
from ..common import ev_charging_layout as layout

logger = logging.getLogger(__name__)


class EvChargingSilverLoader(Loader):
    """정제된 한 달치를 고정 경로의 Parquet 파일 하나로 저장합니다."""

    def __init__(self, base_dir: str, collected_month: str):
        self._base_dir = base_dir
        self._collected_month = collected_month

    def write(self, data: list[dict]) -> WriteResult:
        if not data:
            raise ValueError("적재할 EV Charging Silver 데이터가 없습니다.")

        path = layout.silver_file(self._base_dir, self._collected_month)
        path.parent.mkdir(parents=True, exist_ok=True)
        table = pa.Table.from_pylist(data, schema=SCHEMA)
        atomic_write(
            path,
            lambda temporary: pq.write_table(
                table, temporary, compression="snappy"
            ),
        )

        logger.info(
            "silver_load done path=%s collected_month=%s rows=%d",
            path,
            self._collected_month,
            table.num_rows,
        )
        return WriteResult(location=str(path), row_count=table.num_rows)
