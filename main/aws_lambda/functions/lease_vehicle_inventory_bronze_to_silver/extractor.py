"""보유 차량 Bronze 원본 Parquet 한 파일을 읽습니다."""

import logging
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pipeline_core.extractor import Extractor


logger = logging.getLogger(__name__)


class LeaseVehicleInventoryBronzeExtractor(Extractor):
    """월 파티션의 원본 파일을 그대로 읽습니다. 정제는 Transformer 가 합니다."""

    name = "lease_vehicle_inventory_bronze"

    def __init__(self, bronze_path: str | Path):
        self._path = Path(bronze_path)

    def extract(self) -> pa.Table:
        if not self._path.is_file():
            raise FileNotFoundError(f"보유 차량 Bronze 파일이 없습니다: {self._path}")
        try:
            table = pq.ParquetFile(self._path).read()
        except (OSError, pa.ArrowInvalid) as exc:
            raise ValueError(
                f"보유 차량 Bronze가 읽을 수 있는 Parquet이 아닙니다: {self._path}"
            ) from exc
        logger.info("bronze_extract done path=%s rows=%d", self._path, table.num_rows)
        return table
