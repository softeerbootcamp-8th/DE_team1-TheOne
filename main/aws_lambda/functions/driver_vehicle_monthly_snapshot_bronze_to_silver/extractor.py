"""기사 차량 월별 스냅샷 Bronze 원본 Parquet 한 파일을 읽습니다."""

import logging
from pathlib import Path
from urllib.parse import urlsplit

import pyarrow as pa
import pyarrow.parquet as pq
from pipeline_core.extractor import Extractor

from shared.common.s3_reader import get_object_bytes


logger = logging.getLogger(__name__)


class DriverVehicleMonthlySnapshotBronzeExtractor(Extractor):
    """월 파티션의 원본 파일을 그대로 읽습니다. 정제는 Transformer 가 합니다."""

    name = "driver_vehicle_monthly_snapshot_bronze"

    def __init__(self, bronze_path: str | Path):
        self._source = str(bronze_path)
        self._path = None if self._source.startswith("s3://") else Path(bronze_path)

    def extract(self) -> pa.Table:
        if self._path is None:
            parsed = urlsplit(self._source)
            body = get_object_bytes(parsed.netloc, parsed.path.lstrip("/"))
            source = pa.BufferReader(body)
        else:
            if not self._path.is_file():
                raise FileNotFoundError(
                    f"기사 차량 스냅샷 Bronze 파일이 없습니다: {self._path}"
                )
            source = self._path
        try:
            table = pq.ParquetFile(source).read()
        except (OSError, pa.ArrowInvalid) as exc:
            raise ValueError(
                "기사 차량 스냅샷 Bronze가 읽을 수 있는 Parquet이 아닙니다: "
                f"{self._source}"
            ) from exc
        logger.info("bronze_extract done path=%s rows=%d", self._source, table.num_rows)
        return table
