"""실행일의 전기차 충전소 Bronze 스냅샷을 읽습니다."""

import logging
import re
from datetime import date

import pyarrow as pa
import pyarrow.parquet as pq
from pipeline_core.extractor import Extractor

from ..common import ev_charging_layout as layout

logger = logging.getLogger(__name__)

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class EvChargingBronzeExtractor(Extractor):
    """해당 collected_date 파티션의 가장 최신 Parquet을 읽습니다."""

    name = "ev_charging_bronze"

    def __init__(self, base_dir: str, collected_date: str):
        if not DATE_RE.fullmatch(collected_date):
            raise ValueError("collected_date는 YYYY-MM-DD 형식이어야 합니다.")
        try:
            date.fromisoformat(collected_date)
        except ValueError as exc:
            raise ValueError("유효하지 않은 collected_date입니다.") from exc

        self._base_dir = base_dir
        self.collected_date = collected_date

    def extract(self) -> list[dict]:
        partition = layout.bronze_partition(self._base_dir, self.collected_date)
        paths = sorted(partition.glob("*.parquet"))
        if not paths:
            raise FileNotFoundError(f"Bronze Parquet 파일이 없습니다: {partition}")

        path = paths[-1]
        try:
            rows = pq.ParquetFile(path).read().to_pylist()
        except (OSError, pa.ArrowInvalid) as exc:
            raise RuntimeError(f"Bronze Parquet을 읽지 못했습니다: {path}") from exc
        if not rows:
            raise RuntimeError(f"Bronze Parquet이 비어 있습니다: {path}")

        logger.info("bronze_extract done path=%s rows=%d", path, len(rows))
        return [{**row, "bronze_path": str(path)} for row in rows]
