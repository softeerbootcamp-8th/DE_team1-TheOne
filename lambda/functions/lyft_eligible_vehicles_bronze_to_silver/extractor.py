"""실행일의 Lyft 배차 가능 차량 Bronze 스냅샷을 읽습니다."""

import logging
import re
from datetime import date

import pyarrow as pa
import pyarrow.parquet as pq
from pipeline_core.extractor import Extractor

from ..common import lyft_eligible_vehicles_layout as layout

logger = logging.getLogger(__name__)

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class LyftEligibleVehiclesBronzeExtractor(Extractor):
    """해당 날짜 파티션에서 도시별 최신 Bronze Parquet을 읽습니다."""

    name = "lyft_eligible_vehicles_bronze"

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
        partition = layout.date_partition(self._base_dir, self.collected_date)
        city_dirs = sorted(
            d for d in partition.glob(f"{layout.CITY_PARTITION_KEY}=*") if d.is_dir()
        )
        if not city_dirs:
            raise FileNotFoundError(f"Bronze 파티션이 없습니다: {partition}")

        rows: list[dict] = []
        for city_dir in city_dirs:
            paths = sorted(city_dir.glob("*.parquet"))
            if not paths:
                raise FileNotFoundError(f"Bronze Parquet 파일이 없습니다: {city_dir}")

            path = paths[-1]
            try:
                table = pq.ParquetFile(path).read()
            except (OSError, pa.ArrowInvalid) as exc:
                raise RuntimeError(f"Bronze Parquet을 읽지 못했습니다: {path}") from exc
            if not table.num_rows:
                raise RuntimeError(f"Bronze Parquet이 비어 있습니다: {path}")

            city = layout.city_from_partition(city_dir)
            rows += [
                {**row, "city": city, "bronze_path": str(path)}
                for row in table.to_pylist()
            ]

        logger.info("bronze_extract done cities=%d rows=%d", len(city_dirs), len(rows))
        return rows
