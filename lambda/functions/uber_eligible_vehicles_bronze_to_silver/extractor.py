"""실행일의 Uber 배차 가능 차량 Bronze 스냅샷을 읽습니다."""

import logging
import re
from datetime import date

import pyarrow as pa
import pyarrow.parquet as pq
from pipeline_core.extractor import Extractor

from ..common import uber_eligible_vehicles_layout as layout

logger = logging.getLogger(__name__)

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class UberEligibleVehiclesBronzeExtractor(Extractor):
    """해당 collected_date 파티션에서 도시별 가장 최신 Parquet 을 읽어 합칩니다.

    Bronze 는 collected_date 아래에 city 파티션이 한 단계 더 있습니다.
    같은 날 여러 번 수집하면 파일이 쌓이므로 도시별로 최신 것만 씁니다.
    """

    name = "uber_eligible_vehicles_bronze"

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

            # city 는 파티션 키라서 파일 안에 없습니다. 디렉터리명에서 되살립니다.
            # (Bronze 파일 안의 city_slug 컬럼과 같은 값이지만, 파티션이 정답입니다.)
            city = layout.city_from_partition(city_dir)
            rows += [
                {**row, "city": city, "bronze_path": str(path)}
                for row in table.to_pylist()
            ]

        logger.info("bronze_extract done cities=%d rows=%d", len(city_dirs), len(rows))
        return rows
