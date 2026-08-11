"""정제된 Lyft 배차 가능 차량 목록을 Silver Parquet으로 적재합니다."""

import logging

import pyarrow as pa
import pyarrow.parquet as pq
from pipeline_core.loader import Loader, WriteResult

from ..common import lyft_eligible_vehicles_layout as layout

logger = logging.getLogger(__name__)
SILVER_FILE_NAME = f"{layout.DATASET}.parquet"

# Uber Eligible Silver와 같은 컬럼을 사용해 차량 대장에서 함께 조인합니다.
SCHEMA = pa.schema(
    [
        ("make_key", pa.string()),
        ("model_key", pa.string()),
        ("product", pa.string()),
        ("min_year", pa.int16()),
        ("bronze_path", pa.string()),
    ]
)


class LyftEligibleVehiclesSilverLoader(Loader):
    """도시별 Silver 파일 하나를 쓰고 재실행 시 같은 파일을 덮어씁니다."""

    def __init__(self, base_dir: str, expect_collected_date: str | None = None):
        self._base_dir = base_dir
        self._expect_collected_date = expect_collected_date
        self.paths: list[str] = []

    def write(self, data: list[dict]) -> WriteResult:
        if not data:
            raise ValueError("적재할 Lyft 배차 가능 목록 Silver 데이터가 없습니다.")

        by_city: dict[str, list[dict]] = {}
        for row in data:
            by_city.setdefault(row[layout.CITY_PARTITION_KEY], []).append(row)

        written_rows = 0
        for city, city_rows in sorted(by_city.items()):
            collected_date = city_rows[0]["collected_at"].date()
            if (
                self._expect_collected_date
                and collected_date.isoformat() != self._expect_collected_date
            ):
                raise ValueError(
                    f"요청한 수집일과 변환된 수집일이 다릅니다: "
                    f"{self._expect_collected_date} != {collected_date.isoformat()}"
                )

            path = (
                layout.date_partition(self._base_dir, collected_date.isoformat())
                / f"{layout.CITY_PARTITION_KEY}={city}"
                / SILVER_FILE_NAME
            )
            path.parent.mkdir(parents=True, exist_ok=True)

            table = pa.Table.from_pylist(city_rows, schema=SCHEMA)
            pq.write_table(table, path, compression="snappy")

            logger.info(
                "silver_load done path=%s city=%s rows=%d", path, city, table.num_rows
            )
            self.paths.append(str(path))
            written_rows += table.num_rows

        return WriteResult(
            location=str(layout.dataset_path(self._base_dir)),
            row_count=written_rows,
        )
