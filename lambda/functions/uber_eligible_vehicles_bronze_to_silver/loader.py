"""정제된 Uber 배차 가능 차량 목록을 Silver Parquet 으로 적재합니다."""

import logging

import pyarrow as pa
import pyarrow.parquet as pq
from pipeline_core.loader import Loader, WriteResult

from schema.silver.eligible_vehicles import SCHEMA

from ..common.atomic_write import atomic_write
from ..common import uber_eligible_vehicles_layout as layout

logger = logging.getLogger(__name__)


class UberEligibleVehiclesSilverLoader(Loader):
    """도시별로 Parquet 하나씩 씁니다. 같은 파티션은 덮어씁니다.

    Bronze 와 달리 Silver 는 재실행하면 덮어씁니다. 같은 수집일을 다시 변환한
    결과가 여러 개 남으면 읽는 쪽에서 무엇이 맞는지 알 수 없기 때문입니다.
    """

    def __init__(self, base_dir: str, expect_collected_date: str | None = None):
        self._base_dir = base_dir
        self._expect_collected_date = expect_collected_date
        # 이번 실행이 쓴 도시별 경로. Pipeline 이 중간 데이터를 감추므로
        # 핸들러가 반환값을 만들 때 여기서 읽습니다.
        self.paths: list[str] = []

    def write(self, data: list[dict]) -> WriteResult:
        if not data:
            raise ValueError("적재할 Uber 배차 가능 목록 Silver 데이터가 없습니다.")

        by_city: dict[str, list[dict]] = {}
        for row in data:
            by_city.setdefault(row[layout.CITY_PARTITION_KEY], []).append(row)

        written_rows = 0
        for city, city_rows in sorted(by_city.items()):
            collected_date = city_rows[0]["collected_at"].date()
            # 요청한 수집일과 실제로 정제된 날짜가 어긋나면 엉뚱한 파티션을 덮어씁니다.
            if (
                self._expect_collected_date
                and collected_date.isoformat() != self._expect_collected_date
            ):
                raise ValueError(
                    f"요청한 수집일과 변환된 수집일이 다릅니다: "
                    f"{self._expect_collected_date} != {collected_date.isoformat()}"
                )

            path = layout.silver_file(self._base_dir, collected_date, city)
            path.parent.mkdir(parents=True, exist_ok=True)

            table = pa.Table.from_pylist(city_rows, schema=SCHEMA)
            atomic_write(
                path,
                lambda temporary: pq.write_table(
                    table, temporary, compression="snappy"
                ),
            )

            logger.info(
                "silver_load done path=%s city=%s rows=%d", path, city, table.num_rows
            )
            self.paths.append(str(path))
            written_rows += table.num_rows

        return WriteResult(
            location=str(layout.dataset_path(self._base_dir)),
            row_count=written_rows,
        )
