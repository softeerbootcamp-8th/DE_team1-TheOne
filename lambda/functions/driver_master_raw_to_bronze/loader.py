"""회사 원천 DB의 세 테이블을 날짜별 Bronze 이력으로 적재합니다."""

from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pipeline_core.loader import Loader, WriteResult

from ..common.atomic_write import atomic_write


class CompanySnapshotBronzeLoader(Loader):
    def __init__(self, base_dir: str, snapshot_date: str, collected_at: datetime):
        if collected_at.tzinfo is None:
            raise ValueError("collected_at에 시간대가 필요합니다")
        self._base_dir = base_dir
        self._snapshot_date = snapshot_date
        self._collected_at = collected_at.astimezone(timezone.utc)
        self.paths: list[str] = []
        self.row_counts: dict[str, int] = {}

    def write(self, data: dict[str, pa.Table]) -> WriteResult:
        timestamp = self._collected_at.strftime("%Y%m%dT%H%M%S%fZ")
        for name, table in sorted(data.items()):
            path = (
                Path(self._base_dir)
                / "company"
                / name
                / f"snapshot_date={self._snapshot_date}"
                / f"{timestamp}.parquet"
            )
            path.parent.mkdir(parents=True, exist_ok=True)
            atomic_write(
                path,
                lambda temporary, table=table: pq.write_table(
                    table, temporary, compression="snappy"
                ),
            )
            self.paths.append(str(path))
            self.row_counts[name] = table.num_rows

        return WriteResult(
            location=str(Path(self._base_dir) / "company"),
            row_count=sum(self.row_counts.values()),
        )
