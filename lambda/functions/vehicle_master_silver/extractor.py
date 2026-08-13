"""차량 마스터를 만들 네 개의 원천 Silver 를 읽습니다.

원천마다 최신 파티션이 다른 날짜에 있습니다(제원 월 1회, 나머지 주 1회). 각각
`latest_date_partition` 으로 따로 찾습니다 — 자세한 이유는 그 함수의 docstring.

경로와 파일명은 각 데이터셋의 layout 모듈에서 가져옵니다. 여기에 문자열로
박아두면 원천이 파티션 규칙을 바꿔도 조용히 0건이 됩니다.
"""

import logging
import re
from dataclasses import dataclass, field
from datetime import date
from types import ModuleType

import pyarrow as pa
import pyarrow.parquet as pq
from pipeline_core.extractor import Extractor

from ..common import lyft_eligible_vehicles_layout as lyft_layout
from ..common import uber_eligible_vehicles_layout as uber_layout
from ..common import vehicle_catalog_layout as catalog_layout
from ..common import vehicle_master_layout as layout
from ..common import vehicle_specs_layout as specs_layout

logger = logging.getLogger(__name__)

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

# (필드명, layout 모듈, 하위 파티션 키). 하위 파티션 키가 데이터셋마다 다릅니다 —
# 대장은 업체, 제원은 출처, 자격은 도시입니다.
SOURCES: tuple[tuple[str, ModuleType, str], ...] = (
    ("catalog", catalog_layout, catalog_layout.VENDOR_PARTITION_KEY),
    ("specs", specs_layout, specs_layout.SOURCE_PARTITION_KEY),
    ("uber", uber_layout, uber_layout.CITY_PARTITION_KEY),
    ("lyft", lyft_layout, lyft_layout.CITY_PARTITION_KEY),
)


@dataclass
class SourceTables:
    """원천 네 개를 한 덩어리로 넘깁니다. Transformer 가 그대로 받습니다."""

    catalog: list[dict] = field(default_factory=list)
    specs: list[dict] = field(default_factory=list)
    uber: list[dict] = field(default_factory=list)
    lyft: list[dict] = field(default_factory=list)
    # 어느 스냅샷을 읽었는지. 핸들러 응답에 실어 Airflow 로그에서 확인합니다.
    source_collected_dates: dict[str, str] = field(default_factory=dict)


class VehicleMasterSilverExtractor(Extractor):
    """네 개 Silver 데이터셋의 최신 파티션을 읽어 합칩니다."""

    name = "vehicle_master_silver_sources"

    def __init__(self, base_dir: str, as_of: str):
        if not DATE_RE.fullmatch(as_of):
            raise ValueError("as_of는 YYYY-MM-DD 형식이어야 합니다.")
        try:
            self.as_of = date.fromisoformat(as_of)
        except ValueError as exc:
            raise ValueError("유효하지 않은 as_of입니다.") from exc
        self._base_dir = base_dir
        # 이번 실행이 읽은 원천 스냅샷 날짜. Pipeline 이 중간 데이터를 감추므로
        # 핸들러가 반환값을 만들 때 여기서 읽습니다 (Loader.paths 와 같은 방식).
        self.source_collected_dates: dict[str, str] = {}

    def extract(self) -> SourceTables:
        tables = SourceTables()
        for attr, source_layout, sub_key in SOURCES:
            collected_date, rows = self._read_dataset(source_layout, sub_key)
            setattr(tables, attr, rows)
            tables.source_collected_dates[source_layout.DATASET] = (
                collected_date.isoformat()
            )
            logger.info(
                "source_extract done dataset=%s collected_date=%s rows=%d",
                source_layout.DATASET,
                collected_date.isoformat(),
                len(rows),
            )
        self.source_collected_dates = dict(tables.source_collected_dates)
        return tables

    def _read_dataset(
        self, source_layout: ModuleType, sub_key: str
    ) -> tuple[date, list[dict]]:
        """`collected_date=*/<sub_key>=*/<데이터셋>.parquet` 을 전부 읽습니다.

        Silver 파일명은 데이터셋마다 고정이라 같은 파티션에 여러 파일이 쌓이지
        않습니다. Bronze 처럼 최신 파일을 고를 필요가 없습니다.
        """
        dataset_dir = source_layout.dataset_path(self._base_dir)
        collected_date, partition = layout.latest_date_partition(dataset_dir, self.as_of)

        sub_dirs = sorted(d for d in partition.glob(f"{sub_key}=*") if d.is_dir())
        if not sub_dirs:
            raise FileNotFoundError(f"Silver 파티션이 비어 있습니다: {partition}")

        rows: list[dict] = []
        for sub_dir in sub_dirs:
            path = sub_dir / source_layout.SILVER_FILE_NAME
            if not path.is_file():
                raise FileNotFoundError(f"Silver Parquet 파일이 없습니다: {path}")
            try:
                table = pq.ParquetFile(path).read()
            except (OSError, pa.ArrowInvalid) as exc:
                raise RuntimeError(f"Silver Parquet을 읽지 못했습니다: {path}") from exc
            if not table.num_rows:
                raise RuntimeError(f"Silver Parquet이 비어 있습니다: {path}")

            # 파티션 키는 파일 안에 없습니다. 디렉터리명에서 되살립니다.
            sub_value = sub_dir.name.removeprefix(f"{sub_key}=")
            rows += [{**row, sub_key: sub_value} for row in table.to_pylist()]

        return collected_date, rows
