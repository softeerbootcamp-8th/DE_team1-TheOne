"""정제된 리스 업체 보유 차량 대장을 Silver Parquet 으로 적재합니다."""

import logging

import pyarrow as pa
import pyarrow.parquet as pq
from pipeline_core.loader import Loader, WriteResult

from ..common.atomic_write import atomic_write
from ..common import vehicle_catalog_layout as layout

logger = logging.getLogger(__name__)

# 소비자가 실제로 쓰는 것만 남깁니다. 표기 원문(make/model/raw_name), 링크,
# 상수(currency/price_unit)는 전부 Bronze 에 있고 bronze_path 로 되짚을 수 있습니다.
# vendor / collected_date 는 파티션 키라 컬럼으로 두지 않습니다.
SCHEMA = pa.schema(
    [
        ("make_key", pa.string()),  # 조인 키 (대문자 정규화)
        ("model_key", pa.string()),  # 조인 키 (대문자 정규화)
        ("weekly_price_usd", pa.float64()),
        ("bronze_path", pa.string()),  # 계보
    ]
)


class VehicleCatalogSilverLoader(Loader):
    """업체별로 Parquet 하나씩 씁니다. 같은 파티션은 덮어씁니다.

    Bronze 와 달리 Silver 는 재실행하면 덮어씁니다. 같은 수집일을 다시 변환한
    결과가 여러 개 남으면 읽는 쪽에서 무엇이 맞는지 알 수 없기 때문입니다.
    """

    def __init__(self, base_dir: str, expect_collected_date: str | None = None):
        self._base_dir = base_dir
        self._expect_collected_date = expect_collected_date
        # 이번 실행이 쓴 업체별 경로. Pipeline 이 중간 데이터를 감추므로
        # 핸들러가 반환값을 만들 때 여기서 읽습니다.
        self.paths: list[str] = []

    def write(self, data: list[dict]) -> WriteResult:
        if not data:
            raise ValueError("적재할 차량 대장 Silver 데이터가 없습니다.")

        by_vendor: dict[str, list[dict]] = {}
        for row in data:
            by_vendor.setdefault(row[layout.VENDOR_PARTITION_KEY], []).append(row)

        written_rows = 0
        for vendor, vendor_rows in sorted(by_vendor.items()):
            collected_date = vendor_rows[0]["collected_at"].date()
            # 요청한 수집일과 실제로 정제된 날짜가 어긋나면 엉뚱한 파티션을 덮어씁니다.
            if (
                self._expect_collected_date
                and collected_date.isoformat() != self._expect_collected_date
            ):
                raise ValueError(
                    f"요청한 수집일과 변환된 수집일이 다릅니다: "
                    f"{self._expect_collected_date} != {collected_date.isoformat()}"
                )

            path = layout.silver_file(self._base_dir, collected_date, vendor)
            path.parent.mkdir(parents=True, exist_ok=True)

            table = pa.Table.from_pylist(vendor_rows, schema=SCHEMA)
            atomic_write(
                path,
                lambda temporary: pq.write_table(
                    table, temporary, compression="snappy"
                ),
            )

            logger.info(
                "silver_load done path=%s vendor=%s rows=%d",
                path,
                vendor,
                table.num_rows,
            )
            self.paths.append(str(path))
            written_rows += table.num_rows

        return WriteResult(
            location=str(layout.dataset_path(self._base_dir)),
            row_count=written_rows,
        )
