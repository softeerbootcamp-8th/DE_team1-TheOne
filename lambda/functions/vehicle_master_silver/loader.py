"""차량 마스터를 도시별 Silver Parquet 으로 적재합니다."""

import logging
from datetime import date

import pyarrow as pa
import pyarrow.parquet as pq
from pipeline_core.loader import Loader, WriteResult

from ..common import vehicle_master_layout as layout

logger = logging.getLogger(__name__)

# city / collected_date 는 파티션 키라 컬럼으로 두지 않습니다.
# 원천의 표기 원문과 나머지 제원 컬럼은 각 원천 Silver 에 있고 `*_bronze_path`
# 로 되짚을 수 있습니다.
SCHEMA = pa.schema(
    [
        ("vendor", pa.string()),  # 대장을 낸 리스 업체
        ("make_key", pa.string()),  # 조인 키 (대문자 정규화)
        ("model_key", pa.string()),  # 조인 키 (대문자 정규화)
        ("platform", pa.string()),  # uber / lyft, 자격 없으면 NULL
        ("product", pa.string()),  # UberX / Comfort / Extra Comfort ...
        ("min_year", pa.int16()),  # 이 상품에 필요한 최소 차량 연식
        ("weekly_price_usd", pa.float64()),  # 리스 업체 주간 렌트료
        ("spec_year", pa.int16()),  # 대표 제원의 연식
        ("combined_mpg", pa.float64()),  # 전기차는 MPGe
        ("combined_kwh_per_100mi", pa.float64()),
        ("range_miles", pa.float64()),
        ("atv_type", pa.string()),  # 제원 원본 표기 (EV / Plug-in Hybrid / ...)
        ("fuel_type", pa.string()),  # EV / PHEV / HYBRID / GAS
        ("spec_match_level", pa.string()),  # MODEL / BASE_MODEL / NONE
        ("catalog_bronze_path", pa.string()),  # 계보
        ("specs_bronze_path", pa.string()),
        ("eligibility_bronze_path", pa.string()),
    ]
)


class VehicleMasterSilverLoader(Loader):
    """도시별로 Parquet 하나씩 씁니다. 같은 파티션은 덮어씁니다.

    재실행하면 덮어씁니다. 같은 날 다시 만든 결과가 여러 개 남으면 읽는 쪽에서
    무엇이 맞는지 알 수 없기 때문입니다.
    """

    def __init__(self, base_dir: str, collected_date: str):
        self._base_dir = base_dir
        self._collected_date = date.fromisoformat(collected_date)
        # 이번 실행이 쓴 도시별 경로. Pipeline 이 중간 데이터를 감추므로
        # 핸들러가 반환값을 만들 때 여기서 읽습니다.
        self.paths: list[str] = []

    def write(self, data: list[dict]) -> WriteResult:
        if not data:
            raise ValueError("적재할 차량 마스터 Silver 데이터가 없습니다.")

        by_city: dict[str, list[dict]] = {}
        for row in data:
            by_city.setdefault(row[layout.CITY_PARTITION_KEY], []).append(row)

        written_rows = 0
        for city, city_rows in sorted(by_city.items()):
            path = layout.silver_file(self._base_dir, self._collected_date, city)
            path.parent.mkdir(parents=True, exist_ok=True)

            # city 는 파티션 키라 컬럼에서 뺍니다. 스키마에 없는 키가 남아 있으면
            # from_pylist 가 조용히 무시하지 않고 실패합니다.
            table = pa.Table.from_pylist(
                [
                    {name: row.get(name) for name in SCHEMA.names}
                    for row in city_rows
                ],
                schema=SCHEMA,
            )
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
