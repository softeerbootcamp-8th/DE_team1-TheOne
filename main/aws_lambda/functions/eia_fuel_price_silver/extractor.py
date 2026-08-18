"""휘발유·전력 CLEAN Silver 두 개를 읽습니다.

Bronze 원본을 직접 읽지 않습니다 — 정제(주간·월간 원본을 일별로 펼치는 일)는 각 원천의
`*_bronze_to_silver` 파이프라인이 이미 끝냈습니다(#512, #517). 이 단계는 **날짜로 붙이는
일**만 합니다.

두 CLEAN 은 대상 월 파티션 하나씩이고, 각자 자기 계보(`bronze_collected_date`, 전력은
`ev_price_status`)를 싣고 옵니다. 그래서 여기서 원본을 다시 열어 볼 이유가 없습니다.
"""

import logging
from pathlib import Path

import pyarrow.parquet as pq
from pipeline_core.extractor import Extractor

logger = logging.getLogger(__name__)

GAS_DATASET = "eia_gas_price"
ELECTRICITY_DATASET = "eia_electricity_price"
PARTITION_KEY = "year_month"


def clean_silver_file(base_dir: str, dataset: str, year_month: str) -> Path:
    return Path(base_dir) / dataset / f"{PARTITION_KEY}={year_month}" / f"{dataset}.parquet"


def _read(base_dir: str, dataset: str, year_month: str, dag_id: str) -> list[dict]:
    path = clean_silver_file(base_dir, dataset, year_month)
    if not path.is_file():
        raise FileNotFoundError(
            f"{dataset} CLEAN Silver 가 없습니다: {path} — {dag_id} 을 먼저 돌리세요."
        )
    # `pq.read_table` 은 경로의 `year_month=` 를 컬럼으로 덧붙입니다. 파일에 실제로 쓰인
    # 것만 봐야 하므로 ParquetFile 로 직접 읽습니다.
    rows = pq.ParquetFile(path).read().to_pylist()
    if not rows:
        raise ValueError(f"{dataset} CLEAN Silver 가 비어 있습니다: {path}")
    logger.info("clean_silver_extract done path=%s rows=%d", path, len(rows))
    return rows


class EiaFuelPriceCleanExtractor(Extractor):
    """휘발유·전력 CLEAN Silver 를 함께 읽습니다."""

    def __init__(self, base_dir: str, year_month: str):
        self._base_dir = base_dir
        self._year_month = year_month
        self.name = f"eia_fuel_price_clean:{base_dir}:{year_month}"

    def extract(self) -> dict:
        return {
            "gas_rows": _read(
                self._base_dir, GAS_DATASET, self._year_month,
                "eia_gas_price_bronze_to_silver_pipeline",
            ),
            "electricity_rows": _read(
                self._base_dir, ELECTRICITY_DATASET, self._year_month,
                "eia_electricity_price_bronze_to_silver_pipeline",
            ),
        }
