"""EIA Bronze 원본 두 개를 읽습니다.

각 데이터셋에서 **대상 월 이하 최신 수집분**을 고릅니다. 이력 파일이라 나중에 받은
것에도 그 달 값이 있지만, `as_of` 를 넘기지 않는 쪽을 우선하는 것은 과거 달을 다시
만들 때 그 사이 개정된 값이 섞여 결과가 달라지는 것을 막기 위해서입니다.
"""

import logging
from datetime import date
from pathlib import Path

from pipeline_core.extractor import Extractor

from ..common import eia_fuel_price_layout as layout

logger = logging.getLogger(__name__)


def _read_latest(base_dir: str, dataset: str, file_name: str, as_of: date) -> bytes:
    partition = layout.latest_bronze_partition(base_dir, dataset, as_of)
    path = partition / file_name
    if not path.is_file():
        raise FileNotFoundError(f"EIA Bronze 파일이 없습니다: {path}")
    body = path.read_bytes()
    if not body:
        raise ValueError(f"EIA Bronze 파일이 비어 있습니다: {path}")
    logger.info("bronze_extract done path=%s bytes=%d", path, len(body))
    return body


class EiaFuelPriceBronzeExtractor(Extractor):
    """휘발유·전력 원본 bytes 를 함께 읽습니다."""

    def __init__(self, base_dir: str, year_month: str):
        self._base_dir = base_dir
        self._year_month = year_month
        self.name = f"eia_fuel_price_bronze:{base_dir}:{year_month}"

    def extract(self) -> dict:
        year, month = (int(part) for part in self._year_month.split("-"))
        # 대상 월 말일 기준으로 "그 달 이하 최신" 을 찾습니다. 말일을 정확히 구하지 않고
        # 28일을 쓰는 이유는 같은 달 안이면 어느 날이든 같은 파티션이 골라지기 때문입니다.
        as_of = date(year, month, 28)

        return {
            "gas_body": _read_latest(
                self._base_dir, layout.GAS_DATASET, layout.GAS_FILE_NAME, as_of
            ),
            "electricity_body": _read_latest(
                self._base_dir, layout.ELECTRICITY_DATASET, layout.ELECTRICITY_FILE_NAME, as_of
            ),
        }
