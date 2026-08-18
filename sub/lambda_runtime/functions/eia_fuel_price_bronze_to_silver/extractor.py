"""EIA Bronze 원본 두 개를 읽습니다.

각 데이터셋에서 **가장 최근 수집분**을 씁니다.

대상 월 이하로 제한하지 않는 이유
------------------------------
전력 통계는 약 3개월 늦게 공개됩니다. M월 값은 M+3월쯤 받은 파일에만 들어 있어서,
"대상 월 이하 최신" 으로 고르면 구조적으로 그 달이 없는 파일을 집습니다.

최신을 쓰면 값도 더 정확합니다. EIA 는 최근 약 17개월을 `Preliminary` 로 두고 나중에
`Final` 로 확정하므로, 새 파일일수록 확정분이 많습니다. 대신 나중에 다시 만들면 숫자가
달라질 수 있어서, 어느 수집분을 썼는지 함께 돌려주고 Silver 에 남깁니다.
"""

import logging
from datetime import date
from pathlib import Path

from pipeline_core.extractor import Extractor

from shared.lambda_runtime.common import eia_fuel_price_layout as layout

logger = logging.getLogger(__name__)


def _read_newest(base_dir: str, dataset: str, file_name: str) -> tuple[bytes, date]:
    collected_date, partition = layout.newest_bronze_partition(base_dir, dataset)
    path = partition / file_name
    if not path.is_file():
        raise FileNotFoundError(f"EIA Bronze 파일이 없습니다: {path}")
    body = path.read_bytes()
    if not body:
        raise ValueError(f"EIA Bronze 파일이 비어 있습니다: {path}")
    logger.info("bronze_extract done path=%s bytes=%d", path, len(body))
    return body, collected_date


class EiaFuelPriceBronzeExtractor(Extractor):
    """휘발유·전력 원본 bytes 를 함께 읽습니다."""

    def __init__(self, base_dir: str, year_month: str):
        self._base_dir = base_dir
        self._year_month = year_month
        self.name = f"eia_fuel_price_bronze:{base_dir}:{year_month}"

    def extract(self) -> dict:
        gas_body, gas_collected = _read_newest(
            self._base_dir, layout.GAS_DATASET, layout.GAS_FILE_NAME
        )
        electricity_body, electricity_collected = _read_newest(
            self._base_dir, layout.ELECTRICITY_DATASET, layout.ELECTRICITY_FILE_NAME
        )

        # 두 수집분의 날짜가 다를 수 있습니다(각자 다른 DAG 가 받으니). 계보로 남기는
        # 값은 **더 이른 쪽**입니다 — 결과가 반영하는 정보의 하한이라서요.
        collected_date = min(gas_collected, electricity_collected)
        if gas_collected != electricity_collected:
            logger.info(
                "수집분 날짜가 다릅니다: 휘발유=%s 전력=%s → 계보는 %s",
                gas_collected, electricity_collected, collected_date,
            )

        return {
            "gas_body": gas_body,
            "electricity_body": electricity_body,
            "bronze_collected_date": collected_date,
        }
