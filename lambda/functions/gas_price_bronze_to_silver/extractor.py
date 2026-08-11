"""Gas Price Bronze의 대상 월 일별 JSON을 읽습니다."""

import json
import logging
import re
from pathlib import Path

from pipeline_core.extractor import Extractor

from ..common import gas_price_layout as layout

logger = logging.getLogger(__name__)

MONTH_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")


class GasPriceBronzeExtractor(Extractor):
    """대상 수집월에 저장된 일별 Bronze JSON을 읽습니다."""

    name = "gas_price_bronze"

    def __init__(self, base_dir: str, collected_month: str):
        if not MONTH_RE.fullmatch(collected_month):
            raise ValueError("collected_month는 YYYY-MM 형식이어야 합니다.")

        self._base_dir = base_dir
        self.collected_month = collected_month
        self._partition_pattern = layout.bronze_partition(
            base_dir, f"{collected_month}-*"
        ).name

    def extract(self) -> list[dict]:
        dataset_path = layout.dataset_path(self._base_dir)
        partition_pattern = self._partition_pattern
        paths = sorted(
            path
            for partition in dataset_path.glob(partition_pattern)
            if (path := partition / layout.BRONZE_FILE_NAME).is_file()
        )
        if not paths:
            raise FileNotFoundError(
                f"Bronze JSON 파일이 없습니다: {dataset_path}/{partition_pattern}"
            )

        rows: list[dict] = []
        for path in paths:
            try:
                row = json.loads(path.read_text(encoding="utf-8"))
            except json.JSONDecodeError as exc:
                raise RuntimeError(f"Bronze JSON을 읽지 못했습니다: {path}") from exc
            if not isinstance(row, dict):
                raise RuntimeError(f"Bronze JSON이 객체 형식이 아닙니다: {path}")

            rows.append({**row, "bronze_path": str(path)})

        logger.info(
            "bronze_extract done collected_month=%s rows=%d",
            self.collected_month,
            len(rows),
        )
        return rows
