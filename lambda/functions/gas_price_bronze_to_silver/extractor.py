"""Oil Bronze의 수집일 Hive 파티션을 읽습니다.

정기 실행은 그날 수집분(`collected_date`) 하나만 읽습니다. 월 전체를 다시 읽으면
과거 파티션의 깨진 파일 하나가 그 달 내내 배치를 막기 때문입니다.
과거 보정은 `collected_month`를 지정한 수동 백필로 처리합니다.
"""

import json
import logging
import re
from pathlib import Path

from pipeline_core.extractor import Extractor

from ..common import gas_price_layout as layout

logger = logging.getLogger(__name__)

MONTH_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")
DATE_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$")


class GasPriceBronzeExtractor(Extractor):
    """대상 수집일(또는 수집월) 파티션의 JSON을 읽습니다."""

    name = "gas_price_bronze"

    def __init__(
        self,
        base_dir: str,
        collected_date: str | None = None,
        collected_month: str | None = None,
    ):
        if bool(collected_date) == bool(collected_month):
            raise ValueError(
                "collected_date와 collected_month 중 정확히 하나만 지정해야 합니다."
            )
        if collected_date and not DATE_RE.fullmatch(collected_date):
            raise ValueError("collected_date는 YYYY-MM-DD 형식이어야 합니다.")
        if collected_month and not MONTH_RE.fullmatch(collected_month):
            raise ValueError("collected_month는 YYYY-MM 형식이어야 합니다.")

        self._base_dir = base_dir
        self.target = collected_date or collected_month
        # 하루면 파티션 하나, 한 달이면 그 달의 파티션 전부.
        self._partition_pattern = layout.bronze_partition(
            base_dir, collected_date or f"{collected_month}-*"
        ).name

    def extract(self) -> list[dict]:
        dataset_path = layout.dataset_path(self._base_dir)
        partition_pattern = self._partition_pattern
        paths = sorted(
            path
            for partition in dataset_path.glob(partition_pattern)
            for path in partition.rglob("*.json")
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

        logger.info("bronze_extract done target=%s rows=%d", self.target, len(rows))
        return rows
