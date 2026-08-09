"""Oil Bronze의 날짜별 Hive 파티션을 월 단위로 읽습니다."""

import json
import logging
import re
from pathlib import Path

logger = logging.getLogger(__name__)

DATASET = "oil"
MONTH_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")


def extract(base_dir: str, collected_month: str) -> list[dict]:
    """해당 월에 속한 collected_date 파티션의 JSON을 읽습니다."""
    if not MONTH_RE.fullmatch(collected_month):
        raise ValueError("collected_month는 YYYY-MM 형식이어야 합니다.")

    dataset_path = Path(base_dir) / DATASET
    partition_pattern = f"collected_date={collected_month}-*"
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

    logger.info(
        "Gas Price Bronze 로드 완료: %s (%d건)", collected_month, len(rows)
    )
    return rows
