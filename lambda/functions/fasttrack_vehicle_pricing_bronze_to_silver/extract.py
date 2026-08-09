"""실행일의 리스 업체 보유 차량 대장 Bronze 스냅샷을 읽습니다."""

import logging
import re
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

DATASET = "fasttrack_vehicle_pricing"
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def extract(base_dir: str, collected_date: str) -> list[dict]:
    """해당 collected_date 파티션에서 업체별 가장 최신 Parquet 을 읽어 합칩니다.

    Bronze 는 collected_date 아래에 vendor 파티션이 한 단계 더 있습니다.
    같은 날 여러 번 수집하면 파일이 쌓이므로 업체별로 최신 것만 씁니다.
    """
    if not DATE_RE.fullmatch(collected_date):
        raise ValueError("collected_date는 YYYY-MM-DD 형식이어야 합니다.")
    try:
        date.fromisoformat(collected_date)
    except ValueError as exc:
        raise ValueError("유효하지 않은 collected_date입니다.") from exc

    partition = Path(base_dir) / DATASET / f"collected_date={collected_date}"
    vendor_dirs = sorted(d for d in partition.glob("vendor=*") if d.is_dir())
    if not vendor_dirs:
        raise FileNotFoundError(f"Bronze 파티션이 없습니다: {partition}")

    rows: list[dict] = []
    for vendor_dir in vendor_dirs:
        paths = sorted(vendor_dir.glob("*.parquet"))
        if not paths:
            raise FileNotFoundError(f"Bronze Parquet 파일이 없습니다: {vendor_dir}")

        path = paths[-1]
        try:
            table = pq.ParquetFile(path).read()
        except (OSError, pa.ArrowInvalid) as exc:
            raise RuntimeError(f"Bronze Parquet을 읽지 못했습니다: {path}") from exc
        if not table.num_rows:
            raise RuntimeError(f"Bronze Parquet이 비어 있습니다: {path}")

        # vendor 는 파티션 키라서 파일 안에 없습니다. 디렉터리명에서 되살립니다.
        vendor = vendor_dir.name.removeprefix("vendor=")
        rows += [
            {**row, "vendor": vendor, "bronze_path": str(path)}
            for row in table.to_pylist()
        ]
        logger.info("차량 대장 Bronze 로드: %s (%d건)", path, table.num_rows)

    logger.info("차량 대장 Bronze 로드 완료: 업체 %d곳 %d건", len(vendor_dirs), len(rows))
    return rows
