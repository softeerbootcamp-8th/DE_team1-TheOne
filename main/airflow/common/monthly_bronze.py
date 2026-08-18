"""월별 제공 데이터의 단일 Bronze 파일을 검증합니다.

같은 원천 API에서 데이터셋을 하나씩 받아 오므로, 수집 태스크들이 공유하는
기본 주소와 Bronze 루트도 여기서 한 번만 정합니다.
"""

import hashlib
import json
import os
from pathlib import Path

import pyarrow.parquet as pq

from shared.airflow.common.project_paths import PROJECT_ROOT
from shared.airflow.common.validation import parse_handler_result, parse_year_month


DEFAULT_API_BASE_URL = "http://host.docker.internal:8091"
DEFAULT_BRONZE_DIR = os.getenv("BRONZE_DIR", str(PROJECT_ROOT / "data" / "bronze"))


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def validate_synthetic_bronze(
    result: dict,
    *,
    dataset: str,
    dataset_dir: str,
    base_dir: str | Path | None = None,
) -> tuple[Path, str]:
    parsed = parse_handler_result(result, expected_locations=1)
    year_month = parse_year_month(result.get("year_month"), field="year_month")
    path = parsed.locations[0]
    if not path.is_file():
        raise ValueError(f"Bronze 원본 파일이 없습니다: {path}")
    if (
        path.parent.name != f"year_month={year_month}"
        or path.parent.parent.name != dataset_dir
        or path.name != "data.parquet"
    ):
        raise ValueError(f"Bronze 원본 경로가 월 파티션 계약과 다릅니다: {path}")
    if base_dir is not None:
        expected_partition = Path(base_dir) / dataset_dir / f"year_month={year_month}"
        if path.parent.resolve() != expected_partition.resolve():
            raise ValueError(
                f"Bronze 경로가 base_dir layout과 다릅니다: {path.parent}"
            )
    if path.stat().st_size != result.get("file_size_bytes"):
        raise ValueError(f"Bronze 원본 파일 크기가 수집 결과와 다릅니다: {path}")
    if _sha256(path) != result.get("sha256"):
        raise ValueError(f"Bronze 원본 checksum이 수집 결과와 다릅니다: {path}")
    if pq.ParquetFile(path).metadata.num_rows != parsed.row_count:
        raise ValueError(f"Bronze 원본 행 수가 수집 결과와 다릅니다: {path}")

    marker_path = Path(str(result.get("marker_location", "")))
    if not marker_path.is_file() or marker_path.with_suffix(".parquet") != path:
        raise ValueError(f"Bronze marker가 없습니다: {marker_path}")
    marker = json.loads(marker_path.read_text(encoding="utf-8"))
    expected = {
        "year_month": year_month,
        "dataset": dataset,
        "row_count": parsed.row_count,
        "sha256": result["sha256"],
    }
    if any(marker.get(key) != value for key, value in expected.items()):
        raise ValueError("Bronze marker가 수집 결과와 다릅니다")
    return path, year_month
