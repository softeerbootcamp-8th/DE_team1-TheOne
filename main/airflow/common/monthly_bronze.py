"""월별 원천 API에서 받은 단일 Bronze 수집본을 검증합니다."""

import re
from datetime import datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq

from shared.airflow.common.validation import parse_handler_result, parse_year_month


TIMESTAMP_FILE_PATTERN = re.compile(r"^\d{8}T\d{12}Z\.parquet$")


def silver_version_path(base_dir: str | Path, result: dict) -> Path:
    """Bronze 수집 시각 파일명을 그대로 쓰는 Silver 버전 경로입니다."""
    parsed = parse_handler_result(result, expected_locations=1)
    year_month = parse_year_month(result.get("year_month"), field="year_month")
    file_name = parsed.locations[0].name
    if not TIMESTAMP_FILE_PATTERN.fullmatch(file_name):
        raise ValueError(f"Bronze 파일명이 수집 시각 형식이 아닙니다: {file_name}")
    return Path(base_dir) / f"year_month={year_month}" / file_name


def validate_monthly_parquet_bronze(
    result: dict,
    *,
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
    ):
        raise ValueError(f"Bronze 원본 경로가 월 파티션 계약과 다릅니다: {path}")
    collected_at = result.get("collected_at")
    if not isinstance(collected_at, str):
        raise ValueError("Bronze 수집 결과에 collected_at이 없습니다")
    try:
        timestamp = datetime.strptime(
            collected_at, "%Y-%m-%dT%H:%M:%S.%fZ"
        ).replace(tzinfo=timezone.utc)
    except ValueError as exc:
        raise ValueError("Bronze collected_at이 UTC 수집 시각 형식이 아닙니다") from exc
    if path.name != f"{timestamp:%Y%m%dT%H%M%S%fZ}.parquet":
        raise ValueError(
            f"Bronze 파일명이 collected_at과 다릅니다: {path.name}"
        )
    if base_dir is not None:
        expected_partition = Path(base_dir) / dataset_dir / f"year_month={year_month}"
        if path.parent.resolve() != expected_partition.resolve():
            raise ValueError(
                f"Bronze 경로가 base_dir layout과 다릅니다: {path.parent}"
            )
    if path.stat().st_size != result.get("file_size_bytes"):
        raise ValueError(f"Bronze 원본 파일 크기가 수집 결과와 다릅니다: {path}")
    if pq.ParquetFile(path).metadata.num_rows != parsed.row_count:
        raise ValueError(f"Bronze 원본 행 수가 수집 결과와 다릅니다: {path}")
    return path, year_month
