"""월별 원천 API에서 받은 단일 Bronze 수집본을 검증합니다."""

import re
from datetime import datetime, timezone
from pathlib import Path

from shared.airflow.common.validation import (
    S3Location,
    location_size,
    parquet_file,
    parse_handler_result,
    parse_location,
    parse_year_month,
    require_file,
)
from main.airflow.common.assets import join_segments, service_area_segment
from shared.common.s3_reader import list_keys


BRONZE_DATA_FILE_NAME = "data.parquet"
COLLECTED_AT_DIR_PATTERN = re.compile(r"^collected_at=(\d{8}T\d{12}Z)$")
TIMESTAMP_FILE_PATTERN = re.compile(r"^\d{8}T\d{12}Z\.parquet$")
SOURCE_COLLECTED_AT_PATTERN = re.compile(r"^source_collected_at=(\d{8}T\d{12}Z)$")
SILVER_PART_PATTERN = re.compile(r"^part-.+\.parquet$")
SILVER_SUCCESS_FILE = "_SUCCESS"


def collected_at_token(value: str) -> str:
    try:
        timestamp = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ").replace(
            tzinfo=timezone.utc
        )
    except (TypeError, ValueError) as exc:
        raise ValueError("collected_at이 UTC 수집 시각 형식이 아닙니다") from exc
    return f"{timestamp:%Y%m%dT%H%M%S%fZ}"


def bronze_collection_token(path) -> str | None:
    if TIMESTAMP_FILE_PATTERN.fullmatch(path.name):
        return Path(path.name).stem
    if path.name != BRONZE_DATA_FILE_NAME:
        return None
    match = COLLECTED_AT_DIR_PATTERN.fullmatch(path.parent.name)
    return match.group(1) if match else None


def bronze_partition(path):
    return path.parent.parent if path.name == BRONZE_DATA_FILE_NAME else path.parent


def _is_silver_data_file(file_name: str) -> bool:
    return file_name == "data.parquet" or bool(SILVER_PART_PATTERN.fullmatch(file_name))


def latest_local_silver_version(partition: Path) -> Path | None:
    candidates: list[tuple[str, Path]] = []
    for version_dir in partition.glob("source_collected_at=*"):
        match = SOURCE_COLLECTED_AT_PATTERN.fullmatch(version_dir.name)
        if (
            match
            and version_dir.is_dir()
            and (version_dir / SILVER_SUCCESS_FILE).is_file()
            and any(
                data_file.is_file() and _is_silver_data_file(data_file.name)
                for data_file in version_dir.glob("*.parquet")
            )
        ):
            candidates.append((match.group(1), version_dir))
    return max(candidates, default=(None, None), key=lambda item: item[0])[1]


def silver_version_path(
    base_dir: str | Path,
    result: dict,
    service_area: str,
) -> Path | S3Location:
    """Bronze 수집 시각을 자연 키로 쓰는 지역별 Silver 버전 디렉터리입니다."""
    parsed = parse_handler_result(result, expected_locations=1)
    year_month = parse_year_month(result.get("year_month"), field="year_month")
    collected_at = result.get("collected_at")
    token = (
        collected_at_token(collected_at)
        if collected_at is not None
        else bronze_collection_token(parsed.locations[0])
    )
    if token is None:
        raise ValueError(f"Bronze 경로에 수집 시각이 없습니다: {parsed.locations[0]}")
    version_dir = f"source_collected_at={token}"
    area = service_area_segment(service_area)
    base = parse_location(str(base_dir))
    if isinstance(base, S3Location):
        return S3Location(
            base.bucket,
            join_segments(
                base.key.rstrip("/"), area, f"year_month={year_month}", version_dir
            ),
        )
    if isinstance(parsed.locations[0], S3Location):
        dataset_dir = base.name
        return S3Location(
            parsed.locations[0].bucket,
            join_segments(
                "silver", dataset_dir, area, f"year_month={year_month}", version_dir
            ),
        )
    local = base / area
    return local / f"year_month={year_month}" / version_dir


def silver_part_paths(version: Path | S3Location) -> list[Path | S3Location]:
    """버전 디렉터리 바로 아래의 Spark 호환 part 파일만 반환합니다."""
    if isinstance(version, S3Location):
        prefix = f"{version.key.rstrip('/')}/"
        return [
            S3Location(version.bucket, key)
            for key in list_keys(version.bucket, prefix)
            if SILVER_PART_PATTERN.fullmatch(Path(key).name)
            and "/" not in key.removeprefix(prefix)
        ]
    return sorted(
        path
        for path in Path(version).glob("part-*.parquet")
        if path.is_file() and SILVER_PART_PATTERN.fullmatch(path.name)
    )

def validate_monthly_parquet_bronze(
    result: dict,
    *,
    dataset_dir: str,
    base_dir: str | Path | None = None,
    service_area: str,
) -> tuple[Path | S3Location, str]:
    parsed = parse_handler_result(result, expected_locations=1)
    year_month = parse_year_month(result.get("year_month"), field="year_month")
    path = parsed.locations[0]
    try:
        require_file(path)
    except FileNotFoundError:
        raise ValueError(f"Bronze 원본 파일이 없습니다: {path}")
    partition = bronze_partition(path)
    area = service_area_segment(service_area)
    dataset_root = partition.parent.parent
    if (
        partition.name != f"year_month={year_month}"
        or dataset_root.name != dataset_dir
        or partition.parent.name != area
    ):
        raise ValueError(f"Bronze 원본 경로가 월 파티션 계약과 다릅니다: {path}")
    collected_at = result.get("collected_at")
    try:
        expected_token = collected_at_token(collected_at)
    except ValueError as exc:
        raise ValueError("Bronze collected_at이 UTC 수집 시각 형식이 아닙니다") from exc
    if bronze_collection_token(path) != expected_token:
        raise ValueError(
            f"Bronze 경로의 수집 시각이 collected_at과 다릅니다: {path}"
        )
    if base_dir is not None and isinstance(path, Path):
        expected_root = Path(base_dir) / dataset_dir
        expected_root /= area
        expected_partition = expected_root / f"year_month={year_month}"
        if partition.resolve() != expected_partition.resolve():
            raise ValueError(
                f"Bronze 경로가 base_dir layout과 다릅니다: {partition}"
            )
    if location_size(path) != result.get("file_size_bytes"):
        raise ValueError(f"Bronze 원본 파일 크기가 수집 결과와 다릅니다: {path}")
    if parquet_file(path).metadata.num_rows != parsed.row_count:
        raise ValueError(f"Bronze 원본 행 수가 수집 결과와 다릅니다: {path}")
    return path, year_month
