"""월별 원천 API에서 받은 단일 Bronze 수집본을 검증합니다."""

import re
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal
from uuid import uuid4

import boto3

from shared.airflow.common.validation import (
    S3Location,
    location_size,
    parquet_file,
    parse_handler_result,
    parse_location,
    parse_year_month,
    require_file,
)
from shared.common.s3_reader import list_keys
from shared.common.service_area_path import join_segments, service_area_segment


BRONZE_DATA_FILE_NAME = "data.parquet"
COLLECTED_AT_DIR_PATTERN = re.compile(r"^collected_at=(\d{8}T\d{12}Z)$")
TIMESTAMP_FILE_PATTERN = re.compile(r"^\d{8}T\d{12}Z\.parquet$")
SOURCE_COLLECTED_AT_PATTERN = re.compile(r"^source_collected_at=(\d{8}T\d{12}Z)$")
SILVER_PART_PATTERN = re.compile(r"^part-.+\.parquet$")
SILVER_SUCCESS_FILE = "_SUCCESS"
# 구 단일 파일 staging을 읽기 호환에서 공개 버전으로 세지 않기 위한 패턴입니다.
STAGED_FILE_PATTERN = re.compile(r"^\d{8}T\d{12}Z\.staged\.parquet$")


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
    candidates: list[tuple[str, Path]] = [
        (path.stem, path)
        for path in partition.glob("*.parquet")
        if path.is_file() and TIMESTAMP_FILE_PATTERN.fullmatch(path.name)
    ]
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
    service_area: str | None = None,
) -> Path | S3Location:
    """Bronze 수집 시각을 자연 키로 쓰는 Silver 버전 디렉터리입니다.

    `service_area=None` 이면 지역 계층 없이 **지금과 완전히 같은 경로**를 만듭니다.
    API 3종(monthly_taxi_trip / driver_vehicle_monthly_snapshot /
    lease_vehicle_inventory)이 이 함수를 공유하므로, 데이터셋별로 하나씩 지역을
    켜려면 이 기본값이 필요합니다(#840~#842). 삽입 위치 규칙은
    `shared.common.service_area_path` 가 단독으로 정의합니다.
    """
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
    local = base / area if area else base
    return local / f"year_month={year_month}" / version_dir


def staged_silver_version_path(
    base_dir: str | Path,
    result: dict,
    service_area: str | None = None,
) -> Path | S3Location:
    """`silver_version_path`와 격리된 검증 전 디렉터리입니다."""
    final = silver_version_path(base_dir, result, service_area)
    if isinstance(final, S3Location):
        parent = final.key.rsplit("/", 1)[0]
        return S3Location(final.bucket, f"{parent}/.staging/{final.name}")
    return final.parent / ".staging" / final.name


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


def _matches_layout(file_name: str, layout: Literal["spark_parts", "single_data"]) -> bool:
    if layout == "spark_parts":
        return bool(SILVER_PART_PATTERN.fullmatch(file_name))
    return file_name == "data.parquet"


def silver_data_paths(
    version: Path | S3Location,
    layout: Literal["spark_parts", "single_data"],
) -> list[Path | S3Location]:
    """버전 디렉터리 바로 아래에서 지정 레이아웃의 Parquet만 반환합니다."""
    if isinstance(version, S3Location):
        prefix = f"{version.key.rstrip('/')}/"
        return [
            S3Location(version.bucket, key)
            for key in list_keys(version.bucket, prefix)
            if _matches_layout(Path(key).name, layout)
            and "/" not in key.removeprefix(prefix)
        ]
    return sorted(
        path
        for path in Path(version).glob("*.parquet")
        if path.is_file() and _matches_layout(path.name, layout)
    )


def commit_staged_silver(
    staged: Path | S3Location,
    final: Path | S3Location,
    *,
    layout: Literal["spark_parts", "single_data"],
) -> None:
    """지정 레이아웃으로 검증된 파일만 옮기고 `_SUCCESS`를 마지막에 씁니다."""
    if isinstance(final, S3Location):
        if not isinstance(staged, S3Location):
            raise TypeError("staged와 final의 위치 종류가 다릅니다")
        client = boto3.client("s3")
        staged_prefix = f"{staged.key.rstrip('/')}/"
        final_prefix = f"{final.key.rstrip('/')}/"
        staged_keys = list_keys(staged.bucket, staged_prefix)
        parquet_keys = [
            key
            for key in staged_keys
            if key.endswith(".parquet")
            and "/" not in key.removeprefix(staged_prefix)
        ]
        data_keys = [
            key for key in parquet_keys if _matches_layout(Path(key).name, layout)
        ]
        if (
            not data_keys
            or len(data_keys) != len(parquet_keys)
            or (layout == "single_data" and len(data_keys) != 1)
        ):
            raise ValueError(f"Silver staging 파일이 {layout} 계약과 다릅니다: {staged}")
        final_keys = list_keys(final.bucket, final_prefix)
        marker = f"{final_prefix}{SILVER_SUCCESS_FILE}"
        if marker in final_keys:
            client.delete_object(Bucket=final.bucket, Key=marker)
        for key in final_keys:
            if key != marker:
                client.delete_object(Bucket=final.bucket, Key=key)
        for source_key in data_keys:
            target_key = f"{final_prefix}{Path(source_key).name}"
            client.copy(
                {"Bucket": staged.bucket, "Key": source_key},
                final.bucket,
                target_key,
            )
        client.put_object(Bucket=final.bucket, Key=marker, Body=b"")
        for key in staged_keys:
            client.delete_object(Bucket=staged.bucket, Key=key)
        return
    if isinstance(staged, S3Location):
        raise TypeError("staged와 final의 위치 종류가 다릅니다")
    staged_path, final_path = Path(staged), Path(final)
    parquet_files = sorted(path for path in staged_path.glob("*.parquet") if path.is_file())
    data_files = silver_data_paths(staged_path, layout)
    if (
        not data_files
        or len(data_files) != len(parquet_files)
        or (layout == "single_data" and len(data_files) != 1)
    ):
        raise ValueError(
            f"Silver staging 파일이 {layout} 계약과 다릅니다: {staged_path}"
        )
    (staged_path / SILVER_SUCCESS_FILE).touch()
    final_path.parent.mkdir(parents=True, exist_ok=True)
    backup = final_path.with_name(f".{final_path.name}.backup-{uuid4().hex}")
    if final_path.exists():
        final_path.replace(backup)
    try:
        staged_path.replace(final_path)
    except Exception:
        if backup.exists():
            backup.replace(final_path)
        raise
    finally:
        shutil.rmtree(backup, ignore_errors=True)


def validate_monthly_parquet_bronze(
    result: dict,
    *,
    dataset_dir: str,
    base_dir: str | Path | None = None,
    service_area: str | None = None,
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
    if (
        partition.name != f"year_month={year_month}"
        or (area and partition.parent.name != area)
        or (area and partition.parent.parent.name != dataset_dir)
        or (not area and partition.parent.name != dataset_dir)
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
        dataset_root = Path(base_dir) / dataset_dir
        expected_partition = (
            (dataset_root / area if area else dataset_root)
            / f"year_month={year_month}"
        )
        if partition.resolve() != expected_partition.resolve():
            raise ValueError(
                f"Bronze 경로가 base_dir layout과 다릅니다: {partition}"
            )
    if location_size(path) != result.get("file_size_bytes"):
        raise ValueError(f"Bronze 원본 파일 크기가 수집 결과와 다릅니다: {path}")
    if parquet_file(path).metadata.num_rows != parsed.row_count:
        raise ValueError(f"Bronze 원본 행 수가 수집 결과와 다릅니다: {path}")
    return path, year_month
