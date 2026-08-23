"""월별 원천 API에서 받은 단일 Bronze 수집본을 검증합니다."""

import re
from datetime import datetime, timezone
from pathlib import Path

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


TIMESTAMP_FILE_PATTERN = re.compile(r"^\d{8}T\d{12}Z\.parquet$")
# staged_silver_version_path()가 만드는 이름과 짝을 맞춥니다 — 검증 전 파일을
# "이미 존재하는 버전"으로 세지 않으려면 이 패턴으로 걸러내야 합니다(#742).
STAGED_FILE_PATTERN = re.compile(r"^\d{8}T\d{12}Z\.staged\.parquet$")


def silver_version_path(base_dir: str | Path, result: dict) -> Path | S3Location:
    """Bronze 수집 시각 파일명을 그대로 쓰는 Silver 버전 경로입니다."""
    parsed = parse_handler_result(result, expected_locations=1)
    year_month = parse_year_month(result.get("year_month"), field="year_month")
    file_name = parsed.locations[0].name
    if not TIMESTAMP_FILE_PATTERN.fullmatch(file_name):
        raise ValueError(f"Bronze 파일명이 수집 시각 형식이 아닙니다: {file_name}")
    base = parse_location(str(base_dir))
    if isinstance(base, S3Location):
        return S3Location(
            base.bucket,
            f"{base.key.rstrip('/')}/year_month={year_month}/{file_name}",
        )
    if isinstance(parsed.locations[0], S3Location):
        dataset_dir = base.name
        return S3Location(
            parsed.locations[0].bucket,
            f"silver/{dataset_dir}/year_month={year_month}/{file_name}",
        )
    return base / f"year_month={year_month}" / file_name


def staged_silver_version_path(base_dir: str | Path, result: dict) -> Path | S3Location:
    """`silver_version_path`의 검증 전 임시 위치입니다.

    확장자는 그대로 `.parquet`로 남겨 `parquet_file()`의 확장자 검사를 통과시키되,
    `TIMESTAMP_FILE_PATTERN`과는 겹치지 않게 해 Gold의 "최신 버전" 탐색에서 자연히
    제외됩니다 — 적재 태스크가 검증 통과 전에 최종 경로를 먼저 채우는 사고(#742)를
    막습니다.
    """
    final = silver_version_path(base_dir, result)
    staged_name = f"{Path(final.name).stem}.staged{Path(final.name).suffix}"
    if isinstance(final, S3Location):
        parent = final.key.rsplit("/", 1)[0]
        return S3Location(final.bucket, f"{parent}/{staged_name}")
    return final.with_name(staged_name)


def commit_staged_silver(staged: Path | S3Location, final: Path | S3Location) -> None:
    """검증을 통과한 staging 파일만 최종 Silver 버전 경로로 승격합니다."""
    if isinstance(final, S3Location):
        if not isinstance(staged, S3Location):
            raise TypeError("staged와 final의 위치 종류가 다릅니다")
        client = boto3.client("s3")
        client.copy({"Bucket": staged.bucket, "Key": staged.key}, final.bucket, final.key)
        client.delete_object(Bucket=staged.bucket, Key=staged.key)
        return
    if isinstance(staged, S3Location):
        raise TypeError("staged와 final의 위치 종류가 다릅니다")
    Path(final).parent.mkdir(parents=True, exist_ok=True)
    Path(staged).replace(final)


def validate_monthly_parquet_bronze(
    result: dict,
    *,
    dataset_dir: str,
    base_dir: str | Path | None = None,
) -> tuple[Path | S3Location, str]:
    parsed = parse_handler_result(result, expected_locations=1)
    year_month = parse_year_month(result.get("year_month"), field="year_month")
    path = parsed.locations[0]
    try:
        require_file(path)
    except FileNotFoundError:
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
    if base_dir is not None and isinstance(path, Path):
        expected_partition = Path(base_dir) / dataset_dir / f"year_month={year_month}"
        if path.parent.resolve() != expected_partition.resolve():
            raise ValueError(
                f"Bronze 경로가 base_dir layout과 다릅니다: {path.parent}"
            )
    if location_size(path) != result.get("file_size_bytes"):
        raise ValueError(f"Bronze 원본 파일 크기가 수집 결과와 다릅니다: {path}")
    if parquet_file(path).metadata.num_rows != parsed.row_count:
        raise ValueError(f"Bronze 원본 행 수가 수집 결과와 다릅니다: {path}")
    return path, year_month
