"""월별 API Bronze의 신·구 경로 계약을 해석합니다."""

import re
from datetime import datetime, timezone
from pathlib import Path


BRONZE_DATA_FILE_NAME = "data.parquet"
COLLECTED_AT_DIR_PATTERN = re.compile(r"^collected_at=(\d{8}T\d{12}Z)$")
TIMESTAMP_FILE_PATTERN = re.compile(r"^\d{8}T\d{12}Z\.parquet$")


def collected_at_token(value: str) -> str:
    try:
        timestamp = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ").replace(
            tzinfo=timezone.utc
        )
    except (TypeError, ValueError) as exc:
        raise ValueError("collected_at이 UTC 수집 시각 형식이 아닙니다") from exc
    return f"{timestamp:%Y%m%dT%H%M%S%fZ}"


def collected_at_from_token(token: str) -> str:
    try:
        timestamp = datetime.strptime(token, "%Y%m%dT%H%M%S%fZ").replace(
            tzinfo=timezone.utc
        )
    except (TypeError, ValueError) as exc:
        raise ValueError("Bronze 경로의 수집 시각이 올바르지 않습니다") from exc
    return timestamp.isoformat(timespec="microseconds").replace("+00:00", "Z")


def bronze_collection_token(path) -> str | None:
    """신규 디렉터리와 기존 flat 파일에서 같은 수집 시각 토큰을 꺼냅니다."""
    if TIMESTAMP_FILE_PATTERN.fullmatch(path.name):
        return Path(path.name).stem
    if path.name != BRONZE_DATA_FILE_NAME:
        return None
    match = COLLECTED_AT_DIR_PATTERN.fullmatch(path.parent.name)
    return match.group(1) if match else None


def bronze_partition(path):
    """Bronze data 객체가 속한 `year_month=...` 경로를 반환합니다."""
    return path.parent.parent if path.name == BRONZE_DATA_FILE_NAME else path.parent
