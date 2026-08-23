"""월별 Silver 공개 버전의 경로 계약과 최신 버전 선택."""

import re
from pathlib import Path

from shared.common.monthly_bronze import TIMESTAMP_FILE_PATTERN


SOURCE_COLLECTED_AT_PATTERN = re.compile(
    r"^source_collected_at=(\d{8}T\d{12}Z)$"
)
SILVER_PART_PATTERN = re.compile(r"^part-.+\.parquet$")
SILVER_DATA_FILE = "data.parquet"
SILVER_SUCCESS_FILE = "_SUCCESS"


def is_silver_data_file(file_name: str) -> bool:
    return file_name == SILVER_DATA_FILE or bool(SILVER_PART_PATTERN.fullmatch(file_name))


def latest_local_silver_version(partition: Path) -> Path | None:
    """완료된 새 디렉터리와 구 timestamp 파일 중 최신 버전을 고릅니다."""
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
                data_file.is_file() and is_silver_data_file(data_file.name)
                for data_file in version_dir.glob("*.parquet")
            )
        ):
            candidates.append((match.group(1), version_dir))
    return max(candidates, default=(None, None), key=lambda item: item[0])[1]


def latest_s3_silver_version(keys: list[str], partition_prefix: str) -> str | None:
    """S3 key 목록에서 완료된 새 prefix 또는 구 timestamp 객체를 고릅니다."""
    prefix = f"{partition_prefix.rstrip('/')}/"
    candidates: list[tuple[str, str]] = []
    completed: set[str] = set()
    parts: set[str] = set()

    for key in keys:
        relative = key.removeprefix(prefix)
        if relative == key:
            continue
        if "/" not in relative and TIMESTAMP_FILE_PATTERN.fullmatch(relative):
            candidates.append((Path(relative).stem, key))
            continue
        components = relative.split("/")
        if len(components) != 2:
            continue
        version_name, file_name = components
        if not SOURCE_COLLECTED_AT_PATTERN.fullmatch(version_name):
            continue
        if file_name == SILVER_SUCCESS_FILE:
            completed.add(version_name)
        elif is_silver_data_file(file_name):
            parts.add(version_name)

    for version_name in completed & parts:
        token = SOURCE_COLLECTED_AT_PATTERN.fullmatch(version_name).group(1)
        candidates.append((token, f"{prefix}{version_name}"))
    return max(candidates, default=(None, None), key=lambda item: item[0])[1]
