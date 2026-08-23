"""Spark Gold job이 읽는 월별 Silver 버전 계약."""

import re
from pathlib import Path


TIMESTAMP_FILE_PATTERN = re.compile(r"^\d{8}T\d{12}Z\.parquet$")
SOURCE_COLLECTED_AT_PATTERN = re.compile(r"^source_collected_at=(\d{8}T\d{12}Z)$")
SILVER_PART_PATTERN = re.compile(r"^part-.+\.parquet$")
SILVER_SUCCESS_FILE = "_SUCCESS"


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


def latest_s3_silver_version(keys: list[str], partition_prefix: str) -> str | None:
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
        elif _is_silver_data_file(file_name):
            parts.add(version_name)

    for version_name in completed & parts:
        token = SOURCE_COLLECTED_AT_PATTERN.fullmatch(version_name).group(1)
        candidates.append((token, f"{prefix}{version_name}"))
    return max(candidates, default=(None, None), key=lambda item: item[0])[1]
