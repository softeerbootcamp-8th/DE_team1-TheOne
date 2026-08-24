"""Bronze/Silver 파티션 공개 여부를 나타내는 `_SUCCESS` 경로 규칙."""

from collections.abc import Container
from pathlib import Path, PurePosixPath


SUCCESS_FILE = "_SUCCESS"


def marker_path(directory: str | Path) -> Path:
    return Path(directory) / SUCCESS_FILE


def marker_key(prefix: str) -> str:
    return f"{prefix.rstrip('/')}/{SUCCESS_FILE}"


def data_path_is_complete(path: Path) -> bool:
    return path.is_file() and marker_path(path.parent).is_file()


def data_key_is_complete(key: str, keys: Container[str]) -> bool:
    parent = str(PurePosixPath(key).parent)
    return marker_key(parent) in keys
