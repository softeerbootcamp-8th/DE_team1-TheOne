"""Bronze/Silver 파티션 공개 여부를 나타내는 `_SUCCESS` 경로 규칙."""

from collections.abc import Container
from pathlib import Path, PurePosixPath


SUCCESS_FILE = "_SUCCESS"
QUARANTINE_FILE = "_QUARANTINED.json"
# 변환이 몇 건을 걸렀는지 남기는 sidecar. 계산한 쪽(Spark)과 센 쪽(Airflow)이
# 다른 프로세스라, 이 파일이 없으면 둘의 숫자를 맞대볼 자리가 없다.
RECON_FILE = "_RECON.json"


def marker_path(directory: str | Path) -> Path:
    return Path(directory) / SUCCESS_FILE


def marker_key(prefix: str) -> str:
    return f"{prefix.rstrip('/')}/{SUCCESS_FILE}"


def quarantine_marker_path(directory: str | Path) -> Path:
    return Path(directory) / QUARANTINE_FILE


def quarantine_marker_key(prefix: str) -> str:
    return f"{prefix.rstrip('/')}/{QUARANTINE_FILE}"


def recon_path(directory: str | Path) -> Path:
    return Path(directory) / RECON_FILE


def recon_key(prefix: str) -> str:
    return f"{prefix.rstrip('/')}/{RECON_FILE}"


def data_path_is_complete(path: Path) -> bool:
    return path.is_file() and marker_path(path.parent).is_file()


def data_key_is_complete(key: str, keys: Container[str]) -> bool:
    parent = str(PurePosixPath(key).parent)
    return marker_key(parent) in keys
