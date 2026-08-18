"""같은 디렉터리의 고유 임시 파일을 완성한 뒤 최종 경로를 교체합니다."""

from collections.abc import Callable
from pathlib import Path
from uuid import uuid4


def atomic_write(path: Path, writer: Callable[[Path], object]) -> None:
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        writer(temporary)
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)
