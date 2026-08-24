"""같은 디렉터리의 고유 임시 파일을 완성한 뒤 최종 경로를 교체합니다."""

from collections.abc import Callable
from pathlib import Path
from uuid import uuid4

from shared.common.success_marker import marker_path, quarantine_marker_path


def atomic_write(path: Path, writer: Callable[[Path], object]) -> None:
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        writer(temporary)
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def invalidate_success_marker(directory: Path) -> None:
    """재처리를 시작하며 이전 성공·격리 종결 상태를 모두 무효화합니다."""
    marker_path(directory).unlink(missing_ok=True)
    quarantine_marker_path(directory).unlink(missing_ok=True)
