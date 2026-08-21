"""Dockerfile COPY 와 워크플로 필터 대조 검사의 시나리오.

1. COPY 원본을 덮는 패턴이면 통과
2. COPY 에 있는데 필터에 없으면 그 경로를 이름으로 지적
3. 상위 패턴(`sub/**`)이 하위 COPY(`sub/generators`)를 덮음
4. 형제 패턴(`sub/airflow/**`)은 하위 COPY(`sub/generators`)를 덮지 않음  ← #739 의 실제 구멍
5. `--flags` 와 목적지는 원본으로 세지 않음
"""

import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from check_image_filters import copy_sources, missing


def test_COPY_원본만_뽑고_플래그와_목적지는_제외한다(tmp_path):
    dockerfile = tmp_path / "Dockerfile"
    dockerfile.write_text(
        "FROM scratch\n"
        "COPY --chown=airflow:root sub/airflow/ /opt/x/sub/airflow/\n"
        "COPY a.py b.py /opt/x/\n"
        "RUN echo COPY not-a-copy\n"
    )

    assert copy_sources(dockerfile) == ["sub/airflow", "a.py", "b.py"]


@pytest.mark.parametrize(
    ("pattern", "source"),
    [
        ("sub/generators/**", "sub/generators"),
        ("sub/**", "sub/generators"),
        ("sub/config.py", "sub/config.py"),
        # 필터가 하위를 지목해도 COPY 가 상위면 그 하위 변경은 잡힙니다.
        ("sub/generators/**", "sub"),
    ],
)
def test_덮는_패턴은_통과한다(pattern, source):
    assert missing([source], [pattern]) == []


def test_형제_패턴은_덮지_않는다():
    """#739 의 실제 구멍 — 필터가 sub/airflow/** 뿐인데 COPY 는 sub/generators."""
    assert missing(["sub/generators"], ["sub/airflow/**"]) == ["sub/generators"]


def test_없는_경로를_이름으로_알려준다():
    gaps = missing(["sub/generators", "shared/common"], ["sub/airflow/**"])

    assert gaps == ["shared/common", "sub/generators"]


def test_실제_저장소가_통과한다():
    """규칙을 고쳤으면 실제 Dockerfile·워크플로도 함께 맞춰야 합니다."""
    root = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [sys.executable, str(root / ".github/scripts/check_image_filters.py")],
        capture_output=True, text=True,
    )

    assert result.returncode == 0, result.stdout + result.stderr
