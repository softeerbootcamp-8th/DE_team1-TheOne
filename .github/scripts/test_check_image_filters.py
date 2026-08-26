"""Dockerfile COPY 와 워크플로 필터 대조 검사의 시나리오.

1. COPY 원본을 덮는 패턴이면 통과
2. COPY 에 있는데 필터에 없으면 그 경로를 이름으로 지적
3. 상위 패턴(`sub/**`)이 하위 COPY(`sub/generators`)를 덮음
4. 형제 패턴(`sub/airflow/**`)은 하위 COPY(`sub/generators`)를 덮지 않음  ← #739 의 실제 구멍
5. `--flags` 와 목적지는 원본으로 세지 않음
6. 이미지 안 코드가 import 하는 모듈이 COPY 되지 않으면 지적  ← success_marker 의 실제 구멍
7. 함수 안 지연 import 와 테스트 파일은 세지 않음
8. `/tmp` 로 가는 COPY 는 런타임 경로가 아님
"""

import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from check_image_filters import copy_sources, missing, runtime_copies, uncopied_imports


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


def test_없는_경로를_COPY_하면_잡는다(tmp_path):
    """`docker build` 가 "not found" 로 죽기 전에 잡아야 합니다.

    실제로 `main/aws_lambda/__init__.py` 를 COPY 에 적었다가 CI 이미지 빌드가
    깨졌습니다(#761). 그 파일은 암묵 namespace package 라 존재하지 않습니다.
    """

    dockerfile = tmp_path / "shared" / "airflow" / "Dockerfile"
    dockerfile.parent.mkdir(parents=True)
    dockerfile.write_text("FROM scratch\nCOPY main/nonexistent.py /opt/x/\n")

    sources = copy_sources(dockerfile)
    absent = [s for s in sources if not (tmp_path / s).exists()]

    assert absent == ["main/nonexistent.py"]


def _repo(tmp_path: Path, files: dict[str, str]) -> Path:
    for name, body in files.items():
        target = tmp_path / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(body)
    return tmp_path


def test_import_하는데_COPY_안_된_모듈을_지적한다(tmp_path):
    """실제로 겪은 형태입니다. `shared/common` 에서 쓰는 파일만 골라 COPY 했는데
    새 모듈(`success_marker`)이 생기자 잡이 import 하는데 이미지에는 없었습니다.
    빌드도 CI 도 통과하고 잡을 돌려야만 ModuleNotFoundError 로 보였습니다.
    """
    root = _repo(tmp_path, {
        "shared/common/__init__.py": "",
        "shared/common/s3_reader.py": "",
        "shared/common/success_marker.py": "",
        "main/spark/jobs/job.py": (
            "from shared.common.s3_reader import list_keys\n"
            "from shared.common.success_marker import marker_path\n"
        ),
    })
    sources = ["main/spark/jobs", "shared/common/__init__.py", "shared/common/s3_reader.py"]

    gaps = uncopied_imports(root, sources)

    assert len(gaps) == 1
    assert gaps[0].startswith("shared/common/success_marker.py")
    assert "main/spark/jobs/job.py" in gaps[0]


def test_디렉터리째_COPY_하면_통과한다(tmp_path):
    root = _repo(tmp_path, {
        "shared/common/__init__.py": "",
        "shared/common/success_marker.py": "",
        "main/spark/jobs/job.py": "from shared.common.success_marker import marker_path\n",
    })

    assert uncopied_imports(root, ["main/spark/jobs", "shared/common"]) == []


def test_함수_안_지연_import_는_세지_않는다(tmp_path):
    """그 함수를 부를 때만 필요한 선택적 의존입니다. 최상위 import 만
    "불러오는 순간 반드시 깨지는 것" 입니다.
    """
    root = _repo(tmp_path, {
        "shared/aws_lambda/common/s3_loader.py": "",
        "sub/generators/generate.py": (
            "def write_s3():\n"
            "    from shared.aws_lambda.common.s3_loader import BUCKET_ENV_VAR\n"
        ),
    })

    assert uncopied_imports(root, ["sub/generators"]) == []


def test_테스트_파일의_import_는_세지_않는다(tmp_path):
    """테스트는 이미지에 딸려 들어가지만 거기서 실행되지 않습니다."""
    root = _repo(tmp_path, {
        "sub/source_api/server.py": "",
        "sub/spark/tests/test_job.py": "from sub.source_api.server import app\n",
    })

    assert uncopied_imports(root, ["sub/spark"]) == []


def test_tmp_로_가는_COPY_는_런타임_경로가_아니다(tmp_path):
    """빌드 단계 재료(pyproject, pip 로 설치하는 libs)라 import 경로에 없습니다."""
    dockerfile = tmp_path / "Dockerfile"
    dockerfile.write_text(
        "FROM scratch\n"
        "COPY main/spark/pyproject.toml /tmp/main/spark/\n"
        "COPY libs/pipeline_core /tmp/libs/pipeline_core\n"
        "COPY shared/common/ /home/hadoop/shared/common/\n"
    )

    assert runtime_copies(dockerfile) == ["shared/common"]
