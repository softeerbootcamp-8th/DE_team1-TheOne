#!/usr/bin/env python3
"""Dockerfile 의 COPY 와 워크플로 paths 필터가 어긋나지 않는지 검사합니다.

왜 필요한가
----------
이미지에 들어가는 파일이 바뀌면 이미지를 다시 굽고 배포해야 합니다. 그 판단을
워크플로의 `paths:` 필터가 하는데, 그 목록은 Dockerfile 의 `COPY` 와 **손으로**
맞춰야 합니다. 어긋나면 두 방향으로 틀립니다.

    COPY 에 있는데 필터에 없음   →  코드가 바뀌어도 배포가 스킵됨 (조용함)
    필터에 있는데 COPY 에 없음   →  안 바뀐 이미지를 굽고 배포함 (DAG 이 끊김)
    import 하는데 COPY 안 됨     →  잡 실행 시점에 ModuleNotFoundError (조용함)

앞의 것이 특히 나쁩니다 — 초록불인데 반영이 안 됩니다. 실제로 세 번 겪었습니다
(#653 aws_lambda 필터 6개 누락, #732 config/ 누락, #739 sub/generators 누락).

그래서 목록을 두 곳에 두는 것 자체는 유지하되(워크플로가 Dockerfile 을 읽을 수는
없으므로), **어긋나면 CI 가 막게** 합니다.
"""

from __future__ import annotations

import argparse
import ast
import sys
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]

# 검사 대상. (Dockerfile, 워크플로, 그 워크플로에서 필터를 찾을 위치)
#   ci      = ci.yml 의 paths-filter 항목 이름
#   deploy  = 배포 워크플로의 on.push.paths
TARGETS = [
    {
        "dockerfile": "shared/airflow/Dockerfile",
        "ci_filter": "airflow",
        "deploy_workflow": ".github/workflows/deploy-airflow.yml",
    },
    {
        "dockerfile": "shared/spark/Dockerfile",
        "ci_filter": "spark",
        "deploy_workflow": ".github/workflows/deploy-spark.yml",
    },
    {
        "dockerfile": "shared/aws_lambda/Dockerfile",
        "ci_filter": "aws_lambda",
        "deploy_workflow": ".github/workflows/deploy-lambda.yml",
    },
    {
        "dockerfile": "shared/dashboard/Dockerfile",
        "ci_filter": "dashboard",
        "deploy_workflow": ".github/workflows/deploy-dashboard.yml",
    },
]

# COPY 원본이 아닌 것. 빌드 단계에서만 쓰거나 이미지 내용과 무관합니다.
IGNORED_SOURCES = {"--from=builder"}


def copy_sources(dockerfile: Path) -> list[str]:
    """`COPY [--flags] <src>... <dst>` 에서 원본 경로만 뽑습니다."""
    sources: list[str] = []
    for line in dockerfile.read_text().splitlines():
        line = line.strip()
        if not line.startswith("COPY "):
            continue
        parts = [p for p in line.split()[1:] if not p.startswith("--")]
        if len(parts) < 2:
            continue
        for src in parts[:-1]:  # 마지막은 목적지
            if src not in IGNORED_SOURCES:
                sources.append(src.rstrip("/"))
    return sources


def _covers(pattern: str, source: str) -> bool:
    """필터 패턴이 COPY 원본을 덮는지.

    `sub/generators/**` 는 `sub/generators` 를 덮습니다. `sub/**` 도 덮습니다.
    반대로 `sub/airflow/**` 는 `sub/generators` 를 덮지 않습니다.
    """
    base = pattern.removesuffix("/**").removesuffix("/*").rstrip("/")
    if pattern == source:
        return True
    return source == base or source.startswith(base + "/") or base.startswith(source + "/")


def missing(sources: list[str], patterns: list[str]) -> list[str]:
    return sorted({s for s in sources if not any(_covers(p, s) for p in patterns)})


# 저장소 안의 최상위 패키지. 이 이름으로 시작하는 import 만 COPY 대상인지 봅니다.
FIRST_PARTY = {"shared", "main", "sub", "schema", "config"}


def runtime_copies(dockerfile: Path) -> list[str]:
    """이미지의 **실행 경로**로 들어가는 COPY 원본만.

    `/tmp` 로 가는 것은 빌드 단계 재료(pyproject, uv.lock, pip 로 설치하는 libs)라
    런타임 import 경로에 없습니다. 그걸 포함하면 덮인 것으로 잘못 셉니다.
    """
    sources: list[str] = []
    for line in dockerfile.read_text().splitlines():
        line = line.strip()
        if not line.startswith("COPY "):
            continue
        parts = [p for p in line.split()[1:] if not p.startswith("--")]
        if len(parts) < 2 or parts[-1].startswith("/tmp"):
            continue
        sources += [s.rstrip("/") for s in parts[:-1] if s not in IGNORED_SOURCES]
    return sources


def _module_level_imports(path: Path):
    """모듈 최상위 import 만.

    함수 안 import 는 그 함수를 부를 때만 필요한 선택적 의존입니다 (예:
    `generate.py` 가 S3 경로에서만 `s3_loader` 를 씁니다). 최상위 import 만
    "그 파일을 불러오는 순간 반드시 깨지는 것" 입니다.
    """
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"))
    except (SyntaxError, UnicodeDecodeError):
        return
    for node in tree.body:
        if isinstance(node, ast.ImportFrom):
            if node.level == 0 and node.module:
                yield node.module
        elif isinstance(node, ast.Import):
            for alias in node.names:
                yield alias.name


def _module_file(root: Path, module: str) -> str | None:
    """`shared.common.success_marker` -> `shared/common/success_marker.py`."""
    parts = module.split(".")
    if parts[0] not in FIRST_PARTY:
        return None
    base = root.joinpath(*parts)
    for candidate in (base.with_suffix(".py"), base / "__init__.py"):
        if candidate.is_file():
            return str(candidate.relative_to(root))
    return None


def uncopied_imports(root: Path, sources: list[str]) -> list[str]:
    """이미지 안 코드가 import 하는데 이미지에 안 들어가는 모듈.

    빌드도 CI 도 통과하고 **잡을 실제로 돌려야** ModuleNotFoundError 로 보입니다.
    쓰는 파일만 골라 COPY 하다가 새 모듈이 생기면 그대로 재현됩니다.
    """
    def is_test(path: Path) -> bool:
        # 테스트는 이미지에 딸려 들어가지만 거기서 실행되지 않습니다.
        return path.name.startswith("test_") or "tests" in path.parts

    in_image: list[Path] = []
    for source in sources:
        path = root / source
        if path.is_dir():
            in_image += sorted(f for f in path.rglob("*.py") if not is_test(f))
        elif path.suffix == ".py" and not is_test(path):
            in_image.append(path)

    def covered(target: str) -> bool:
        return any(target == s or target.startswith(s + "/") for s in sources)

    gaps: set[str] = set()
    for path in in_image:
        for module in _module_level_imports(path):
            target = _module_file(root, module)
            if target and not covered(target):
                gaps.add(f"{target}  <- {path.relative_to(root)}")
    return sorted(gaps)


def ci_filter_paths(workflow: Path, name: str) -> list[str]:
    """`dorny/paths-filter` 의 `filters:` 블록에서 한 항목의 경로 목록.

    `filters` 값은 YAML 안의 **문자열 블록**(`|` 스칼라)입니다. 워크플로를 통째로
    파싱하면 그 값이 문자열로 나오므로, 그 문자열만 다시 YAML 로 읽습니다.
    """
    document = yaml.safe_load(workflow.read_text())
    for job in (document.get("jobs") or {}).values():
        for step in job.get("steps") or []:
            if not str(step.get("uses", "")).startswith("dorny/paths-filter"):
                continue
            raw = (step.get("with") or {}).get("filters")
            if not raw:
                continue
            parsed = yaml.safe_load(raw) or {}
            if name in parsed:
                return list(parsed[name])
    raise SystemExit(f"{workflow} 에 '{name}' 필터가 없습니다")


def deploy_paths(workflow: Path) -> list[str]:
    document = yaml.safe_load(workflow.read_text())
    # `on` 은 YAML 1.1 에서 True 로 파싱됩니다.
    triggers = document.get("on") or document.get(True) or {}
    return list((triggers.get("push") or {}).get("paths") or [])


def main() -> int:
    parser = argparse.ArgumentParser(description="Dockerfile COPY 와 워크플로 필터 대조")
    parser.add_argument("--root", default=str(REPO_ROOT))
    args = parser.parse_args()
    root = Path(args.root)

    failures: list[str] = []
    for target in TARGETS:
        dockerfile = root / target["dockerfile"]
        if not dockerfile.is_file():
            failures.append(f"Dockerfile 이 없습니다: {target['dockerfile']}")
            continue
        sources = copy_sources(dockerfile)

        # 없는 경로를 COPY 하면 `docker build` 가 "not found" 로 죽습니다. 이미지를
        # 굽기 전에 여기서 잡습니다 — 빌드는 몇 분, 이 검사는 즉시입니다.
        absent = sorted(s for s in sources if not (root / s).exists())
        status = "ok  " if not absent else "FAIL"
        print(f"{status} {target['dockerfile']} COPY 대상 존재")
        if absent:
            for path in absent:
                print(f"       COPY 하는데 저장소에 없음: {path}")
            failures.append(f"{target['dockerfile']} COPY 대상 존재")

        gaps = uncopied_imports(root, runtime_copies(dockerfile))
        status = "ok  " if not gaps else "FAIL"
        print(f"{status} {target['dockerfile']} import 대상이 이미지에 있음")
        if gaps:
            for gap in gaps:
                print(f"       import 하는데 COPY 안 됨: {gap}")
            failures.append(f"{target['dockerfile']} import 대상이 이미지에 있음")

        for label, patterns in (
            ("ci.yml", ci_filter_paths(root / ".github/workflows/ci.yml", target["ci_filter"])),
            (target["deploy_workflow"], deploy_paths(root / target["deploy_workflow"])),
        ):
            gaps = missing(sources, patterns)
            status = "ok  " if not gaps else "FAIL"
            print(f"{status} {target['dockerfile']} vs {label}")
            if gaps:
                for gap in gaps:
                    print(f"       COPY 에 있는데 필터에 없음: {gap}")
                failures.append(f"{target['dockerfile']} vs {label}")

    if failures:
        print()
        print("이미지에 들어가는데 필터에 없는 경로가 있습니다. 그 파일을 고치면 이미지가")
        print("바뀌는데 빌드·배포가 스킵됩니다. 필터에 추가하거나 COPY 를 줄이세요.")
        print("import 인데 COPY 안 된 것이 있으면, 그 잡은 실행 시점에")
        print("ModuleNotFoundError 로 죽습니다. 파일 단위 COPY 를 디렉터리로 넓히세요.")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
