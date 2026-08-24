#!/usr/bin/env python3
"""변경 파일을 실제로 영향받는 pytest 묶음으로 변환합니다."""

from __future__ import annotations

import argparse
import ast
import functools
import json
import os
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
PRODUCTS = ("main", "sub")
AIRFLOW_GLOBAL_TESTS = {
    "test_dag_concurrency.py",
    "test_dag_module_contracts.py",
    "test_source_dag_runtime_contracts.py",
    "test_dag_lambda_handler_names.py",
    "test_dag_spark_pythonpath.py",
    "test_slack_callbacks.py",
}
# 규약(`test_{pipeline}_dag.py`)으로 못 찾는 것만 적습니다. 이름이 규약과 다르거나,
# 한 테스트가 여러 파이프라인을 함께 보는 경우입니다.
# 여기 적은 이름이 실재하는지는 `test_select_tests.py` 가 확인합니다 — 예전에 EIA 가
# main 으로 옮겨간 뒤(#518) `sub` 쪽 항목이 죽은 참조로 남아, EIA 를 고쳐도 전용
# 테스트가 하나도 안 돌았습니다(#538).
AIRFLOW_OVERRIDES = {
    "main": {
        # 가스·전력 원본 적재를 한 파일에서 함께 검증합니다.
        "eia_electricity_price_raw_to_bronze": {"test_eia_raw_to_bronze_validation.py"},
        "eia_gas_price_raw_to_bronze": {"test_eia_raw_to_bronze_validation.py"},
        "eia_electricity_price_bronze_to_silver": {
            "test_eia_electricity_price_raw_to_silver_dag.py"
        },
        "eia_gas_price_bronze_to_silver": {
            "test_eia_gas_price_raw_to_silver_dag.py"
        },
        "monthly_taxi_trip_raw_to_silver": {
            "test_monthly_taxi_trip_raw_to_silver_dag.py",
            "test_monthly_taxi_trip_validation.py",
        },
        "lease_vehicle_inventory_raw_to_silver": {"test_lease_vehicle_inventory_dag.py"},
    },
    "sub": {
        "fueleconomy_vehicle_specs_raw_to_curated": {
            "test_fueleconomy_vehicle_specs_raw_to_curated_dag.py",
            "test_vehicle_specs_validation.py",
        },
        "lyft_eligible_vehicles_raw_to_curated": {
            "test_lyft_eligible_vehicles_raw_to_curated_dag.py",
            "test_lyft_eligible_validation.py",
        },
        "uber_eligible_vehicles_raw_to_curated": {
            "test_uber_eligible_vehicles_raw_to_curated_dag.py",
            "test_uber_eligible_validation.py",
        },
        "vehicle_catalog_raw_to_curated": {
            "test_vehicle_catalog_raw_to_curated_dag.py",
            "test_vehicle_catalog_validation.py",
        },
    },
}


# spark 프로젝트는 파일 하나만 고쳐도 테스트 전체(222건, 약 5분)가 돌았습니다.
# airflow 처럼 손으로 매핑하면 잘못 적었을 때 **테스트가 아예 안 도는** 쪽으로 틀립니다
# (#538 이 그 사례). 그래서 매핑을 적지 않고 import 관계에서 뽑습니다.
#
# 안전 방향을 한쪽으로 고정합니다 — 애매하면 전체를 돌립니다.
#   conftest.py / __init__.py 변경        전체 (수집 자체에 영향)
#   닿는 테스트가 하나도 없는 모듈          전체 (매핑 누락일 수 있음)
IMPORT_GRAPH_PROJECTS = {"main/spark", "sub/spark"}
FIRST_PARTY_ROOTS = ("main", "sub", "shared", "schema", "libs", "config")
ALWAYS_FULL_FILES = {"conftest.py", "__init__.py"}


def _module_of(path: str) -> str | None:
    """저장소 경로를 모듈 이름으로. `sub/spark/jobs/x/y.py` -> `sub.spark.jobs.x.y`"""
    if not path.endswith(".py"):
        return None
    parts = Path(path).with_suffix("").parts
    if not parts or parts[0] not in FIRST_PARTY_ROOTS:
        return None
    if parts[-1] == "__init__":
        parts = parts[:-1]
    return ".".join(parts) if parts else None


def _imports_of(file: Path) -> set[str]:
    """그 파일이 import 하는 저장소 안 모듈.

    함수 안 import 도 셉니다. 여기서는 과하게 고르는 쪽이 안전합니다 — 빠뜨리면
    테스트가 안 돌고, 더 고르면 조금 느려질 뿐입니다.
    """
    try:
        tree = ast.parse(file.read_text(encoding="utf-8"))
    except (SyntaxError, UnicodeDecodeError, OSError):
        return set()
    found: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            found.add(node.module)
        elif isinstance(node, ast.Import):
            found |= {alias.name for alias in node.names}
    return {name for name in found if name.split(".")[0] in FIRST_PARTY_ROOTS}


@functools.lru_cache(maxsize=None)
def _import_graph(project: str) -> tuple[dict[str, frozenset[str]], tuple[str, ...]]:
    """프로젝트 안 모듈별 import 목록과 테스트 모듈 목록.

    PR 하나가 파일 여러 개를 건드리므로 프로젝트당 한 번만 훑습니다.
    """
    root = ROOT / project
    files = [
        f
        for f in root.rglob("*.py")
        if ".venv" not in f.parts and "__pycache__" not in f.parts
    ]
    graph: dict[str, frozenset[str]] = {}
    tests: list[str] = []
    for file in files:
        module = _module_of(str(file.relative_to(ROOT)))
        if module is None:
            continue
        graph[module] = frozenset(_imports_of(file))
        if file.name.startswith("test_"):
            tests.append(module)
    return graph, tuple(sorted(tests))


def _tests_reaching(project: str, changed_module: str) -> set[str] | None:
    """`changed_module` 에 전이적으로 닿는 그 프로젝트의 테스트 파일.

    닿는 것이 없으면 `None` 을 돌려 호출부가 전체를 돌리게 합니다.
    """
    imports, test_modules = _import_graph(project)

    # changed_module 에 닿는 모듈을 역방향으로 넓힙니다.
    reaching = {changed_module}
    while True:
        grown = {
            module
            for module, targets in imports.items()
            if module not in reaching
            and any(
                target == r or target.startswith(r + ".") or r.startswith(target + ".")
                for target in targets
                for r in reaching
            )
        }
        if not grown:
            break
        reaching |= grown

    tests = {
        f"tests/{module.rsplit('.', 1)[-1]}.py"
        for module in test_modules
        if module in reaching
    }
    return tests or None


@dataclass
class Selection:
    tests: dict[str, set[str]] = field(default_factory=dict)
    full: set[str] = field(default_factory=set)

    def add(self, project: str, *paths: str) -> None:
        if project not in self.full:
            self.tests.setdefault(project, set()).update(paths)

    def add_full(self, *projects: str) -> None:
        for project in projects:
            self.full.add(project)
            self.tests.pop(project, None)


ALL_PROJECTS = {
    ".github/scripts",
    "main/airflow",
    "sub/airflow",
    "main/aws_lambda",
    "sub/aws_lambda",
    "shared/aws_lambda",
    "shared/common",
    "main/spark",
    "sub/spark",
    "main/dashboard",
    "libs/pipeline_core",
}

RUNNERS = {
    ".github/scripts": ("main/airflow", ".github/scripts"),
    "main/airflow": ("main/airflow", "main/airflow"),
    "sub/airflow": ("main/airflow", "sub/airflow"),
    "main/aws_lambda": ("main/aws_lambda", "main/aws_lambda"),
    "sub/aws_lambda": ("main/aws_lambda", "sub/aws_lambda"),
    "shared/aws_lambda": ("main/aws_lambda", "shared/aws_lambda"),
    # shared/common 테스트는 별도 uv 프로젝트가 없어 main/aws_lambda 런타임으로
    # 실행합니다. 세 제품 런타임에서 함께 쓰는 표준 라이브러리 기반 모듈만 둡니다.
    "shared/common": ("main/aws_lambda", "shared/common"),
    "main/spark": ("main/spark", "main/spark"),
    "sub/spark": ("main/spark", "sub/spark"),
    "main/dashboard": ("main/dashboard", "main/dashboard"),
    "libs/pipeline_core": ("libs/pipeline_core", "libs/pipeline_core"),
}

# 저장소 운영 도구는 제품 런타임을 import하지 않습니다. 이 경로를 일반 `.py`
# fallback에 맡기면 모든 제품 테스트를 돌면서도 정작 도구 자체 검사는 빠집니다.
# matrix 이름과 실제 명령을 함께 고정해 관련 검사만 짧게 실행합니다.
TOOL_COMMANDS = {
    ".claude/hooks": (
        (
            sys.executable,
            str(ROOT / ".claude/hooks/test_review_gate.py"),
            "-v",
        ),
        (
            sys.executable,
            str(ROOT / ".claude/hooks/review_gate.py"),
            "--self-check",
        ),
    ),
    ".agents/skills/write-issue": tuple(
        (
            sys.executable,
            str(ROOT / ".agents/skills/write-issue/check_draft.py"),
            str(ROOT / template),
        )
        for template in (
            ".github/ISSUE_TEMPLATE/task.md",
            ".github/ISSUE_TEMPLATE/bug.md",
            ".github/pull_request_template.md",
        )
    ),
    ".claude/skills/write-issue": (
        (
            sys.executable,
            str(ROOT / ".claude/skills/write-issue/check_draft.py"),
            "--self-check",
        ),
    ),
}

SKILL_CHECKER_PROJECTS = {
    ".agents/skills/write-issue/check_draft.py": ".agents/skills/write-issue",
    ".claude/skills/write-issue/check_draft.py": ".claude/skills/write-issue",
}

GITHUB_SCRIPT_TESTS = {
    ".github/scripts/select_tests.py": "test_select_tests.py",
    ".github/scripts/test_select_tests.py": "test_select_tests.py",
    ".github/scripts/check_image_filters.py": "test_check_image_filters.py",
    ".github/scripts/test_check_image_filters.py": "test_check_image_filters.py",
}


def _existing_airflow_tests(product: str) -> set[str]:
    tests_dir = ROOT / product / "airflow" / "tests"
    return {path.name for path in tests_dir.glob("test_*.py")}


def _airflow_tests_for(product: str, pipeline: str) -> set[str]:
    existing = _existing_airflow_tests(product)
    selected = AIRFLOW_OVERRIDES.get(product, {}).get(pipeline, set()).copy()
    conventional = f"test_{pipeline}_dag.py"
    if conventional in existing:
        selected.add(conventional)
    selected.update(AIRFLOW_GLOBAL_TESTS & existing)
    return {f"tests/{name}" for name in selected}


def select_tests(changed_files: list[str]) -> Selection:
    selection = Selection()
    for raw_path in changed_files:
        path = raw_path.strip().removeprefix("./")
        if not path:
            continue
        parts = Path(path).parts

        if path.startswith(("docs/",)) or path in {"README.md", "architecture.png"}:
            continue
        # monitoring/** 는 ci.yml 의 경량 전용 job 이 설정·배포 계약을 검증합니다.
        # 여기서 일반 Python fallback까지 태우면 같은 테스트와 제품 전체를 함께 실행합니다.
        if path.startswith("monitoring/"):
            continue
        if path.startswith((".claude/hooks/", ".githooks/")):
            selection.add(".claude/hooks", "test_review_gate.py")
            continue
        if path in SKILL_CHECKER_PROJECTS:
            selection.add(SKILL_CHECKER_PROJECTS[path], Path(path).name)
            continue
        if path.endswith(".md") and path.startswith(
            (".agents/skills/", ".claude/skills/")
        ):
            continue
        if path in GITHUB_SCRIPT_TESTS:
            selection.add(".github/scripts", GITHUB_SCRIPT_TESTS[path])
            continue
        if path.startswith(".github/scripts/"):
            selection.add_full(".github/scripts")
            continue
        if path in {"Makefile", ".github/workflows/ci.yml"}:
            selection.add_full(*ALL_PROJECTS)
            continue
        if path.startswith("shared/airflow/"):
            selection.add_full("main/airflow", "sub/airflow")
            continue
        if path.startswith("shared/aws_lambda/"):
            selection.add_full(
                "main/aws_lambda", "sub/aws_lambda", "shared/aws_lambda"
            )
            continue
        if path.startswith("shared/spark/"):
            selection.add_full("main/spark", "sub/spark")
            continue
        if path.startswith(("schema/", "libs/pipeline_core/")):
            selection.add_full(*ALL_PROJECTS)
            continue

        if len(parts) >= 3 and parts[0] in PRODUCTS and parts[1] == "airflow":
            product = parts[0]
            project = f"{product}/airflow"
            if parts[2] == "tests" and len(parts) == 4 and parts[3].startswith("test_"):
                if (ROOT / path).is_file():
                    selection.add(project, f"tests/{parts[3]}")
            elif parts[2] == "dags" and path.endswith("_dag.py"):
                pipeline = Path(path).stem.removesuffix("_dag")
                selection.add(project, *_airflow_tests_for(product, pipeline))
            elif parts[2] == "scripts" and len(parts) >= 4:
                selection.add(project, *_airflow_tests_for(product, parts[3]))
            else:
                selection.add_full(project)
            continue

        runtime_projects = {
            "main/aws_lambda": "main/aws_lambda",
            "sub/aws_lambda": "sub/aws_lambda",
            "main/spark": "main/spark",
            "sub/spark": "sub/spark",
            "main/dashboard": "main/dashboard",
        }
        matched = next(
            (project for prefix, project in runtime_projects.items() if path.startswith(f"{prefix}/")),
            None,
        )
        if matched in IMPORT_GRAPH_PROJECTS:
            module = _module_of(path)
            tests = None
            if module and Path(path).name not in ALWAYS_FULL_FILES:
                if Path(path).name.startswith("test_"):
                    tests = {f"tests/{Path(path).name}"} if (ROOT / path).is_file() else None
                else:
                    tests = _tests_reaching(matched, module)
            if tests:
                selection.add(matched, *tests)
            else:
                selection.add_full(matched)
        elif matched:
            selection.add_full(matched)
        elif path.endswith(".py"):
            selection.add_full(*ALL_PROJECTS)
    return selection


def changed_files(base: str, head: str) -> list[str]:
    result = subprocess.run(
        ["git", "diff", "--name-only", f"{base}...{head}"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.splitlines()


def render(selection: Selection) -> str:
    lines = []
    for project in sorted(selection.full | selection.tests.keys()):
        tests = (
            "ALL"
            if project in selection.full
            else " ".join(sorted(selection.tests[project]))
        )
        lines.append(f"{project}: {tests}")
    return "\n".join(lines) or "NONE"


def selected_projects(selection: Selection) -> list[str]:
    """실행 대상 프로젝트 목록. CI 가 이 값으로 matrix 를 폅니다."""
    return sorted(selection.full | selection.tests.keys())


def run(selection: Selection, only: str | None = None) -> None:
    projects = selected_projects(selection)
    if only is not None:
        projects = [project for project in projects if project == only]
        if not projects:
            print(f"{only} 는 이번 변경의 테스트 대상이 아닙니다")
            return
    if not projects:
        print("테스트 대상 없음")
        return
    environment = os.environ.copy()
    environment.pop("VIRTUAL_ENV", None)
    environment["PYTHONPATH"] = str(ROOT)
    for project in projects:
        if project in TOOL_COMMANDS:
            for command in TOOL_COMMANDS[project]:
                print(f"==> {project}: {' '.join(command)}", flush=True)
                subprocess.run(
                    list(command), cwd=ROOT, env=environment, check=True
                )
            continue
        runtime_dir, target_dir = RUNNERS[project]
        runtime = ROOT / runtime_dir
        target = ROOT / target_dir
        if project == ".github/scripts":
            names = (
                {path.name for path in target.glob("test_*.py")}
                if project in selection.full
                else selection.tests[project]
            )
            paths = [str(target / name) for name in sorted(names)]
        elif project in selection.full:
            paths = [str(target / "tests")]
        else:
            paths = [str(target / path) for path in sorted(selection.tests[project])]
        command = ["uv", "run", "--frozen", "pytest", "-q", *paths]
        print(f"==> {project}: {' '.join(paths)}", flush=True)
        subprocess.run(command, cwd=runtime, env=environment, check=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base")
    parser.add_argument("--head", default="HEAD")
    parser.add_argument("--run", action="store_true")
    # CI 가 matrix 를 펼 때 씁니다. 선택된 프로젝트를 JSON 배열로만 찍습니다.
    parser.add_argument("--matrix", action="store_true")
    # matrix 각 갈래가 자기 프로젝트만 돌 때 씁니다.
    parser.add_argument("--only")
    parser.add_argument("files", nargs="*")
    args = parser.parse_args()
    files = changed_files(args.base, args.head) if args.base else args.files
    selection = select_tests(files)
    if args.matrix:
        print(json.dumps(selected_projects(selection)))
        return
    print(render(selection))
    if args.run:
        run(selection, only=args.only)


if __name__ == "__main__":
    main()
