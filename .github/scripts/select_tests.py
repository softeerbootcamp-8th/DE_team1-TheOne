#!/usr/bin/env python3
"""변경 파일을 실제로 영향받는 pytest 묶음으로 변환합니다."""

from __future__ import annotations

import argparse
import os
import subprocess
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
AIRFLOW_OVERRIDES = {
    "main": {
        "hvfhv_driver_trip_silver": {"test_driver_trip_silver_dag.py"},
        "hvfhv_raw_to_silver": {
            "test_hvfhv_raw_to_silver_dag.py",
            "test_hvfhv_validation.py",
        },
        "hvfhv_silver_to_gold": {"test_silver_to_gold_dag.py"},
    },
    "sub": {
        "eia_electricity_price_raw_to_bronze": {"test_eia_fuel_price_dag.py"},
        "eia_fuel_price_bronze_to_silver": {"test_eia_fuel_price_dag.py"},
        "eia_gas_price_raw_to_bronze": {"test_eia_fuel_price_dag.py"},
        "fueleconomy_vehicle_specs_raw_to_silver": {
            "test_fueleconomy_vehicle_specs_raw_to_silver_dag.py",
            "test_vehicle_specs_validation.py",
        },
        "lyft_eligible_vehicles_raw_to_silver": {
            "test_lyft_eligible_vehicles_raw_to_silver_dag.py",
            "test_lyft_eligible_validation.py",
        },
        "uber_eligible_vehicles_raw_to_silver": {
            "test_uber_eligible_vehicles_raw_to_silver_dag.py",
            "test_uber_eligible_validation.py",
        },
        "vehicle_catalog_raw_to_silver": {
            "test_vehicle_catalog_raw_to_silver_dag.py",
            "test_vehicle_catalog_validation.py",
        },
    },
}


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
    "main/spark": ("main/spark", "main/spark"),
    "sub/spark": ("main/spark", "sub/spark"),
    "main/dashboard": ("main/dashboard", "main/dashboard"),
    "libs/pipeline_core": ("libs/pipeline_core", "libs/pipeline_core"),
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
        if path in {"Makefile", ".github/workflows/ci.yml"} or path.startswith(
            ".github/scripts/"
        ):
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
        if matched:
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


def run(selection: Selection) -> None:
    if not selection.full and not selection.tests:
        print("테스트 대상 없음")
        return
    environment = os.environ.copy()
    environment.pop("VIRTUAL_ENV", None)
    environment["PYTHONPATH"] = str(ROOT)
    for project in sorted(selection.full | selection.tests.keys()):
        runtime_dir, target_dir = RUNNERS[project]
        runtime = ROOT / runtime_dir
        target = ROOT / target_dir
        if project == ".github/scripts":
            paths = [str(target / "test_select_tests.py")]
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
    parser.add_argument("files", nargs="*")
    args = parser.parse_args()
    files = changed_files(args.base, args.head) if args.base else args.files
    selection = select_tests(files)
    print(render(selection))
    if args.run:
        run(selection)


if __name__ == "__main__":
    main()
