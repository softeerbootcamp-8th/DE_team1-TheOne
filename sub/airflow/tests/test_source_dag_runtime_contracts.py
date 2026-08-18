"""원천 DAG의 Lambda 핸들러와 실패 알림 연결 계약."""

import ast
import importlib
from pathlib import Path

import pytest


AIRFLOW_DIR = Path(__file__).resolve().parents[1]
DAGS_DIR = AIRFLOW_DIR / "dags"
SCRIPTS_DIR = AIRFLOW_DIR / "scripts"


def _handler_names():
    found = []
    for source_path in [*DAGS_DIR.glob("*.py"), *SCRIPTS_DIR.glob("**/*.py")]:
        tree = ast.parse(source_path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == "lambda_handler_for"
                and node.args
                and isinstance(node.args[0], ast.Constant)
            ):
                found.append(node.args[0].value)
    return sorted(found)


HANDLER_NAMES = _handler_names()


def test_원천_DAG가_부르는_Lambda_핸들러가_모두_import된다():
    assert len(HANDLER_NAMES) == 9, HANDLER_NAMES
    for function_name in HANDLER_NAMES:
        module = importlib.import_module(
            f"sub.aws_lambda.functions.{function_name}.handler"
        )
        assert callable(module.lambda_handler)


@pytest.mark.parametrize("dag_path", sorted(DAGS_DIR.glob("*_dag.py")))
def test_원천_DAG의_모든_task에_실패_콜백이_연결된다(dag_path):
    module = importlib.import_module(f"dags.{dag_path.stem}")
    dags = [value for value in vars(module).values() if hasattr(value, "tasks")]
    assert len(dags) == 1
    for task in dags[0].tasks:
        assert task.on_failure_callback
