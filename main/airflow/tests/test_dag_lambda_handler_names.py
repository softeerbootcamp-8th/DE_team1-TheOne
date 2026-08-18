"""DAG 가 넘기는 핸들러 이름이 실제로 import 되는지 확인합니다.

각 데이터셋 실행 모듈은 `lambda_handler_for("<함수 디렉터리 이름>")` 로
lambda/functions 아래 모듈을 동적으로 불러옵니다. `lambda` 가 파이썬 예약어라
정적 import 가 안 돼 문자열로 넘기는데, **문자열이라 오타가 나도 import 시점까지
아무도 모릅니다.**

실제로 HVFHV DAG 가 데이터셋 이름(`"hvfhv"`)을 넘겨 `raw_to_bronze` 태스크가
`ModuleNotFoundError` 로 죽었습니다(#322). 다른 DAG 테스트는 `lambda_handler_for`
를 가짜로 바꿔서 이 경로를 검증하지 못합니다 — 가짜로 바꾸는 순간 import 가
일어나지 않기 때문입니다.

그래서 여기서는 **가짜로 바꾸지 않고 진짜로 import** 합니다. DAG와 분리된 scripts
소스에서 문자열 인자를 AST 로 뽑아내므로 실행 모듈이 새로 생겨도 자동으로 대상에
들어갑니다.
"""

import ast
import importlib
import sys
from pathlib import Path

import pytest

AIRFLOW_DIR = Path(__file__).resolve().parents[1]
PROJECT_ROOT = AIRFLOW_DIR.parent.parent
DAGS_DIR = AIRFLOW_DIR / "dags"
SCRIPTS_DIR = AIRFLOW_DIR / "scripts"

# DAG 가 컨테이너에서 하는 것과 같은 경로 설정입니다. 저장소 루트에도 별도
# ``scripts`` package가 있으므로 Airflow 디렉터리가 반드시 먼저 와야 합니다.
for path in (
    PROJECT_ROOT / "libs" / "pipeline_core",
    PROJECT_ROOT,
    AIRFLOW_DIR,
):
    path_text = str(path)
    if path_text in sys.path:
        sys.path.remove(path_text)
    sys.path.insert(0, path_text)


def handler_names() -> list[tuple[str, str]]:
    """DAG와 scripts에서 ``lambda_handler_for(...)`` 문자열 인자를 뽑습니다."""
    found: list[tuple[str, str]] = []
    source_paths = [
        *sorted(DAGS_DIR.glob("*.py")),
        *sorted(SCRIPTS_DIR.glob("**/*.py")),
    ]
    for source_path in source_paths:
        tree = ast.parse(
            source_path.read_text(encoding="utf-8"), filename=str(source_path)
        )
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == "lambda_handler_for"
                and node.args
                and isinstance(node.args[0], ast.Constant)
                and isinstance(node.args[0].value, str)
            ):
                found.append(
                    (str(source_path.relative_to(AIRFLOW_DIR)), node.args[0].value)
                )
    return found


HANDLER_NAMES = handler_names()


def test_핸들러를_부르는_실행_모듈을_실제로_찾았다():
    """AST 추출이 조용히 0건이 되면 아래 테스트가 통째로 무력해집니다."""
    assert len(HANDLER_NAMES) == 2, HANDLER_NAMES


@pytest.mark.parametrize(
    ("dag_file", "function_name"),
    HANDLER_NAMES,
    ids=[f"{dag}:{name}" for dag, name in HANDLER_NAMES],
)
def test_DAG_가_넘기는_핸들러_이름이_import_된다(dag_file, function_name):
    module = importlib.import_module(f"main.aws_lambda.functions.{function_name}.handler")

    # 이름이 맞아도 `lambda_handler` 가 없으면 태스크는 똑같이 죽습니다.
    assert callable(module.lambda_handler), f"{dag_file}: {function_name}"
