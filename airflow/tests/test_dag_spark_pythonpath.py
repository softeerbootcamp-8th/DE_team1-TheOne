"""Spark job 을 부르는 DAG 가 필요한 경로를 전부 넘기는지 확인합니다.

BashOperator 가 띄우는 프로세스는 **DAG 파싱 때 넣은 `sys.path` 를 물려받지
않습니다.** 그래서 각 DAG 가 `env["PYTHONPATH"]` 로 직접 넘겨야 하는데, 여기서
`libs/pipeline_core` 를 빠뜨리는 실수가 두 번 났습니다(#328, #351). Spark job 을
부르는 DAG 가 늘 때마다 반복될 자리라 검사로 막습니다.

DAG 목록을 하드코딩하지 않고 `bash_command` 가 `spark/jobs/` 를 가리키는 태스크를
찾아냅니다. 새 DAG 가 생겨도 자동으로 대상에 들어갑니다.
"""

import importlib
from pathlib import Path

import pytest

DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"

# Spark job 이 import 하는 것들의 뿌리. `common.io` 는 `spark/`, 그 안에서 쓰는
# `pipeline_core` 는 `libs/pipeline_core` 에 있습니다.
REQUIRED_ROOTS = ("", "/spark", "/libs/pipeline_core")


def spark_bash_tasks():
    """`bash_command` 가 spark job 을 부르는 (DAG 파일, 태스크) 목록."""
    found = []
    for dag_path in sorted(DAGS_DIR.glob("*.py")):
        module = importlib.import_module(f"dags.{dag_path.stem}")
        for value in vars(module).values():
            if not hasattr(value, "task_ids"):
                continue
            for task in value.tasks:
                command = getattr(task, "bash_command", None)
                if command and "spark/jobs/" in command:
                    found.append((dag_path.name, task))
    return found


SPARK_TASKS = spark_bash_tasks()


def test_Spark_job_을_부르는_태스크를_실제로_찾았다():
    """찾지 못하면 아래 테스트가 통째로 무력해집니다."""
    assert SPARK_TASKS, "spark/jobs/ 를 부르는 BashOperator 를 하나도 못 찾았습니다"


@pytest.mark.parametrize(
    ("dag_file", "task"),
    SPARK_TASKS,
    ids=[f"{dag_file}:{task.task_id}" for dag_file, task in SPARK_TASKS],
)
def test_Spark_태스크가_PYTHONPATH_에_필요한_경로를_모두_넘긴다(dag_file, task):
    python_path = (task.env or {}).get("PYTHONPATH")
    assert python_path, f"{dag_file}:{task.task_id} 에 PYTHONPATH 가 없습니다"

    # 프로젝트 루트는 DAG 마다 계산 방식이 달라 접미사로만 확인합니다.
    entries = [entry for entry in python_path.split(":") if entry]
    for suffix in REQUIRED_ROOTS:
        assert any(
            entry.endswith(suffix) for entry in entries
        ), f"{dag_file}:{task.task_id} 의 PYTHONPATH 에 '{suffix or '<프로젝트 루트>'}' 가 없습니다"
