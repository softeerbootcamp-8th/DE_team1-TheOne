"""변경 기반 테스트 선택 시나리오.

1. DAG 변경은 전용 테스트와 공통 계약 테스트 선택
2. shared Airflow 변경은 양 제품 전체 테스트
3. 문서만 변경되면 pytest 생략
4. 알 수 없는 Python 변경은 전체 테스트 fallback
5. 테스트 파일 변경은 해당 테스트만 선택
6. 분리된 제품 테스트 실행은 저장소 루트를 import 경로로 사용
"""

import importlib.util
from pathlib import Path
import sys


MODULE_PATH = Path(__file__).with_name("select_tests.py")
SPEC = importlib.util.spec_from_file_location("select_tests", MODULE_PATH)
select_tests = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = select_tests
SPEC.loader.exec_module(select_tests)


def test_DAG_변경은_전용_테스트와_공통_계약을_선택한다():
    result = select_tests.select_tests(
        ["main/airflow/dags/driver_master_raw_to_silver_dag.py"]
    )

    assert result.full == set()
    assert "tests/test_driver_master_raw_to_silver_dag.py" in result.tests["main/airflow"]
    assert "tests/test_dag_module_contracts.py" in result.tests["main/airflow"]
    assert "tests/test_slack_callbacks.py" in result.tests["main/airflow"]


def test_shared_Airflow_변경은_양쪽_전체_테스트를_선택한다():
    result = select_tests.select_tests(["shared/airflow/common/validation.py"])

    assert result.full == {"main/airflow", "sub/airflow"}


def test_문서만_변경되면_테스트를_선택하지_않는다():
    result = select_tests.select_tests(["README.md", "docs/GETTING_STARTED.md"])

    assert select_tests.render(result) == "NONE"


def test_알_수_없는_Python_변경은_전체_테스트로_fallback한다():
    result = select_tests.select_tests(["tools/new_job.py"])

    assert result.full == select_tests.ALL_PROJECTS


def test_테스트_파일_변경은_그_테스트만_선택한다():
    result = select_tests.select_tests(
        ["main/airflow/tests/test_driver_master_raw_to_silver_dag.py"]
    )

    assert result.full == set()
    assert result.tests == {
        "main/airflow": {"tests/test_driver_master_raw_to_silver_dag.py"}
    }


def test_분리된_제품_테스트는_저장소_루트를_import_경로로_사용한다(monkeypatch):
    calls = []
    monkeypatch.setattr(
        select_tests.subprocess,
        "run",
        lambda *args, **kwargs: calls.append((args, kwargs)),
    )

    selection = select_tests.Selection()
    selection.add("sub/airflow", "tests/test_source_dag_runtime_contracts.py")
    select_tests.run(selection)

    _, kwargs = calls[0]
    assert kwargs["cwd"] == select_tests.ROOT / "main/airflow"
    assert kwargs["env"]["PYTHONPATH"] == str(select_tests.ROOT)
    assert "VIRTUAL_ENV" not in kwargs["env"]
