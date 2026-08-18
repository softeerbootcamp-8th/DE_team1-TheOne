"""변경 기반 테스트 선택 시나리오.

1. DAG 변경은 전용 테스트와 공통 계약 테스트 선택
2. shared Airflow 변경은 양 제품 전체 테스트
3. 문서만 변경되면 pytest 생략
4. 알 수 없는 Python 변경은 전체 테스트 fallback
5. 테스트 파일 변경은 해당 테스트만 선택
6. 분리된 제품 테스트 실행은 저장소 루트를 import 경로로 사용
7. shared AWS Lambda 변경은 세 Lambda 코드 영역 전체 테스트 선택
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


def test_shared_AWS_Lambda_변경은_세_Lambda_영역_전체_테스트를_선택한다():
    result = select_tests.select_tests(["shared/aws_lambda/common/logging_setup.py"])

    assert result.full == {
        "main/aws_lambda",
        "sub/aws_lambda",
        "shared/aws_lambda",
    }


# --- CI matrix 연동 ---------------------------------------------------------
#
# 프로젝트별 러너로 나누면서 추가된 두 진입점입니다. 여기가 틀리면 CI 가 조용히
# 일부만 돌거나(--only 오타) matrix 가 안 펴집니다(--matrix 형식).


def test_matrix_는_선택된_프로젝트를_정렬해_돌려준다():
    selection = select_tests.select_tests(["main/spark/jobs/x.py", "sub/spark/jobs/y.py"])

    assert select_tests.selected_projects(selection) == ["main/spark", "sub/spark"]


def test_대상이_없으면_matrix_는_빈_목록이다():
    """CI 가 이 값으로 job 을 띄울지 정합니다. 빈 matrix 는 에러라 미리 걸러야 합니다."""
    selection = select_tests.select_tests(["docs/README.md"])

    assert select_tests.selected_projects(selection) == []


def test_only_는_그_프로젝트만_돌린다(monkeypatch):
    """matrix 각 갈래가 자기 몫만 돌아야 합니다. 안 그러면 spark 를 러너마다 반복합니다."""
    ran = []
    monkeypatch.setattr(
        select_tests.subprocess, "run", lambda command, **kwargs: ran.append(kwargs["cwd"].name)
    )
    selection = select_tests.select_tests(["main/spark/jobs/x.py", "sub/spark/jobs/y.py"])

    select_tests.run(selection, only="sub/spark")

    assert ran == ["spark"]


def test_only_가_대상이_아니면_아무것도_돌지_않는다(monkeypatch):
    """선택되지 않은 프로젝트가 matrix 에 들어와도 헛돌지 않아야 합니다."""
    ran = []
    monkeypatch.setattr(select_tests.subprocess, "run", lambda command, **kwargs: ran.append(1))
    selection = select_tests.select_tests(["main/spark/jobs/x.py"])

    select_tests.run(selection, only="main/dashboard")

    assert ran == []
