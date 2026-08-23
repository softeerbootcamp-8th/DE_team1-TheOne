"""변경 기반 테스트 선택 시나리오.

1. DAG 변경은 전용 테스트와 공통 계약 테스트 선택
2. shared Airflow 변경은 양 제품 전체 테스트
3. 문서만 변경되면 pytest 생략
4. 알 수 없는 Python 변경은 전체 테스트 fallback
5. 테스트 파일 변경은 해당 테스트만 선택
6. 분리된 제품 테스트 실행은 저장소 루트를 import 경로로 사용
7. shared AWS Lambda 변경은 세 Lambda 코드 영역 전체 테스트 선택
8. 수동 매핑표의 키가 실재하는 파이프라인인지 확인 (죽은 항목 차단)
9. 수동 매핑표의 값이 실재하는 테스트 파일인지 확인
10. 모든 파이프라인이 전용 테스트를 최소 1개 고르는지 확인
11. 삭제된 테스트 파일은 실행 대상에서 제외
12. Hook 변경은 Hook 전용 테스트만 선택
13. Skill 문서는 제품 테스트를 선택하지 않음
14. Skill 검사기 변경은 해당 자체 검사만 선택
15. GitHub CI 스크립트는 파일별 전용 테스트만 선택
16. 모니터링 경로는 모니터링 전용 CI가 소유하므로 제품 테스트를 선택하지 않음
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
        ["main/airflow/dags/driver_vehicle_monthly_snapshot_raw_to_silver_dag.py"]
    )

    assert result.full == set()
    assert "tests/test_driver_vehicle_monthly_snapshot_raw_to_silver_dag.py" in result.tests["main/airflow"]
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


def test_Review_Hook_변경은_Hook_전용_테스트만_선택한다():
    result = select_tests.select_tests([".claude/hooks/review_gate.py"])

    assert result.full == set()
    assert result.tests == {".claude/hooks": {"test_review_gate.py"}}
    assert select_tests.selected_projects(result) == [".claude/hooks"]


def test_Skill_문서는_제품_테스트를_선택하지_않는다():
    result = select_tests.select_tests(
        [
            ".agents/skills/write-pr/SKILL.md",
            ".claude/skills/review-engineering/references/senior-de-playbook.md",
        ]
    )

    assert select_tests.render(result) == "NONE"


def test_Skill_검사기_변경은_각_자체_검사만_선택한다():
    result = select_tests.select_tests(
        [
            ".agents/skills/write-issue/check_draft.py",
            ".claude/skills/write-issue/check_draft.py",
        ]
    )

    assert result.full == set()
    assert result.tests == {
        ".agents/skills/write-issue": {"check_draft.py"},
        ".claude/skills/write-issue": {"check_draft.py"},
    }


def test_GitHub_CI_스크립트는_파일별_전용_테스트만_선택한다():
    cases = {
        ".github/scripts/select_tests.py": "test_select_tests.py",
        ".github/scripts/test_select_tests.py": "test_select_tests.py",
        ".github/scripts/check_image_filters.py": "test_check_image_filters.py",
        ".github/scripts/test_check_image_filters.py": "test_check_image_filters.py",
    }

    for changed, expected in cases.items():
        result = select_tests.select_tests([changed])
        assert result.full == set()
        assert result.tests == {".github/scripts": {expected}}


def test_분류되지_않은_GitHub_CI_스크립트는_스크립트_테스트만_전체선택한다():
    result = select_tests.select_tests([".github/scripts/new_guard.py"])

    assert result.full == {".github/scripts"}
    assert result.tests == {}


def test_모니터링_변경은_제품_테스트를_선택하지_않는다():
    result = select_tests.select_tests(
        ["monitoring/tests/test_monitoring.py", "monitoring/cloudformation.yml"]
    )

    assert select_tests.render(result) == "NONE"


def test_테스트_파일_변경은_그_테스트만_선택한다():
    result = select_tests.select_tests(
        ["main/airflow/tests/test_driver_vehicle_monthly_snapshot_raw_to_silver_dag.py"]
    )

    assert result.full == set()
    assert result.tests == {
        "main/airflow": {"tests/test_driver_vehicle_monthly_snapshot_raw_to_silver_dag.py"}
    }


def test_삭제된_테스트_파일은_실행_대상에서_제외한다():
    result = select_tests.select_tests(
        ["main/airflow/tests/test_removed_dag.py"]
    )

    assert select_tests.render(result) == "NONE"


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


def test_GitHub_CI_스크립트_러너는_선택된_전용_테스트만_실행한다(monkeypatch):
    calls = []
    monkeypatch.setattr(
        select_tests.subprocess,
        "run",
        lambda command, **kwargs: calls.append((command, kwargs)),
    )
    selection = select_tests.select_tests(
        [".github/scripts/check_image_filters.py"]
    )

    select_tests.run(selection, only=".github/scripts")

    command, kwargs = calls[0]
    assert command[-1] == str(
        select_tests.ROOT / ".github/scripts/test_check_image_filters.py"
    )
    assert str(select_tests.ROOT / ".github/scripts/test_select_tests.py") not in command
    assert kwargs["cwd"] == select_tests.ROOT / "main/airflow"


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


def test_Hook_러너는_제품_uv환경없이_전용_unittest만_실행한다(monkeypatch):
    calls = []
    monkeypatch.setattr(
        select_tests.subprocess,
        "run",
        lambda command, **kwargs: calls.append((command, kwargs)),
    )
    selection = select_tests.select_tests([".claude/hooks/review_gate.py"])

    select_tests.run(selection, only=".claude/hooks")

    assert [command for command, _ in calls] == [
        [
            sys.executable,
            str(select_tests.ROOT / ".claude/hooks/test_review_gate.py"),
            "-v",
        ],
        [
            sys.executable,
            str(select_tests.ROOT / ".claude/hooks/review_gate.py"),
            "--self-check",
        ],
    ]
    assert all(kwargs["cwd"] == select_tests.ROOT for _, kwargs in calls)


def test_Skill_검사기_러너는_제품_uv환경없이_자체검사만_실행한다(monkeypatch):
    calls = []
    monkeypatch.setattr(
        select_tests.subprocess,
        "run",
        lambda command, **kwargs: calls.append((command, kwargs)),
    )
    selection = select_tests.select_tests(
        [".claude/skills/write-issue/check_draft.py"]
    )

    select_tests.run(selection, only=".claude/skills/write-issue")

    assert [command for command, _ in calls] == [
        [
            sys.executable,
            str(select_tests.ROOT / ".claude/skills/write-issue/check_draft.py"),
            "--self-check",
        ]
    ]
    assert calls[0][1]["cwd"] == select_tests.ROOT


def _pipelines(product: str) -> set[str]:
    scripts = select_tests.ROOT / product / "airflow" / "scripts"
    return {
        path.name
        for path in scripts.iterdir()
        if path.is_dir() and (path / "tasks.py").is_file()
    }


def test_표의_키가_실재하는_파이프라인이다():
    """옮겨가거나 이름이 바뀐 파이프라인의 항목은 아무 효과 없이 남습니다.

    #518 에서 EIA 가 main 으로 간 뒤 `sub` 쪽 항목 3개가 그대로 남아, 표만 보면
    매핑이 있는 것처럼 보이는데 실제로는 한 번도 쓰이지 않았습니다(#538).
    """
    for product, overrides in select_tests.AIRFLOW_OVERRIDES.items():
        assert set(overrides) <= _pipelines(product), (
            f"{product} 에 없는 파이프라인: {sorted(set(overrides) - _pipelines(product))}"
        )


def test_표의_값이_실재하는_테스트_파일이다():
    """없는 이름은 `_airflow_tests_for` 가 조용히 걸러내 빈 선택이 됩니다."""
    for product, overrides in select_tests.AIRFLOW_OVERRIDES.items():
        existing = select_tests._existing_airflow_tests(product)
        named = {name for names in overrides.values() for name in names}
        assert named <= existing, f"{product} 에 없는 테스트: {sorted(named - existing)}"


def test_모든_파이프라인이_전용_테스트를_하나는_고른다():
    """전역 계약 테스트만 도는 파이프라인은 사실상 CI 밖입니다.

    규약(`test_{pipeline}_dag.py`)과 이름이 다른 테스트만 있으면 여기서 걸립니다 —
    `lease_vehicle_inventory_raw_to_silver` 가 그랬습니다(#538).
    """
    for product in select_tests.PRODUCTS:
        project = f"{product}/airflow"
        globals_ = select_tests.AIRFLOW_GLOBAL_TESTS
        for pipeline in sorted(_pipelines(product)):
            changed = f"{product}/airflow/scripts/{pipeline}/tasks.py"
            chosen = select_tests.select_tests([changed]).tests.get(project, set())
            dedicated = {
                name for name in chosen if Path(name).name not in globals_
            }
            assert dedicated, f"{project}/{pipeline} 이 전용 테스트를 고르지 않습니다"
