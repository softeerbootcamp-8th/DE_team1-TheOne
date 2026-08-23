"""deferrable EMR Serverless 실패 사유가 `KeyError` 로 덮이지 않는지 검증합니다.

provider 9.31.0 의 `execute_complete` 는 실패 경로에서 성공 경로 전용 키
(`job_details`) 를 읽어 `KeyError` 를 냅니다. 그러면 UI·Slack 알림에 실제 원인
(OOM, ExitCode 137 등)이 아니라 `KeyError` 가 뜹니다.

provider 를 올릴 때 이 테스트가 깨지면 상류가 고쳐졌다는 뜻입니다 — 그때는
하위 클래스를 지우고 provider 를 직접 쓰면 됩니다.
"""

import pytest
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.operators.emr import (
    EmrServerlessStartJobOperator as ProviderOperator,
)

from shared.airflow.common.emr_serverless import EmrServerlessStartJobOperator


# 00:56 실행(job 00g87cm0vgnta02r)의 트리거가 실제로 내보낸 모양입니다.
FAILURE_EVENT = {
    "status": "failure",
    "message": (
        "Serverless Job failed: FAILED - Job failed, please check complete logs "
        "in configured logging destination. ExitCode: 137. Last few exceptions: "
        "Worker has been killed as memory usage exceeded configured memory size"
    ),
}
SUCCESS_EVENT = {
    "status": "success",
    "job_details": {"application_id": "app-1", "job_id": "job-1"},
}


def _operator(cls):
    return cls(
        task_id="build",
        application_id="app-1",
        execution_role_arn="arn:aws:iam::572660899671:role/example",
        job_driver={},
        deferrable=True,
        cancel_on_kill=True,
    )


def test_실패_사유가_예외_메시지에_그대로_실린다():
    with pytest.raises(AirflowException) as caught:
        _operator(EmrServerlessStartJobOperator).execute_complete(
            context={}, event=FAILURE_EVENT
        )
    message = str(caught.value)
    assert "ExitCode: 137" in message
    assert "memory usage exceeded configured memory size" in message


def test_성공_경로는_provider_구현에_위임한다():
    result = _operator(EmrServerlessStartJobOperator).execute_complete(
        context={}, event=SUCCESS_EVENT
    )
    assert result == "job-1"


def test_provider_원본은_아직_KeyError_를_낸다(monkeypatch):
    """상류가 고쳐지면 이 테스트가 깨집니다 — 그때 하위 클래스를 지우세요.

    provider 는 `self.hook.conn.cancel_job_run(...)` 에서 **인수보다 먼저** boto3
    클라이언트를 만듭니다. region 이 없으면 `KeyError` 대신 `NoRegionError` 가 나서
    러너의 AWS 설정에 따라 결과가 갈립니다. 실제 Airflow 환경에는 region 이 있으므로
    그쪽을 재현하려고 region 만 넣어 줍니다. 자격증명은 필요 없습니다 — 인수 평가에서
    `KeyError` 가 나 API 호출까지 가지 않습니다.
    """
    monkeypatch.setenv("AWS_DEFAULT_REGION", "ap-northeast-2")
    with pytest.raises(KeyError, match="job_details"):
        _operator(ProviderOperator).execute_complete(context={}, event=FAILURE_EVENT)


def test_EMR_을_쓰는_DAG_은_모두_하위_클래스를_쓴다():
    """provider 를 직접 import 하면 그 DAG 만 조용히 KeyError 로 되돌아갑니다."""
    from pathlib import Path

    root = Path(__file__).resolve().parents[3]
    # 하위 클래스 자신은 provider 를 상속해야 하므로 예외입니다.
    subclass = root / "shared/airflow/common/emr_serverless.py"
    offenders = []
    for path in root.glob("*/airflow/**/*.py"):
        if ".venv" in path.parts or "tests" in path.parts or path == subclass:
            continue
        text = path.read_text(encoding="utf-8")
        if "from airflow.providers.amazon.aws.operators.emr import" in text:
            offenders.append(str(path.relative_to(root)))
    assert offenders == [], f"provider 를 직접 import 합니다: {offenders}"
