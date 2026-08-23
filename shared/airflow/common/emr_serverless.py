"""deferrable EMR Serverless 실패 사유가 `KeyError` 로 덮이는 것을 막습니다.

provider(`apache-airflow-providers-amazon==9.31.0`)의
`EmrServerlessStartJobOperator.execute_complete` 는 실패 경로에서도 성공 경로에만
있는 키를 읽습니다.

    # operators/emr.py:1421-1432
    if validated_event["status"] == "success":
        return validated_event["job_details"]["job_id"]
    self.log.info("Cancelling EMR Serverless job %s", self.job_id)
    self.hook.conn.cancel_job_run(
        applicationId=validated_event["job_details"]["application_id"],  # ← KeyError
        jobRunId=validated_event["job_details"]["job_id"],
    )
    raise AirflowException("EMR Serverless job failed or timed out in deferrable mode")

`job_details` 는 트리거가 **성공할 때만** 싣습니다. 실패는 사유 문자열만 옵니다.

    # triggers/emr.py:406 / :530
    return_key="job_details",                                  # 성공 경로 전용
    yield TriggerEvent({"status": "failure", "message": str(e)})

그래서 job 이 실패하면 항상 `KeyError: 'job_details'` 가 나고, 마지막 줄의
`AirflowException` 은 실행되지 못합니다. Airflow UI 와 Slack 알림에 뜨는 사유가
실제 원인이 아니라 `KeyError` 가 됩니다 — 예를 들어 이런 원인이 사라집니다.

    ExitCode: 137. Worker has been killed as memory usage exceeded configured
    memory size, consider increasing memory size

원인은 트리거 로그 한 줄 위에 남아 있지만, 온콜이 그걸 찾으려면 task 로그를 열어
`TriggerEvent` 줄을 눈으로 찾아야 합니다. 알림만 보고 1차 판단이 안 됩니다.

취소는 여기서 하지 않습니다. deferred 상태에서 재개된 operator 인스턴스에는
`self.job_id` 가 없고(로그에 `Cancelling EMR Serverless job None` 으로 남습니다)
실패 이벤트에도 식별자가 없어서, 애초에 취소할 대상을 특정할 수 없습니다. 사용자가
task 를 취소하는 경우는 트리거가 `job_id` 를 직렬화해 들고 있어 그쪽에서 처리합니다
(`triggers/emr.py` 의 `CancelledError` 핸들러와 `safe_to_cancel()`).
"""

from typing import Any

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.operators.emr import (
    EmrServerlessStartJobOperator as _ProviderOperator,
)
from airflow.providers.amazon.aws.utils import validate_execute_complete_event

# `airflow.utils.context` 는 deprecated 입니다 (DeprecatedImportWarning).
from airflow.sdk.definitions.context import Context


class EmrServerlessStartJobOperator(_ProviderOperator):
    """실패 사유를 그대로 실어 올리는 `EmrServerlessStartJobOperator`.

    성공 경로는 provider 구현을 그대로 씁니다 — 고칠 대상은 실패 경로뿐입니다.
    """

    def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> Any:
        validated_event = validate_execute_complete_event(event)
        if validated_event.get("status") == "success":
            return super().execute_complete(context, event)

        message = validated_event.get("message") or "(트리거가 사유를 싣지 않았습니다)"
        raise AirflowException(f"EMR Serverless job 실패: {message}")
