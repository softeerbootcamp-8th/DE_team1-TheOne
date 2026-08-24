"""운영 Airflow에서 Lambda를 동기 호출하고 JSON 응답을 XCom dict로 돌려줍니다."""

import json

from airflow.providers.amazon.aws.operators.lambda_function import (
    LambdaInvokeFunctionOperator,
)


def templated_json_payload(**expressions: str) -> str:
    """Jinja 표현식을 렌더링 뒤에도 유효한 JSON payload로 만듭니다."""
    fields = [
        f"{json.dumps(key)}: {{{{ {expression} | tojson }}}}"
        for key, expression in expressions.items()
    ]
    return "{" + ", ".join(fields) + "}"


class JsonLambdaInvokeFunctionOperator(LambdaInvokeFunctionOperator):
    """AWS 응답 문자열을 기존 TaskFlow XCom 계약인 dict로 복원합니다."""

    def execute(self, context):
        payload = super().execute(context)
        try:
            result = json.loads(payload)
        except (TypeError, json.JSONDecodeError) as exc:
            raise ValueError("Lambda 응답 payload가 JSON이 아닙니다") from exc
        if not isinstance(result, dict):
            raise ValueError("Lambda 응답 payload가 JSON 객체가 아닙니다")
        return result
