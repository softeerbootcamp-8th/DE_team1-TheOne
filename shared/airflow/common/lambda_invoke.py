"""Lambda 를 운영에서는 원격 호출하고, 로컬에서는 프로세스 안에서 실행합니다.

왜 원격으로 옮기나
----------------
`lambda_handler_for` 는 핸들러를 `importlib` 로 불러 **Airflow 프로세스 안에서**
실행합니다. 수집 함수는 외부 사이트를 크롤링하고 파일을 만들기 때문에, 그대로 두면
스케줄러와 자원을 공유합니다 — 한 수집이 메모리를 먹으면 다른 DAG 이 같이 죽습니다.
AWS 에 이미 배포된 함수를 호출하면 그 경계가 생깁니다.

로컬 실행을 남겨 두는 이유
----------------------
테스트와 로컬 개발이 AWS 자격증명 없이 돌아야 합니다. `LAMBDA_INVOKE` 가 `remote`
일 때만 원격으로 가고, 기본값은 로컬입니다.
"""

import json
import logging
import os

from shared.airflow.common.lambda_runtime import lambda_handler_for


logger = logging.getLogger(__name__)

INVOKE_MODE_ENV = "LAMBDA_INVOKE"
REMOTE = "remote"

# 가장 긴 함수가 300초입니다 (vehicle_catalog_source_to_raw). boto3 기본 읽기
# 타임아웃은 60초라 그대로 두면 **함수는 성공하는데 호출부만 끊깁니다** — 결과를
# 못 받아 태스크가 실패하고, 재시도하면 같은 수집이 두 번 돕니다.
READ_TIMEOUT_SECONDS = 330


def invoke_lambda(
    function_name: str,
    *,
    package: str,
    event: dict,
    local_event: dict | None = None,
) -> dict:
    """`event` 를 그 함수에 넘기고 응답 dict 를 그대로 돌려줍니다.

    `local_event` 는 **로컬 실행에만** 더하는 키입니다. 로컬 파일시스템 경로가
    대표적입니다 — 원격으로 보내면 안 됩니다. 핸들러가 `event.get("base_dir") or
    os.getenv("RAW_DIR")` 순서로 읽어서 이벤트가 Lambda 자신의 설정을 이기고,
    `vehicle_catalog` 는 HTML 스냅샷과 이미지를 그 경로에 직접 쓰므로 Lambda 의
    read-only 파일시스템에서 실패합니다.
    """
    if os.getenv(INVOKE_MODE_ENV, "local").strip().lower() != REMOTE:
        merged = {**event, **(local_event or {})}
        return lambda_handler_for(function_name, package=package)(event=merged)
    return _invoke_remote(function_name, event)


def _invoke_remote(function_name: str, event: dict) -> dict:
    import boto3
    from botocore.config import Config

    client = boto3.client(
        "lambda",
        config=Config(
            connect_timeout=10,
            read_timeout=READ_TIMEOUT_SECONDS,
            # RequestResponse 재시도는 같은 수집을 두 번 돌립니다. 재시도는 Airflow 가
            # 맡습니다 (태스크 retries + exponential backoff).
            retries={"max_attempts": 0},
        ),
    )
    logger.info("Lambda 원격 호출: %s event=%s", function_name, event)
    response = client.invoke(
        FunctionName=function_name,
        InvocationType="RequestResponse",
        Payload=json.dumps(event, ensure_ascii=False).encode("utf-8"),
    )
    payload = response["Payload"].read().decode("utf-8")

    # 핸들러가 예외로 죽으면 StatusCode 는 200 이고 FunctionError 에 표시됩니다.
    # 이걸 안 보면 실패가 성공으로 지나갑니다.
    if response.get("FunctionError"):
        raise RuntimeError(
            f"Lambda {function_name} 실패 ({response['FunctionError']}): {payload[:2000]}"
        )
    if response["StatusCode"] != 200:
        raise RuntimeError(
            f"Lambda {function_name} 이 {response['StatusCode']} 를 돌려줬습니다: {payload[:500]}"
        )

    result = json.loads(payload) if payload.strip() else {}
    if not isinstance(result, dict):
        raise RuntimeError(
            f"Lambda {function_name} 응답이 dict 가 아닙니다: {type(result).__name__}"
        )
    logger.info("Lambda 원격 호출 완료: %s -> %s", function_name, result)
    return result
