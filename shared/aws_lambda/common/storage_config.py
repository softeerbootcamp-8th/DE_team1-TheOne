"""저장 위치(storage) 설정 해석.

10개 수집 핸들러가 같은 규칙을 써야 해서 한 곳에 둡니다. 각자 `os.getenv(..., "local")`
를 쓰면 배포 설정 누락이 조용히 로컬 폴백으로 가려집니다.

배포된 Lambda 런타임에서 저장 위치가 이벤트·환경변수 어디에도 없으면 컨테이너 휘발
파일시스템에 쓰고 성공으로 끝납니다 — 데이터가 사라지는데 파이프라인은 초록불인
실패라서, Lambda 에서는 기본 폴백을 금지합니다. 로컬 실행(docker compose·pytest)에는
`AWS_LAMBDA_FUNCTION_NAME` 이 없으므로 현행 local 기본값이 그대로 유지됩니다.
"""

import os


def resolve_storage(
    event: dict | None,
    *,
    env_key: str = "BRONZE_STORAGE",
    local_default: str = "local",
) -> str:
    """이벤트 → 환경변수 순으로 storage 를 골라 돌려줍니다.

    Lambda 런타임에서는 둘 다 없으면 ValueError 로 실패하고, 그 외(로컬)에서는
    `local_default` 를 돌려줍니다.
    """
    storage = (event or {}).get("storage") or os.getenv(env_key)
    if storage:
        return storage
    if os.getenv("AWS_LAMBDA_FUNCTION_NAME"):
        raise ValueError(
            f"배포된 Lambda 에는 {env_key} 환경변수나 event.storage 가 필요합니다"
        )
    return local_default
