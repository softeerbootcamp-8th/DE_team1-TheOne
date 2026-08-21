"""로컬 개발 시 저장소 루트의 .env 파일에서 환경변수를 읽어옵니다."""

import logging
import os
from pathlib import Path

try:
    from dotenv import load_dotenv
except ModuleNotFoundError:  # pragma: no cover - 런타임에 따라 갈립니다
    # 세 런타임 중 **spark 에는 `dotenv` 가 없습니다**(airflow·aws_lambda 는 전이
    # 의존으로 들어옵니다 — 어디도 직접 선언하지는 않습니다). 그대로 최상단에서
    # import 하면 spark 쪽에서 이 헬퍼를 쓰는 코드가 ModuleNotFoundError 로 죽습니다.
    # 모듈 속성으로는 남겨둡니다 — 테스트가 이 이름을 patch 합니다.
    load_dotenv = None

logger = logging.getLogger(__name__)

# shared/common/env.py 기준: [0]=common [1]=shared [2]=저장소 루트.
# 예전에는 [4] 였는데 그건 저장소 **바깥**이라, load_dotenv 가 없는 경로에 조용히
# 실패하며 아무것도 안 읽었습니다 (#536).
_ENV_FILE = Path(__file__).resolve().parents[2] / ".env"


def load_local_env() -> None:
    """`.env` 를 읽어 환경변수를 채웁니다.

    이 함수는 **로컬 편의 기능**입니다 — 컨테이너·Lambda 에서는 환경변수가 이미
    주입돼 있습니다. 그래서 `dotenv` 가 없는 런타임(spark)에서는 건너뜁니다.
    값이 실제로 없으면 그것을 요구하는 쪽에서 무엇을 설정해야 하는지 알려주며
    실패하므로, 여기서 막지 않아도 조용히 묻히지 않습니다.
    """
    if os.getenv("AWS_LAMBDA_FUNCTION_NAME"):
        return
    if load_dotenv is None:
        logger.info("python-dotenv 가 없어 %s 를 읽지 않습니다 (환경변수는 그대로 사용)", _ENV_FILE)
        return
    load_dotenv(_ENV_FILE)
