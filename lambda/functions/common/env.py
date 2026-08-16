"""로컬 개발 시 저장소 루트의 .env 파일에서 환경변수를 읽어옵니다.
"""

import os
from pathlib import Path

from dotenv import load_dotenv

_ENV_FILE = Path(__file__).resolve().parents[3] / ".env"


def load_local_env() -> None:
    if os.getenv("AWS_LAMBDA_FUNCTION_NAME"):
        return
    load_dotenv(_ENV_FILE)
