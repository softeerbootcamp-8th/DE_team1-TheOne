"""로컬 개발 시 저장소 루트의 .env 파일에서 환경변수를 읽어옵니다.
"""

import os
from pathlib import Path

from dotenv import load_dotenv

# shared/common/env.py 기준: [0]=common [1]=shared [2]=저장소 루트.
# 예전에는 [4] 였는데 그건 저장소 **바깥**이라, load_dotenv 가 없는 경로에 조용히
# 실패하며 아무것도 안 읽었습니다 (#536).
_ENV_FILE = Path(__file__).resolve().parents[2] / ".env"


def load_local_env() -> None:
    if os.getenv("AWS_LAMBDA_FUNCTION_NAME"):
        return
    load_dotenv(_ENV_FILE)
