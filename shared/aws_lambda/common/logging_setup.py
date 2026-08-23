"""Lambda 핸들러 공통 로깅 설정.

AWS Lambda는 부팅 때 이미 루트 핸들러를 붙여두기 때문에 `logging.basicConfig()`가
no-op이 됩니다(포맷이 적용되지 않습니다). 실제로 필요한 건 레벨뿐입니다.

Airflow가 핸들러를 in-process로 import할 때는 Airflow가 로깅의 주인이므로
아무것도 하지 않습니다.
"""

import logging
import os


def configure_lambda_logging() -> None:
    """Lambda 런타임에서 실행 중일 때만 루트 로거 레벨을 정합니다."""
    if os.getenv("AWS_LAMBDA_FUNCTION_NAME"):
        logging.getLogger().setLevel(os.getenv("LOG_LEVEL", "INFO"))
