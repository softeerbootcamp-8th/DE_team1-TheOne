"""예약어인 ``lambda`` 패키지의 핸들러를 동적으로 불러옵니다."""

import importlib


def lambda_handler_for(function_name: str):
    module = importlib.import_module(f"lambda.functions.{function_name}.handler")
    return module.lambda_handler
