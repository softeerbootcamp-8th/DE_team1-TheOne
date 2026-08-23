"""예약어인 ``lambda`` 패키지의 핸들러를 동적으로 불러옵니다."""

import importlib


def lambda_handler_for(
    function_name: str,
    *,
    package: str = "main.aws_lambda.functions",
):
    module = importlib.import_module(f"{package}.{function_name}.handler")
    return module.lambda_handler
