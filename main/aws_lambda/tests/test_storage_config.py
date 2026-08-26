"""저장 위치 설정 해석 시나리오 (#1083).

1. event.storage 가 환경변수보다 우선
2. BRONZE_STORAGE 환경변수로 폴백
3. 배포된 Lambda(AWS_LAMBDA_FUNCTION_NAME 존재)에서 미설정이면 ValueError
4. 로컬에서 미설정이면 local 기본값 유지
"""

import pytest

from shared.aws_lambda.common.storage_config import resolve_storage


def test_event_storage가_환경변수보다_우선한다(monkeypatch):
    monkeypatch.setenv("BRONZE_STORAGE", "s3")

    assert resolve_storage({"storage": "local"}) == "local"


def test_환경변수로_폴백한다(monkeypatch):
    monkeypatch.setenv("BRONZE_STORAGE", "s3")

    assert resolve_storage({}) == "s3"


def test_Lambda_런타임에서는_미설정이면_실패한다(monkeypatch):
    monkeypatch.setenv("AWS_LAMBDA_FUNCTION_NAME", "collect-fn")
    monkeypatch.delenv("BRONZE_STORAGE", raising=False)

    with pytest.raises(ValueError, match="BRONZE_STORAGE"):
        resolve_storage({})


def test_로컬에서는_local_기본값을_유지한다(monkeypatch):
    monkeypatch.delenv("AWS_LAMBDA_FUNCTION_NAME", raising=False)
    monkeypatch.delenv("BRONZE_STORAGE", raising=False)

    assert resolve_storage({}) == "local"
