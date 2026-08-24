"""Sub 수집 Lambda 원격 호출 계약.

1. source_to_raw 는 원격 호출기를 쓰고 raw_to_curated 는 프로세스 실행을 유지
2. 로컬 모드는 AWS 없이 핸들러를 그대로 실행하고 local_event 를 합침
3. 원격 모드는 local_event 를 보내지 않음 — 보내면 Lambda 가 없는 경로에 씀
4. 핸들러 예외(FunctionError)는 태스크 실패로 올라감
5. 재시도를 boto3 에 맡기지 않음 — 같은 수집이 두 번 돌지 않게
6. 읽기 타임아웃이 가장 긴 함수보다 길음
7. 운영 compose 가 원격 모드를 켬
"""

import io
import json
from pathlib import Path

import pytest

from shared.airflow.common import lambda_invoke


ROOT = Path(__file__).resolve().parents[3]
PIPELINES = {
    "fueleconomy_vehicle_specs": "fueleconomy_vehicle_specs_source_to_raw",
    "vehicle_catalog": "vehicle_catalog_source_to_raw",
    "uber_eligible_vehicles": "uber_eligible_vehicles_source_to_raw",
    "lyft_eligible_vehicles": "lyft_eligible_vehicles_source_to_raw",
}
# AWS 에 배포된 함수 중 가장 긴 타임아웃 (vehicle_catalog_source_to_raw).
LONGEST_FUNCTION_TIMEOUT_SECONDS = 300


@pytest.mark.parametrize("pipeline", sorted(PIPELINES))
def test_source_to_raw만_원격_호출로_바뀐다(pipeline):
    """`*_raw_to_curated` 는 그대로 프로세스에서 돌립니다 (이슈 완료 조건)."""
    source = (
        ROOT / "sub/airflow/scripts" / f"{pipeline}_raw_to_curated/tasks.py"
    ).read_text()

    assert source.count("invoke_lambda(") == 1, "source_to_raw 하나만 원격입니다"
    assert source.count("lambda_handler_for(") == 1, "raw_to_curated 는 유지합니다"
    assert f'"{PIPELINES[pipeline]}"' in source


@pytest.mark.parametrize("pipeline", sorted(PIPELINES))
def test_로컬_경로를_원격으로_보내지_않는다(pipeline):
    """핸들러가 `event.get("base_dir") or os.getenv("RAW_DIR")` 순서로 읽습니다.

    이벤트가 Lambda 자신의 설정을 이기므로, 로컬 경로를 보내면 원격에서 그 경로에
    쓰려 합니다. `vehicle_catalog` 는 HTML 스냅샷과 이미지를 거기 직접 써서
    read-only 파일시스템에 걸립니다.
    """
    source = (
        ROOT / "sub/airflow/scripts" / f"{pipeline}_raw_to_curated/tasks.py"
    ).read_text()
    call = source.split("invoke_lambda(")[1].split("logger.info")[0]

    assert 'local_event={"base_dir"' in call, "base_dir 은 local_event 로 넘겨야 합니다"
    event = call.split("event={")[1].split("}")[0]
    assert "base_dir" not in event, "원격 이벤트에 로컬 경로가 있습니다"


def test_로컬_모드는_AWS_없이_핸들러를_실행한다(monkeypatch):
    monkeypatch.delenv(lambda_invoke.INVOKE_MODE_ENV, raising=False)
    seen = {}

    def fake_handler(event=None):
        seen.update(event or {})
        return {"row_count": 1}

    monkeypatch.setattr(
        lambda_invoke, "lambda_handler_for", lambda *a, **k: fake_handler
    )

    result = lambda_invoke.invoke_lambda(
        "any", package="p", event={"collected_date": "2026-08-01"},
        local_event={"base_dir": "/local/raw"},
    )

    assert result == {"row_count": 1}
    # 로컬에서는 두 dict 가 합쳐져야 기존 동작이 유지됩니다.
    assert seen == {"collected_date": "2026-08-01", "base_dir": "/local/raw"}


class _FakeLambda:
    def __init__(self, payload, *, function_error=None, status=200):
        self._payload = payload
        self._error = function_error
        self._status = status
        self.calls = []

    def invoke(self, **kwargs):
        self.calls.append(kwargs)
        response = {
            "StatusCode": self._status,
            "Payload": io.BytesIO(json.dumps(self._payload).encode()),
        }
        if self._error:
            response["FunctionError"] = self._error
        return response


def _patch_boto(monkeypatch, fake):
    captured = {}

    class _Config:
        def __init__(self, **kwargs):
            captured.update(kwargs)

    monkeypatch.setitem(
        __import__("sys").modules, "boto3",
        type("m", (), {"client": staticmethod(lambda *a, **k: fake)}),
    )
    monkeypatch.setitem(
        __import__("sys").modules, "botocore.config",
        type("m", (), {"Config": _Config}),
    )
    return captured


def test_원격_모드는_이벤트를_그대로_넘긴다(monkeypatch):
    monkeypatch.setenv(lambda_invoke.INVOKE_MODE_ENV, "remote")
    fake = _FakeLambda({"row_count": 7, "collected_date": "2026-08-01"})
    _patch_boto(monkeypatch, fake)

    result = lambda_invoke.invoke_lambda(
        "vehicle_catalog_source_to_raw", package="p",
        event={"collected_date": "2026-08-01"},
        local_event={"base_dir": "/opt/airflow/data"},
    )

    assert result == {"row_count": 7, "collected_date": "2026-08-01"}
    sent = json.loads(fake.calls[0]["Payload"].decode())
    assert sent == {"collected_date": "2026-08-01"}, "local_event 이 원격으로 갔습니다"
    assert fake.calls[0]["InvocationType"] == "RequestResponse"


def test_핸들러_예외는_태스크_실패로_올라간다(monkeypatch):
    """StatusCode 는 200 이고 FunctionError 에만 표시됩니다. 안 보면 실패가 성공이 됩니다."""
    monkeypatch.setenv(lambda_invoke.INVOKE_MODE_ENV, "remote")
    fake = _FakeLambda(
        {"errorMessage": "boom", "errorType": "RuntimeError"},
        function_error="Unhandled",
    )
    _patch_boto(monkeypatch, fake)

    with pytest.raises(RuntimeError, match="Unhandled"):
        lambda_invoke.invoke_lambda("f", package="p", event={})


def test_boto3_재시도를_끈다(monkeypatch):
    """RequestResponse 재시도는 같은 수집을 두 번 돌립니다. 재시도는 Airflow 가 합니다."""
    monkeypatch.setenv(lambda_invoke.INVOKE_MODE_ENV, "remote")
    captured = _patch_boto(monkeypatch, _FakeLambda({}))

    lambda_invoke.invoke_lambda("f", package="p", event={})

    assert captured["retries"] == {"max_attempts": 0}


def test_읽기_타임아웃이_가장_긴_함수보다_길다():
    """boto3 기본값 60초로는 함수는 성공하는데 호출부만 끊깁니다."""
    assert lambda_invoke.READ_TIMEOUT_SECONDS > LONGEST_FUNCTION_TIMEOUT_SECONDS


def test_운영_compose_가_원격_모드를_켠다():
    compose = (ROOT / "docker-compose.ec2.yml").read_text()

    assert "LAMBDA_INVOKE: remote" in compose
