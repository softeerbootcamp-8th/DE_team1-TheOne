"""Schema Validator 및 Slack Notifier 공용 모듈 단위 테스트."""

import io
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from shared.aws_lambda.common.schema_validator import validate_parquet_schema
from shared.aws_lambda.common.slack_notifier import SlackNotifier


def _create_parquet_bytes(schema: pa.Schema) -> bytes:
    """테스트용 빈 PyArrow Table Parquet 바이너리를 생성합니다."""
    table = schema.empty_table()
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


@pytest.fixture
def expected_schema():
    return pa.schema(
        [
            ("col_a", pa.string()),
            ("col_b", pa.int64()),
        ]
    )


def test_동일한_스키마는_diff가_없다(expected_schema):
    data = _create_parquet_bytes(expected_schema)
    result = validate_parquet_schema(data, expected_schema)
    assert result.diffs == ()
    assert result.errors == ()
    assert result.warnings == ()


def test_누락된_컬럼_감지(expected_schema):
    actual_schema = pa.schema([("col_a", pa.string())])
    data = _create_parquet_bytes(actual_schema)
    result = validate_parquet_schema(data, expected_schema)
    assert len(result.errors) == 1
    assert "❌ 누락된 컬럼: `col_b`" in result.missing_columns[0]


def test_신규_추가된_컬럼_감지(expected_schema):
    actual_schema = pa.schema(
        [
            ("col_a", pa.string()),
            ("col_b", pa.int64()),
            ("col_c", pa.float64()),
        ]
    )
    data = _create_parquet_bytes(actual_schema)
    result = validate_parquet_schema(data, expected_schema)
    assert result.errors == ()
    assert len(result.warnings) == 1
    assert "➕ 신규 추가된 컬럼: `col_c`" in result.extra_columns[0]


def test_타입_불일치_감지(expected_schema):
    actual_schema = pa.schema(
        [
            ("col_a", pa.string()),
            ("col_b", pa.float64()),  # int64 -> float64
        ]
    )
    data = _create_parquet_bytes(actual_schema)
    result = validate_parquet_schema(data, expected_schema)
    assert len(result.errors) == 1
    assert "⚠️ 타입 불일치 컬럼 `col_b`" in result.type_mismatches[0]


def test_손상된_바이너리_입력_시_파싱_실패_메시지_반환(expected_schema):
    data = b"invalid_parquet_bytes"
    result = validate_parquet_schema(data, expected_schema)
    assert len(result.errors) == 1
    assert "⚠️ Parquet 메타데이터 파싱 실패" in result.parse_error


def test_이미_읽은_PyArrow_스키마도_검증한다(expected_schema):
    result = validate_parquet_schema(expected_schema, expected_schema)

    assert result.diffs == ()


def test_slack_webhook_url이_없으면_알림을_보내지_않는다(monkeypatch):
    monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)
    diffs = ["❌ 누락된 컬럼: `test`"]
    notifier = SlackNotifier()
    assert notifier.is_enabled is False
    assert notifier.send_schema_drift_alert("hvfhv", "2026-08", diffs) is False


def test_slack_webhook_url이_있으면_post_요청을_보낸다(monkeypatch):
    webhook_url = "https://hooks.slack.com/services/test/mock"
    called = False

    class MockResponse:
        def raise_for_status(self):
            pass

    def mock_post(url, json, timeout):
        nonlocal called
        called = True
        assert url == webhook_url
        assert "[HVFHV]" in json["text"]
        return MockResponse()

    monkeypatch.setattr("requests.post", mock_post)
    diffs = ["❌ 누락된 컬럼: `test`"]
    notifier = SlackNotifier(webhook_url=webhook_url)
    assert notifier.is_enabled is True
    assert notifier.send_schema_drift_alert("hvfhv", "2026-08", diffs) is True
    assert called is True
