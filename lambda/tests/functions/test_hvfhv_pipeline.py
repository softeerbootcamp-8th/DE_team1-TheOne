"""HVFHV Raw -> Bronze 배선 검증 (네트워크 없이 Loader만 실행)."""

from datetime import datetime, timezone
from pathlib import Path

import pytest

from functions.hvfhv.handler import lambda_handler
from functions.hvfhv.loader import HvfhvBronzeLoader

COLLECTED_AT = datetime(2026, 8, 10, 0, 0, tzinfo=timezone.utc)
CONTENT = b"PAR1_fake_parquet_bytes"


def test_원본_바이너리를_그대로_쓴다(tmp_path):
    """Bronze 는 원본 보존이 목적이라 파싱하지 않습니다."""
    result = HvfhvBronzeLoader(str(tmp_path), COLLECTED_AT).write(CONTENT)

    path = Path(result.location)
    assert path.read_bytes() == CONTENT
    assert path.parent.name == "collected_date=2026-08-10"
    assert path.parent.parent.name == "hvfhv"


def test_같은_날_다시_받아도_덮어쓰지_않는다(tmp_path):
    """파일명이 수집 시각이라 재시도분이 따로 쌓입니다.

    Silver 가 collected_date 파티션 전체를 읽으므로 이 동작이 중복 집계로
    이어집니다. 파이프라인 차원의 문제라 별도 이슈로 다룹니다.
    """
    first = HvfhvBronzeLoader(str(tmp_path), COLLECTED_AT).write(CONTENT).location
    second = (
        HvfhvBronzeLoader(str(tmp_path), COLLECTED_AT.replace(hour=3))
        .write(CONTENT)
        .location
    )

    assert first != second
    assert len(list(Path(first).parent.glob("*.parquet"))) == 2


@pytest.mark.parametrize(
    "event",
    [
        {"month": "08"},
        {"year": "2026"},
        {},
    ],
)
def test_연월이_없으면_수집_전에_실패한다(event, monkeypatch):
    monkeypatch.delenv("YEAR", raising=False)
    monkeypatch.delenv("MONTH", raising=False)

    with pytest.raises(ValueError, match="year와 month"):
        lambda_handler(event)
