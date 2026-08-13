"""HVFHV Raw -> Bronze 배선 검증 (네트워크 없이 Loader만 실행)."""

from datetime import datetime, timezone
from pathlib import Path

import pytest

from functions.hvfhv_raw_to_bronze.handler import lambda_handler
from functions.hvfhv_raw_to_bronze.loader import HvfhvBronzeLoader

COLLECTED_AT = datetime(2026, 8, 10, 0, 0, tzinfo=timezone.utc)
CONTENT = b"PAR1_fake_parquet_bytes"


def test_원본_바이너리를_그대로_쓴다(tmp_path):
    """Bronze 는 원본 보존이 목적이라 파싱하지 않습니다."""
    result = HvfhvBronzeLoader(str(tmp_path), "2026-08", COLLECTED_AT).write(CONTENT)

    path = Path(result.location)
    assert path.read_bytes() == CONTENT
    assert path.parent.name == "year_month=2026-08"
    assert path.parent.parent.name == "hvfhv"


def test_같은_날_다시_받아도_덮어쓰지_않는다(tmp_path):
    """파일명이 수집 시각이라 재시도분이 따로 쌓입니다."""
    first = HvfhvBronzeLoader(str(tmp_path), "2026-08", COLLECTED_AT).write(CONTENT).location
    second = (
        HvfhvBronzeLoader(str(tmp_path), "2026-08", COLLECTED_AT.replace(hour=3))
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


# --- 원본 공개 여부 확인 (#345) -------------------------------------------
#
# TLC 는 두 달쯤 늦게 공개합니다. 받기 전에 있는지 물어봐야 스케줄 실행이 매번
# 죽지 않습니다. 네트워크를 타지 않도록 `requests.head` 만 대체합니다.


class FakeResponse:
    def __init__(self, status_code: int):
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


@pytest.mark.parametrize(
    ("status_code", "expected"),
    [(200, True), (403, False), (404, False)],
)
def test_공개_여부를_상태코드로_판단한다(monkeypatch, status_code, expected):
    from functions.hvfhv_raw_to_bronze import extractor

    monkeypatch.setattr(
        extractor.requests, "head", lambda *a, **kw: FakeResponse(status_code)
    )

    assert extractor.is_available("2026", "07") is expected


def test_서버_오류는_아직_없음_으로_삼키지_않는다(monkeypatch):
    """삼키면 일시 장애가 '미공개'로 둔갑해 조용히 아무것도 안 하게 됩니다."""
    from functions.hvfhv_raw_to_bronze import extractor

    monkeypatch.setattr(extractor.requests, "head", lambda *a, **kw: FakeResponse(500))

    with pytest.raises(RuntimeError):
        extractor.is_available("2026", "07")
