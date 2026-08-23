"""수집 시각 해석 시나리오 (#585).

크롤러 4종이 `datetime.now()` 로 고정돼 있어 지정 일자 수집이 불가능했습니다.
파티션 키와 행의 `collected_at` 이 **함께** 움직여야 Bronze 검증을 통과합니다.

1. 지정하면 그 날 00:00 UTC — 같은 날 두 번 돌려도 값이 같아야 함
2. 비우면 현재 시각
3. 형식이 틀리면 명시적으로 실패 — 조용히 오늘로 떨어지면 백필이 엉뚱한 곳에 쌓임
4. date 객체도 그대로 받음 (Airflow 가 파싱해 넘기는 경우)
"""

from datetime import date, datetime, timezone

import pytest

from shared.aws_lambda.common.collected_at import resolve_collected_at

NOW = datetime(2026, 8, 20, 14, 37, tzinfo=timezone.utc)


def test_지정하면_그_날_자정_UTC_로_고정된다():
    assert resolve_collected_at({"collected_date": "2026-05-01"}, now=NOW) == datetime(
        2026, 5, 1, 0, 0, tzinfo=timezone.utc
    )


def test_같은_날_두_번_돌려도_같은_값이다():
    """실행 시각을 쓰면 재실행마다 행의 collected_at 이 달라집니다."""
    first = resolve_collected_at({"collected_date": "2026-05-01"})
    second = resolve_collected_at({"collected_date": "2026-05-01"})

    assert first == second


@pytest.mark.parametrize("event", [None, {}, {"collected_date": None}, {"collected_date": ""}])
def test_비우면_현재_시각을_쓴다(event):
    assert resolve_collected_at(event, now=NOW) == NOW


@pytest.mark.parametrize("bad", ["2026-5-1", "20260501", "abc", "2026-13-01"])
def test_형식이_틀리면_실패한다(bad):
    """조용히 오늘로 떨어지면 백필이 엉뚱한 파티션에 쌓입니다."""
    with pytest.raises(ValueError, match="YYYY-MM-DD"):
        resolve_collected_at({"collected_date": bad}, now=NOW)


def test_date_객체도_받는다():
    assert resolve_collected_at({"collected_date": date(2026, 5, 1)}, now=NOW) == datetime(
        2026, 5, 1, 0, 0, tzinfo=timezone.utc
    )


def test_돌려준_값은_시간대를_갖는다():
    """Bronze 검증이 collected_at 의 tz 가 UTC 인지 봅니다."""
    for event in ({"collected_date": "2026-05-01"}, {}):
        assert resolve_collected_at(event).tzinfo is timezone.utc
