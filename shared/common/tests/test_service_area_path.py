"""지역(`service_area=`) 경로 계층의 삽입 규칙. 이슈 #839, #674.

1. `None` 이면 지역 세그먼트가 빈 문자열 — 지금 경로와 완전히 같아야 한다
2. 지역을 주면 `service_area=<sa>` 세그먼트가 나온다
3. 잘못된 지역 코드는 거부한다 — 특히 구분자·소문자
4. `join_segments` 가 빈 세그먼트 때문에 `//` 를 만들지 않는다
5. 읽는 쪽 후보는 **지역 경로 우선, 지역 없는 경로 폴백** 순서다

5번이 이 모듈의 존재 이유입니다. 이 폴백이 있어야 데이터셋별로 writer 를 하나씩
옮길 수 있고(#840~#848), 폴백 순서가 뒤집히면 이미 옮긴 데이터셋이 **옛 경로의 낡은
데이터를 조용히 집어갑니다.**
"""

import pytest

from shared.common.service_area_path import (
    candidate_segments,
    join_segments,
    service_area_segment,
    validate_service_area,
)


def test_지역이_없으면_세그먼트가_빈_문자열이다():
    """이게 이 이슈 전체의 안전망입니다 — 빈 문자열이라야 호출부가 분기 없이
    조립해도 지금과 같은 경로가 나옵니다."""
    assert service_area_segment(None) == ""


def test_지역을_주면_service_area_세그먼트가_나온다():
    assert service_area_segment("NYC") == "service_area=NYC"
    assert service_area_segment("LA_METRO") == "service_area=LA_METRO"


@pytest.mark.parametrize(
    "service_area",
    ["nyc", "NYC/TX", "NYC=1", "1NYC", "", "  ", "N-YC"],
    ids=["소문자", "슬래시", "등호", "숫자시작", "빈값", "공백", "하이픈"],
)
def test_잘못된_지역_코드는_거부한다(service_area):
    """슬래시·등호를 허용하면 경로 계층이 하나 더 생기거나 파티션 키 파싱이
    엉뚱한 값을 돌려줍니다."""
    with pytest.raises(ValueError, match="service_area"):
        service_area_segment(service_area)


def test_유효한_코드는_그대로_돌려준다():
    assert validate_service_area("NYC") == "NYC"


def test_빈_세그먼트는_이중_슬래시를_만들지_않는다():
    """지역이 없을 때 `bronze//year_month=...` 가 되면 S3 키가 달라집니다."""
    assert join_segments("bronze", "ds", "", "year_month=2026-08") == (
        "bronze/ds/year_month=2026-08"
    )
    assert join_segments("bronze", "ds", "service_area=NYC", "year_month=2026-08") == (
        "bronze/ds/service_area=NYC/year_month=2026-08"
    )


def test_지역이_없으면_후보는_지역없는_경로_하나다():
    assert candidate_segments(None) == (None,)


def test_지역이_있으면_지역경로를_먼저_보고_없으면_지역없는_경로를_본다():
    """순서가 뒤집히면 이미 옮긴 데이터셋이 옛 경로의 낡은 데이터를 집어갑니다."""
    candidates = candidate_segments("NYC")

    assert candidates == ("service_area=NYC", None)
    assert candidates[0] == "service_area=NYC", "지역 경로가 먼저여야 한다"
    assert candidates[-1] is None, "지역 없는 경로가 폴백이어야 한다"
