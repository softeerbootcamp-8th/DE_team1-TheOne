"""Spark 지역(`service_area=`) 경로 계층의 삽입 규칙.

1. `None` 이면 기존 경로 유지
2. 지역을 주면 `service_area=<sa>` 세그먼트 생성
3. 잘못된 지역 코드 거부
4. 읽기 후보는 지역 경로 우선, 지역 없는 경로 폴백
"""

import pytest

from main.spark.jobs.service_area_path import (
    candidate_segments,
    join_segments,
    service_area_segment,
)


def test_지역이_없으면_세그먼트가_빈_문자열이다():
    assert service_area_segment(None) == ""


def test_지역을_주면_service_area_세그먼트가_나온다():
    assert service_area_segment("NYC") == "service_area=NYC"
    assert service_area_segment("LA_METRO") == "service_area=LA_METRO"


@pytest.mark.parametrize(
    "service_area",
    ["nyc", "NYC/TX", "NYC=1", "1NYC", "", "  ", "N-YC"],
)
def test_잘못된_지역_코드는_거부한다(service_area):
    with pytest.raises(ValueError, match="service_area"):
        service_area_segment(service_area)


def test_빈_세그먼트는_이중_슬래시를_만들지_않는다():
    assert join_segments("bronze", "ds", "", "year_month=2026-08") == (
        "bronze/ds/year_month=2026-08"
    )


def test_지역경로를_먼저_보고_없으면_지역없는_경로를_본다():
    assert candidate_segments("NYC") == ("service_area=NYC", None)
