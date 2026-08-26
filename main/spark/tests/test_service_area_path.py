"""Spark 지역(`service_area=`) 경로 계층의 삽입 규칙.

1. `service_area` 는 필수 인자
2. 지역을 주면 `service_area=<sa>` 세그먼트 생성
3. 잘못된 지역 코드 거부
4. 읽기 후보는 지역 경로 하나뿐
"""

import pytest

from main.spark.jobs.service_area_path import (
    join_segments,
    service_area_prefix,
    service_area_root,
    service_area_segment,
)


def test_service_area는_필수인자다():
    with pytest.raises(TypeError):
        service_area_segment()


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


def test_읽기경로는_지역계층만_반환한다(tmp_path):
    assert service_area_root(tmp_path, "NYC") == tmp_path / "service_area=NYC"
    assert service_area_prefix(
        "silver", "dataset", service_area="NYC"
    ) == "silver/dataset/service_area=NYC"
