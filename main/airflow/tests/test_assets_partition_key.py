"""Asset 파티션 키 규약 (#674, #807).

파티션 키는 `"{service_area}:{year_month}"` 복합 문자열입니다. Airflow 에 다차원
파티션 키가 없어서(`add_partitions` 는 `str | list[str]` 만 받고
`DagRun.partition_key` 도 문자열 컬럼 하나) 한 문자열로 합칩니다 — API 제약이지
설계 선호가 아닙니다.

이 규약이 깨지는 방식은 정해져 있습니다.

1. 생산자와 소비자 중 한쪽만 바뀌면 Gold 가 **아무 에러 없이** 안 돕니다.
   그래서 지역 성분 없는 옛 키는 조용히 받아주지 않고 요란하게 실패시킵니다.
2. 구분자를 값에 넣을 수 있으면 파싱이 조용히 엉뚱한 값을 돌려줍니다.
3. 두 지역의 같은 달이 같은 키가 되면 "지역별 독립" 자체가 성립하지 않습니다.
"""

import pytest

from main.airflow.common.assets import (
    DEFAULT_SERVICE_AREA,
    build_partition_key,
    parse_partition_key,
    resolve_service_area,
)


def test_지역과_월을_복합키로_합친다():
    assert build_partition_key("NYC", "2026-08") == "NYC:2026-08"


def test_복합키를_지역과_월로_다시_나눈다():
    assert parse_partition_key("NYC:2026-08") == ("NYC", "2026-08")


def test_합치고_나누면_원래_값이_나온다():
    for service_area, year_month in [
        ("NYC", "2026-01"),
        ("TX", "2026-12"),
        ("LA_METRO", "2030-06"),
    ]:
        key = build_partition_key(service_area, year_month)

        assert parse_partition_key(key) == (service_area, year_month)


def test_다른_지역의_같은_달은_다른_키다():
    """이게 깨지면 "지역별로 독립적으로 트리거된다" 는 주장 자체가 성립하지 않습니다."""
    assert build_partition_key("NYC", "2026-08") != build_partition_key(
        "TX", "2026-08"
    )


def test_지역_성분이_없는_옛_키는_받지_않는다():
    """조용히 기본 지역으로 넘기면 "생산자를 아직 안 고쳤다" 는 사실이 묻힙니다."""
    with pytest.raises(ValueError, match="지역 성분이 없습니다"):
        parse_partition_key("2026-08")


@pytest.mark.parametrize(
    "service_area",
    ["nyc", "NYC:extra", "1NYC", "", "  ", "N-YC"],
    ids=["소문자", "구분자_포함", "숫자_시작", "빈값", "공백", "하이픈"],
)
def test_잘못된_지역_코드는_거부한다(service_area):
    """특히 `"NYC:extra"` 를 허용하면 키가 `"NYC:extra:2026-08"` 이 되고
    `partition` 파싱이 지역만 떼어 조용히 엉뚱한 월을 돌려줍니다."""
    with pytest.raises(ValueError, match="service_area"):
        build_partition_key(service_area, "2026-08")


@pytest.mark.parametrize(
    "year_month",
    ["2026-13", "2026-00", "26-08", "2026-8", "2026/08", ""],
    ids=["13월", "0월", "두자리연도", "한자리월", "슬래시", "빈값"],
)
def test_잘못된_연월은_거부한다(year_month):
    with pytest.raises(ValueError, match="year_month"):
        build_partition_key("NYC", year_month)


def test_파라미터가_비면_기본_지역을_쓴다():
    assert resolve_service_area({}) == DEFAULT_SERVICE_AREA
    assert resolve_service_area({"service_area": None}) == DEFAULT_SERVICE_AREA
    assert resolve_service_area({"service_area": "  "}) == DEFAULT_SERVICE_AREA


def test_파라미터로_준_지역을_쓴다():
    assert resolve_service_area({"service_area": "TX"}) == "TX"


def test_파라미터의_잘못된_지역_코드는_거부한다():
    """기본값으로 조용히 되돌리면 오타가 다른 지역의 데이터를 오염시킵니다."""
    with pytest.raises(ValueError, match="service_area"):
        resolve_service_area({"service_area": "nyc"})
