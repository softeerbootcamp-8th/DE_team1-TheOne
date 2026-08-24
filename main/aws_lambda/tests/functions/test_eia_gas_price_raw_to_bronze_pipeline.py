"""EIA 휘발유 원본 Raw→Bronze 적재의 지역(service_area) 처리 시나리오. 이슈 #843.

1. service_area별로 Bronze 경로가 service_area=<sa>/collected_date=.../ 로 나간다
2. FILE_URL_DICT에 없는 지역은 즉시 ValueError
3. 지역별로 실제 다른 URL을 쓴다 (지역 코드가 URL에 반영 안 되면 지역 구분이
   이름표만 있고 실제로는 전부 같은 데이터를 받아오게 됨)
4. 같은 지역에서 재수집해도 내용이 같으면 새 파티션을 만들지 않는다 (dedup)
5. 다른 지역이면 내용이 같아도 서로의 dedup 이력에 영향을 주지 않는다
"""

from datetime import date

import pytest

from main.aws_lambda.common import eia_fuel_price_layout as layout
from main.aws_lambda.functions.eia_gas_price_raw_to_bronze.extractor import (
    FILE_URL_DICT,
    file_url,
)
from main.aws_lambda.functions.eia_gas_price_raw_to_bronze.loader import (
    EiaGasPriceBronzeLoader,
)

COLLECTED = date(2026, 8, 17)
LATER = date(2026, 8, 24)


def test_지역별로_bronze_경로에_service_area_세그먼트가_들어간다(tmp_path):
    loader = EiaGasPriceBronzeLoader(str(tmp_path), COLLECTED, "NYC")

    result = loader.write({"body": b"nyc-body"})

    assert result.location == str(layout.gas_bronze_file(str(tmp_path), COLLECTED, "NYC"))
    assert "service_area=NYC" in result.location


def test_등록되지_않은_지역은_즉시_실패한다():
    with pytest.raises(ValueError, match="등록되지 않은 지역"):
        file_url("TX_INVALID")


@pytest.mark.parametrize("service_area", sorted(FILE_URL_DICT))
def test_등록된_지역은_URL을_돌려준다(service_area):
    assert file_url(service_area) == FILE_URL_DICT[service_area]


def test_지역마다_실제로_다른_URL을_쓴다():
    assert file_url("NYC") != file_url("TX")
    assert "SNY" in file_url("NYC")
    assert "STX" in file_url("TX")


def test_같은_지역에서_재수집해도_내용이_같으면_새_파티션을_안_만든다(tmp_path):
    first = EiaGasPriceBronzeLoader(str(tmp_path), COLLECTED, "NYC").write(
        {"body": b"same-body"}
    )
    (layout.gas_bronze_file(str(tmp_path), COLLECTED, "NYC").parent / "_SUCCESS").touch()

    second = EiaGasPriceBronzeLoader(str(tmp_path), LATER, "NYC").write(
        {"body": b"same-body"}
    )

    assert second.location == first.location
    assert not layout.gas_bronze_file(str(tmp_path), LATER, "NYC").exists()


def test_다른_지역이면_내용이_같아도_dedup되지_않는다(tmp_path):
    """service_area 를 안 넘기면 dedup 이 지역 구분 없이 동작해, 다른 지역의 첫
    수집이 조용히 다른 지역 파티션과 같은 것으로 취급돼 스킵될 수 있습니다."""
    EiaGasPriceBronzeLoader(str(tmp_path), COLLECTED, "NYC").write({"body": b"same-body"})

    tx_result = EiaGasPriceBronzeLoader(str(tmp_path), COLLECTED, "TX").write(
        {"body": b"same-body"}
    )

    assert "service_area=TX" in tx_result.location
    assert layout.gas_bronze_file(str(tmp_path), COLLECTED, "TX").is_file()
