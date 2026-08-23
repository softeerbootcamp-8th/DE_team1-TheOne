"""EIA 전력요금 원본 Raw→Bronze 적재의 지역(service_area) 처리 시나리오. 이슈 #844.

1. service_area별로 Bronze 경로가 service_area=<sa>/collected_date=.../ 로 나간다
2. 같은 지역에서 재수집해도 내용이 같으면 새 파티션을 만들지 않는다 (dedup)
3. 다른 지역이면 내용이 같아도 서로의 dedup 이력에 영향을 주지 않는다

FILE_URL_DICT(gas)와 달리 여기는 지역별 소스 URL이 없습니다 — EIA-861M이 전
지역 공통 단일 파일이라, extractor는 손대지 않고 저장 경로만 지역별로 나눕니다.
지역별로 실제 다른 값을 읽는 부분(SERVICE_AREA_TO_STATE)은
test_eia_electricity_price_pipeline.py 에서 검증합니다.
"""

from datetime import date

from main.aws_lambda.common import eia_fuel_price_layout as layout
from main.aws_lambda.functions.eia_electricity_price_raw_to_bronze.loader import (
    EiaElectricityPriceBronzeLoader,
)

COLLECTED = date(2026, 8, 17)
LATER = date(2026, 8, 24)


def test_지역별로_bronze_경로에_service_area_세그먼트가_들어간다(tmp_path):
    loader = EiaElectricityPriceBronzeLoader(str(tmp_path), COLLECTED, "NYC")

    result = loader.write({"body": b"nyc-body"})

    assert result.location == str(
        layout.electricity_bronze_file(str(tmp_path), COLLECTED, "NYC")
    )
    assert "service_area=NYC" in result.location


def test_같은_지역에서_재수집해도_내용이_같으면_새_파티션을_안_만든다(tmp_path):
    first = EiaElectricityPriceBronzeLoader(str(tmp_path), COLLECTED, "NYC").write(
        {"body": b"same-body"}
    )

    second = EiaElectricityPriceBronzeLoader(str(tmp_path), LATER, "NYC").write(
        {"body": b"same-body"}
    )

    assert second.location == first.location
    assert not layout.electricity_bronze_file(str(tmp_path), LATER, "NYC").exists()


def test_다른_지역이면_내용이_같아도_dedup되지_않는다(tmp_path):
    EiaElectricityPriceBronzeLoader(str(tmp_path), COLLECTED, "NYC").write(
        {"body": b"same-body"}
    )

    tx_result = EiaElectricityPriceBronzeLoader(str(tmp_path), COLLECTED, "TX").write(
        {"body": b"same-body"}
    )

    assert "service_area=TX" in tx_result.location
    assert layout.electricity_bronze_file(str(tmp_path), COLLECTED, "TX").is_file()
