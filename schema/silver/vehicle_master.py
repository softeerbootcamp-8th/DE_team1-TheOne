"""[차량 마스터] Silver 스키마.

city / collected_date 는 파티션 키라 컬럼으로 두지 않습니다.
원천의 표기 원문과 나머지 제원 컬럼은 각 원천 Silver 에 있고 `*_bronze_path`
로 되짚을 수 있습니다.
"""

import pyarrow as pa

SCHEMA = pa.schema(
    [
        ("vendor", pa.string()),  # 대장을 낸 리스 업체
        ("make_key", pa.string()),  # 조인 키 (대문자 정규화)
        ("model_key", pa.string()),  # 조인 키 (대문자 정규화)
        ("platform", pa.string()),  # uber / lyft, 자격 없으면 NULL
        ("product", pa.string()),  # UberX / Comfort / Extra Comfort ...
        ("min_year", pa.int16()),  # 이 상품에 필요한 최소 차량 연식
        ("weekly_price_usd", pa.float64()),  # 리스 업체 주간 렌트료
        ("image_url", pa.string()),  # 리스 업체 차량 이미지
        ("spec_match_level", pa.string()),  # MODEL / DRIVETRAIN / NONE
        # 제원은 대표 1건이 아니라 후보 트림 전체의 범위입니다. 대장에 트림 정보가
        # 없어 어느 값이 맞는지 모르기 때문입니다 — 고르는 것은 Gold 가 합니다.
        ("spec_trim_count", pa.int32()),
        ("spec_year_min", pa.int16()),
        ("spec_year_max", pa.int16()),
        ("combined_mpg_min", pa.float64()),  # 전기차는 MPGe
        ("combined_mpg_max", pa.float64()),
        ("combined_kwh_per_100mi_min", pa.float64()),
        ("combined_kwh_per_100mi_max", pa.float64()),
        ("range_miles_min", pa.float64()),
        ("fuel_type", pa.string()),  # EV / PHEV / HYBRID / GAS / MIXED
        ("catalog_bronze_path", pa.string()),  # 계보
        ("specs_bronze_path", pa.string()),
        ("eligibility_bronze_path", pa.string()),
    ]
)
