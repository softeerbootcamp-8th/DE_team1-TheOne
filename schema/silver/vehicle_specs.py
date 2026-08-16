"""[fueleconomy.gov 차종별 제원] Silver 스키마.

원본 84컬럼 중 소비자가 실제로 쓰는 것만 남깁니다. 나머지는 Bronze 에
그대로 있고 (source_id, bronze_path) 로 되짚을 수 있습니다.
source / collected_date 는 파티션 키라 컬럼으로 두지 않습니다.
"""

import pyarrow as pa

SCHEMA = pa.schema(
    [
        ("source_id", pa.string()),  # 원본 레코드 식별자 (계보)
        ("year", pa.int16()),
        ("make_key", pa.string()),  # 조인 키 (대문자 정규화)
        ("model_key", pa.string()),  # 조인 키 (대문자 정규화)
        ("base_model_key", pa.string()),  # 구동방식 접미사가 빠진 대안 조인 키
        ("combined_mpg", pa.float64()),  # 전기차는 MPGe
        ("combined_kwh_per_100mi", pa.float64()),
        ("range_miles", pa.float64()),
        ("atv_type", pa.string()),  # EV / Plug-in Hybrid / ...
        ("bronze_path", pa.string()),  # 계보
    ]
)
