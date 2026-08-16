"""[Uber/Lyft 배차 가능 차량] Silver 스키마.

Uber Eligible Silver와 Lyft Eligible Silver가 공유합니다 — 둘 다 차량 대장에서
함께 조인하는 데 씁니다. 소비자가 실제로 쓰는 것만 남깁니다. 표기 원문
(make/model/raw_eligibility)은 Bronze 에 있고 bronze_path 로 되짚을 수 있습니다.
city / collected_date 는 파티션 키라 컬럼으로 두지 않습니다.
"""

import pyarrow as pa

SCHEMA = pa.schema(
    [
        ("make_key", pa.string()),  # 조인 키 (대문자 정규화)
        ("model_key", pa.string()),  # 조인 키 (대문자 정규화)
        ("product", pa.string()),  # UberX / Comfort / XL ...
        ("min_year", pa.int16()),  # 이 상품에 필요한 최소 차량 연식
        ("bronze_path", pa.string()),  # 계보
    ]
)
