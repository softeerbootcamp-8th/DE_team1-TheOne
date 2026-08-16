"""[Fast Track Leasing 렌탈 차량 카탈로그] Silver 스키마.

소비자가 실제로 쓰는 것만 남깁니다. 표기 원문(make/model/raw_name), 링크,
상수(currency/price_unit)는 전부 Bronze 에 있고 bronze_path 로 되짚을 수 있습니다.
vendor / collected_date 는 파티션 키라 컬럼으로 두지 않습니다.
"""

import pyarrow as pa

SCHEMA = pa.schema(
    [
        ("make_key", pa.string()),  # 조인 키 (대문자 정규화)
        ("model_key", pa.string()),  # 조인 키 (대문자 정규화)
        ("weekly_price_usd", pa.float64()),
        ("bronze_path", pa.string()),  # 계보
    ]
)
