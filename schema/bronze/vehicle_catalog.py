"""[Fast Track Leasing 렌탈 차량 카탈로그] Bronze 스키마.

vendor 는 파티션 키(vendor=)로만 남깁니다. 파일 안에 같은 이름의 컬럼을 또 두면
읽을 때 파티션 값(dictionary)과 타입이 충돌합니다.
"""

import pyarrow as pa

SCHEMA = pa.schema(
    [
        ("make", pa.string()),
        ("model", pa.string()),
        ("raw_name", pa.string()),  # 사이트 표기 원문
        ("price_usd", pa.float64()),  # 이미지 안에만 있어 현재는 항상 null
        ("price_period", pa.string()),
        ("image_url", pa.string()),  # 가격이 바뀌면 이 URL 이 바뀜
        ("booking_url", pa.string()),
        ("source_url", pa.string()),
        ("source_html_path", pa.string()),
        ("source_image_path", pa.string()),
        ("collected_at", pa.timestamp("us", tz="UTC")),
    ]
)
