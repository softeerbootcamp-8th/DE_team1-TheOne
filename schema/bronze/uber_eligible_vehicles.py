"""[Uber Eligible Vehicles] Bronze 스키마."""

import pyarrow as pa

SCHEMA = pa.schema(
    [
        ("city_slug", pa.string()),
        ("make", pa.string()),
        ("model", pa.string()),
        ("min_year", pa.int16()),
        ("products", pa.list_(pa.string())),
        ("raw_eligibility", pa.string()),  # 모델 원문 (파싱 검증/재처리용)
        ("collected_at", pa.timestamp("us", tz="UTC")),
    ]
)
