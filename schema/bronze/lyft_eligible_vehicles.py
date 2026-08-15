"""[Lyft Premium 자격 차량] Bronze 스키마."""

import pyarrow as pa

SCHEMA = pa.schema(
    [
        ("city_slug", pa.string()),
        ("make", pa.string()),
        ("model", pa.string()),
        ("min_year", pa.int16()),
        ("products", pa.list_(pa.string())),
        ("raw_eligibility", pa.string()),
        ("raw_vehicle", pa.string()),
        ("source_url", pa.string()),
        ("collected_at", pa.timestamp("us", tz="UTC")),
    ]
)
