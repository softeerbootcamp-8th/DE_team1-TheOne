"""[뉴욕주 충전소 요금] Silver 스키마."""

import pyarrow as pa

SCHEMA = pa.schema(
    [
        ("date", pa.date32()),
        ("ev_price", pa.float64()),
    ]
)
