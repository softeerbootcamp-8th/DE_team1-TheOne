"""[뉴욕주 휘발유 요금] Silver 스키마."""

import pyarrow as pa

SCHEMA = pa.schema(
    [
        ("date", pa.date32()),
        ("gas_price", pa.float64()),
    ]
)
