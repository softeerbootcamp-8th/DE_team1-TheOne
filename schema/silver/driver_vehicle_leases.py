"""기사 데이터 Silver의 논리 스키마."""

import pyarrow as pa


SCHEMA = pa.schema(
    [
        ("lease_id", pa.string()),
        ("customer_id", pa.string()),
        ("driver_id", pa.string()),
        ("taxi_id", pa.string()),
        ("make_key", pa.string()),
        ("model_key", pa.string()),
        ("model_year", pa.int64()),
        ("lease_started_on", pa.date32()),
        ("lease_ended_on", pa.date32()),
    ]
)

REQUIRED_NON_NULL = {
    "lease_id",
    "customer_id",
    "driver_id",
    "taxi_id",
    "make_key",
    "model_key",
    "model_year",
    "lease_started_on",
}
