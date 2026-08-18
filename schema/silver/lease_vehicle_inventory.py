"""[리스 업체 보유 차량] 월별 API 스키마.

한 행은 제조사·모델·연식이 같은 보유 차량 묶음입니다. ``stock``은 그 묶음의
실제 차량 대수이고, ``fuel_efficiency``는 전기차도 MPGe로 통일합니다.
"""

import pyarrow as pa

SCHEMA = pa.schema(
    [
        ("vehicle_model_id", pa.string()),
        ("manufacturer", pa.string()),
        ("model_name", pa.string()),
        ("model_year", pa.int16()),
        ("fuel_type", pa.string()),
        ("fuel_efficiency", pa.float64()),
        ("comfort_eligible", pa.bool_()),
        ("extra_comfort_eligible", pa.bool_()),
        ("weekly_price_usd", pa.float64()),
        ("image_url", pa.string()),
        ("stock", pa.int32()),
    ]
)

REQUIRED_NON_NULL = frozenset(SCHEMA.names)
