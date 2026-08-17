"""[HVFHV+taxi_id 데이터] Bronze 물리 스키마.

TLC 원본 Parquet 의 **물리 스키마**입니다. Bronze 는 받은 바이트를 파싱 없이 그대로
쓰므로(``hvfhv_raw_to_bronze/loader.py::write`` 참고) 여기 적힌 타입이 실제 파일과
다르면 검증이 영원히 통과하지 못합니다 — ``pa.string()`` / ``pa.int64()`` 로 두었다가
그렇게 됐습니다(#324).

월별 원본 footer 를 직접 확인한 값입니다.

    2024-06  필드 24  large_string  int32  cbd_congestion_fee 없음
    2025-01  필드 25  large_string  int32  cbd_congestion_fee 있음  <- 이때 추가
    2026-06  필드 25  large_string  int32  cbd_congestion_fee 있음
"""

import pyarrow as pa

TLC_SCHEMA = pa.schema(
    [
        ("hvfhs_license_num", pa.large_string()),
        ("dispatching_base_num", pa.large_string()),
        ("originating_base_num", pa.large_string()),
        ("request_datetime", pa.timestamp("us")),
        ("on_scene_datetime", pa.timestamp("us")),
        ("pickup_datetime", pa.timestamp("us")),
        ("dropoff_datetime", pa.timestamp("us")),
        ("PULocationID", pa.int32()),
        ("DOLocationID", pa.int32()),
        ("trip_miles", pa.float64()),
        ("trip_time", pa.int64()),
        ("base_passenger_fare", pa.float64()),
        ("tolls", pa.float64()),
        ("bcf", pa.float64()),
        ("sales_tax", pa.float64()),
        ("congestion_surcharge", pa.float64()),
        ("airport_fee", pa.float64()),
        ("tips", pa.float64()),
        ("driver_pay", pa.float64()),
        ("shared_request_flag", pa.large_string()),
        ("shared_match_flag", pa.large_string()),
        ("access_a_ride_flag", pa.large_string()),
        ("wav_request_flag", pa.large_string()),
        ("wav_match_flag", pa.large_string()),
        # CBD 혼잡통행료. 2025-01 부터 있습니다 — 그 이전 달에는 이 컬럼이 없어서
        # 스키마 변동으로 잡히지만, 원본 그대로가 맞으므로 적재는 그대로 진행합니다.
        ("cbd_congestion_fee", pa.float64()),
    ]
)

# 2024-12 이전 원본에는 cbd_congestion_fee 가 없습니다 — 그 시점 이전 데이터를 검증할 때
# 씁니다.
TLC_LEGACY_SCHEMA = pa.schema(
    [field for field in TLC_SCHEMA if field.name != "cbd_congestion_fee"]
)

# 제공 데이터는 각 월의 TLC 원본 컬럼을 보존하고 배정된 taxi_id 하나만 추가합니다.
SCHEMA = pa.schema([*TLC_SCHEMA, pa.field("taxi_id", pa.string())])
LEGACY_SCHEMA = pa.schema(
    [*TLC_LEGACY_SCHEMA, pa.field("taxi_id", pa.string())]
)
