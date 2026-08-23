import pyarrow as pa

"""
[CLEAN 월별 택시 운행 기록]
input: schema/bronze.py - MONTHLY_TAXI_TRIP_SCHEMA
output: schema/silver.py - CLEAN_MONTHLY_TAXI_TRIP_SCHEMA
"""
CLEAN_MONTHLY_TAXI_TRIP_SCHEMA = pa.schema(
    [
        ("taxi_id", pa.string()),  # 운행한 택시 ID
        ("hvfhs_license_num", pa.string()),  # Uber or Lyft 구분 (HV0003: Uber, HV0005: Lyft)
        ("on_scene_datetime", pa.timestamp("us")),  # 장소 도착 시각
        ("pickup_datetime", pa.timestamp("us")),  # 승차 시각
        ("dropoff_datetime", pa.timestamp("us")),  # 하차 시각
        ("PULocationID", pa.int32()),  # 승차 지역 ID
        ("DOLocationID", pa.int32()),  # 하차 지역 ID
        ("pickup_zone", pa.string()),  # 승차 지역
        ("dropoff_zone", pa.string()),  # 하차 지역
        ("trip_miles", pa.float64()),  # 운행 거리
        ("trip_time", pa.int64()),  # 운행 시간
        ("driver_pay", pa.float64()),  # 기사 수익
        ("tips", pa.float64()),  # 팁
        ("estimated_service_tier", pa.string()),  # 원천에서 확정한 운행 등급
    ]
)

# `on_scene_datetime` 은 원천이 채우지 않는 달이 있습니다(플랫폼에 따라 비어 있고,
# 합성 원천도 `source_job` 에서 non-null 검사 대상이 아닙니다). 스키마에는 남기고
# 필수값 검사에서만 뺍니다 — 기사 스냅샷의 `exit_date` 와 같은 취급입니다.
CLEAN_MONTHLY_TAXI_TRIP_REQUIRED_NON_NULL = frozenset(
    set(CLEAN_MONTHLY_TAXI_TRIP_SCHEMA.names) - {"on_scene_datetime"}
)

"""
[CLEAN 월 기사 차량 스냅샷]
input: schema/bronze.py - DRIVER_VEHICLE_MONTHLY_SNAPSHOT_SCHEMA
output: schema/silver.py - CLEAN_DRIVER_VEHICLE_MONTHLY_SNAPSHOT_SCHEMA
"""
CLEAN_DRIVER_VEHICLE_MONTHLY_SNAPSHOT_SCHEMA = pa.schema(
    [
        ("snapshot_month", pa.string()),  # 스냅샷 대상 월 (YYYY-MM)
        ("driver_id", pa.string()),  # 기사 ID
        ("taxi_id", pa.string()),  # 택시 ID
        ("vehicle_model_id", pa.string()),  # 차량 모델 ID
        ("manufacturer", pa.string()),  # make_key
        ("model_name", pa.string()),  # model_key
        ("fuel_type", pa.string()),  # specs.atv_type 에서 유도
        ("comfort_eligible", pa.bool_()),  # uber_comfort_eligible
        ("extra_comfort_eligible", pa.bool_()),  # Lyft 자격 보존
        ("weekly_lease_fee", pa.float64()),  # weekly_rental_fee_usd
        ("join_date", pa.date32()),  # joined_on
        ("exit_date", pa.date32()),  # 기사 퇴사일 (재직 중이면 NULL)
        ("experience_years", pa.int32()),  # 운전 경력 (년)
        ("vehicle_since", pa.date32()),  # 현재 차량 운행 시작일
        ("snapshot_created_at", pa.timestamp("us")),  # 스냅샷 생성 시각
    ]
)

CLEAN_DRIVER_VEHICLE_MONTHLY_SNAPSHOT_SCHEMA_REQUIRED_NON_NULL = frozenset(
    set(CLEAN_DRIVER_VEHICLE_MONTHLY_SNAPSHOT_SCHEMA.names) - {"exit_date"}
)


"""
[CLEAN 리스 업체 보유 차량 데이터]
input: schema/bronze.py - LEASE_VEHICLE_INVENTORY_SCHEMA
output: schema/silver.py - CLEAN_LEASE_VEHICLE_INVENTORY_SCHEMA"""
CLEAN_LEASE_VEHICLE_INVENTORY_SCHEMA = pa.schema(
    [
        ("vehicle_model_id", pa.string()),  # 차량 모델 ID
        ("manufacturer", pa.string()),  # 제조사
        ("model_name", pa.string()),  # 모델명
        ("model_year", pa.int16()),  # 연식
        ("fuel_type", pa.string()),  # 유종
        ("fuel_efficiency", pa.float64()),  # 연비 (전기차는 MPGe)
        ("comfort_eligible", pa.bool_()),  # Comfort 등급 대상 여부
        ("extra_comfort_eligible", pa.bool_()),  # Extra Comfort 등급 대상 여부
        ("weekly_lease_fee", pa.float64()),  # 주간 리스료
        ("image_url", pa.string()),  # 차량 이미지 URL
        ("stock", pa.int32()),  # 보유 대수
    ]
)

CLEAN_LEASE_VEHICLE_INVENTORY_REQUIRED_NON_NULL = frozenset(
    CLEAN_LEASE_VEHICLE_INVENTORY_SCHEMA.names
)

"""
[뉴욕주 휘발유 요금]
input: schema/bronze.py - xlsx 원본 데이터
output: schema/silver.py - CLEAN_GAS_PRICE_SCHEMA
"""
CLEAN_GAS_PRICE_SCHEMA = pa.schema(
    [
        ("date", pa.date32()),
        ("gas_price", pa.float64()),
        ("bronze_collected_date", pa.date32()),
    ]
)

"""
[뉴욕주 충전소 요금]
input: schema/bronze.py - xlsx 원본 데이터
output: schema/silver.py - CLEAN_EV_CHARGING_PRICE_SCHEMA
"""
CLEAN_EV_CHARGING_PRICE_SCHEMA = pa.schema(
    [
        ("date", pa.date32()),
        ("ev_price", pa.float64()),
        ("bronze_collected_date", pa.date32()),
        ("ev_price_status", pa.string()),
    ]
)

"""
[뉴욕시 월별 연료비]
input: schema/silver.py - CLEAN_GAS_PRICE_SCHEMA, CLEAN_EV_CHARGING_PRICE_SCHEMA
output: schema/silver.py - CLEAN_FUEL_PRICE_SCHEMA"""
CLEAN_FUEL_PRICE_SCHEMA = pa.schema(
    [
        ("date", pa.date32()),
        ("gas_price", pa.float64()),
        ("ev_price", pa.float64()),
        ("price_source", pa.string()),  # 출처 (예: "eia")
        ("bronze_collected_date", pa.date32()),  # 수집 시점
        ("ev_price_status", pa.string()),  # 전력값 확정 여부 ("Preliminary" / "Final")
    ]
)

# 출처 이름. 역할("backfill")이 아니라 어디서 왔는지로 둡니다.
EIA = "eia"

# EIA 가 파일에 직접 적는 값입니다. 우리가 정하는 것이 아니라 읽어서 옮깁니다.
PRELIMINARY = "Preliminary"
FINAL = "Final"
