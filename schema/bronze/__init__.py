import pyarrow as pa

"""[월별 택시 운행 기록]"""
MONTHLY_TAXI_TRIP_SCHEMA = pa.schema(
    [
        ("taxi_id", pa.string()),  # 운행한 택시 ID
        ("hvfhs_license_num", pa.string()),  # platform_name 역매핑 (Uber→HV0003, Lyft→HV0005)
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

"""[월 기사 차량 스냅샷]"""
DRIVER_VEHICLE_MONTHLY_SNAPSHOT_SCHEMA = pa.schema(
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

"""[리스 업체 보유 차량 데이터]"""
LEASE_VEHICLE_INVENTORY_SCHEMA = pa.schema(
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

"""[뉴욕시 휘발유 요금]"""
# 원본 xlsx 그대로 저장

"""[뉴욕시 충전소 요금]"""
# 원본 xlsx 그대로 저장
