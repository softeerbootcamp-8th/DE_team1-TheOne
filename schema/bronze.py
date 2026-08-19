import pyarrow as pa

"""[월별 택시 운행 기록]"""
MONTHLY_TAXI_TRIP_SCHEMA = pa.schema(
    [
        ("taxi_id", pa.string()),  # 운행한 택시 ID
        ("hvfhs_license_num", pa.string()),  # Uber or Lyft 구분
        ("on_scene_datetime", pa.timestamp("us")),  # 장소 도착 시각
        ("pickup_datetime", pa.timestamp("us")),  # 승차 시각
        ("dropoff_datetime", pa.timestamp("us")),  # 하차 시각
        ("pickup_zone", pa.string()),  # 승차 지역
        ("dropoff_zone", pa.string()),  # 하차 지역
        ("trip_miles", pa.float64()),  # 운행 거리
        ("trip_time", pa.int64()),  # 운행 시간
        ("driver_pay", pa.float64()),  # 기사 수익
        ("tips", pa.float64()),  # 팁
    ]
)

"""[월 기사 차량 스냅샷]"""
DRIVER_VEHICLE_MONTHLY_SNAPSHOT_SCHEMA = pa.schema(
    [
        ("snapshot_month", pa.string()),  # 스냅샷 대상 월 (YYYY-MM)
        ("driver_id", pa.string()),  # 기사 ID
        ("taxi_id", pa.string()),  # 택시 ID
        ("vehicle_model_id", pa.string()),  # 차량 모델 ID
        ("manufacturer", pa.string()),  # 제조사
        ("model_name", pa.string()),  # 모델명
        ("fuel_type", pa.string()),  # 유종
        ("comfort_eligible", pa.bool_()),  # Comfort 등급 대상 여부
        ("weekly_lease_fee", pa.float64()),  # 주간 리스료
        ("join_date", pa.date32()),  # 기사 입사일
        ("experience_years", pa.int32()),  # 운전 경력 (년)
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