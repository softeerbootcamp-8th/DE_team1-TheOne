"""sub/ 프로젝트(가짜 데이터 실험)에서만 참조하는 스키마.

schema/bronze, schema/silver, schema/gold 는 main/ 이 쓰는 것만 남깁니다. sub/ 가
단독으로 쓰는 스키마는 여기 모읍니다.
"""

import pyarrow as pa

"""[Lyft Premium 자격 차량] Bronze 스키마."""
LYFT_ELIGIBLE_VEHICLES_SCHEMA = pa.schema(
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

"""[Uber Eligible Vehicles] Bronze 스키마."""
UBER_ELIGIBLE_VEHICLES_SCHEMA = pa.schema(
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

"""[Fast Track Leasing 렌탈 차량 카탈로그] Bronze 스키마.

vendor 는 파티션 키(vendor=)로만 남깁니다. 파일 안에 같은 이름의 컬럼을 또 두면
읽을 때 파티션 값(dictionary)과 타입이 충돌합니다.
"""
VEHICLE_CATALOG_BRONZE_SCHEMA = pa.schema(
    [
        ("make", pa.string()),
        ("model", pa.string()),
        ("raw_name", pa.string()),  # 사이트 표기 원문
        ("price_usd", pa.float64()),  # 이미지 안에만 있어 현재는 항상 null
        ("price_period", pa.string()),
        ("image_url", pa.string()),  # 가격이 바뀌면 이 URL 이 바뀜
        ("booking_url", pa.string()),
        ("source_url", pa.string()),
        ("source_html_path", pa.string()),
        ("source_image_path", pa.string()),
        ("collected_at", pa.timestamp("us", tz="UTC")),
    ]
)

"""[Uber/Lyft 배차 가능 차량] Silver 스키마.

Uber Eligible Silver와 Lyft Eligible Silver가 공유합니다 — 둘 다 차량 대장에서
함께 조인하는 데 씁니다. 소비자가 실제로 쓰는 것만 남깁니다. 표기 원문
(make/model/raw_eligibility)은 Bronze 에 있고 bronze_path 로 되짚을 수 있습니다.
city / collected_date 는 파티션 키라 컬럼으로 두지 않습니다.
"""
ELIGIBLE_VEHICLES_SCHEMA = pa.schema(
    [
        ("make_key", pa.string()),  # 조인 키 (대문자 정규화)
        ("model_key", pa.string()),  # 조인 키 (대문자 정규화)
        ("product", pa.string()),  # UberX / Comfort / XL ...
        ("min_year", pa.int16()),  # 이 상품에 필요한 최소 차량 연식
        ("bronze_path", pa.string()),  # 계보
    ]
)

"""[Fast Track Leasing 렌탈 차량 카탈로그] Silver 스키마.

소비자가 실제로 쓰는 것만 남깁니다. 표기 원문(make/model/raw_name), 링크,
상수(currency/price_unit)는 전부 Bronze 에 있고 bronze_path 로 되짚을 수 있습니다.
vendor / collected_date 는 파티션 키라 컬럼으로 두지 않습니다.
"""
VEHICLE_CATALOG_SCHEMA = pa.schema(
    [
        ("make_key", pa.string()),  # 조인 키 (대문자 정규화)
        ("model_key", pa.string()),  # 조인 키 (대문자 정규화)
        ("weekly_lease_fee", pa.float64()),
        ("image_url", pa.string()),  # 보유 차량 API 표시용 원천 이미지
        ("bronze_path", pa.string()),  # 계보
    ]
)

"""[차량 마스터] Silver 스키마.

city / collected_date 는 파티션 키라 컬럼으로 두지 않습니다.
원천의 표기 원문과 나머지 제원 컬럼은 각 원천 Silver 에 있고 `*_bronze_path`
로 되짚을 수 있습니다.
"""
VEHICLE_MASTER_SCHEMA = pa.schema(
    [
        ("vendor", pa.string()),  # 대장을 낸 리스 업체
        ("make_key", pa.string()),  # 조인 키 (대문자 정규화)
        ("model_key", pa.string()),  # 조인 키 (대문자 정규화)
        ("platform", pa.string()),  # uber / lyft, 자격 없으면 NULL
        ("product", pa.string()),  # UberX / Comfort / Extra Comfort ...
        ("min_year", pa.int16()),  # 이 상품에 필요한 최소 차량 연식
        ("weekly_lease_fee", pa.float64()),  # 리스 업체 주간 렌트료
        ("image_url", pa.string()),  # 리스 업체 차량 이미지
        ("spec_match_level", pa.string()),  # MODEL / DRIVETRAIN / NONE
        # 제원은 대표 1건이 아니라 후보 트림 전체의 범위입니다. 대장에 트림 정보가
        # 없어 어느 값이 맞는지 모르기 때문입니다 — 고르는 것은 Gold 가 합니다.
        ("spec_trim_count", pa.int32()),
        ("spec_year_min", pa.int16()),
        ("spec_year_max", pa.int16()),
        ("combined_mpg_min", pa.float64()),  # 전기차는 MPGe
        ("combined_mpg_max", pa.float64()),
        ("combined_kwh_per_100mi_min", pa.float64()),
        ("combined_kwh_per_100mi_max", pa.float64()),
        ("range_miles_min", pa.float64()),
        ("fuel_type", pa.string()),  # EV / PHEV / HYBRID / GAS / MIXED
        ("catalog_bronze_path", pa.string()),  # 계보
        ("specs_bronze_path", pa.string()),
        ("eligibility_bronze_path", pa.string()),
    ]
)

# 자격·제원 매칭 결과와 무관하게 **항상** 값이 있어야 하는 컬럼.
# `platform`·`product`·`min_year` 는 자격이 없으면 NULL 이고, `spec_*`·`combined_*` 는
# 제원 매칭이 NONE 이면 NULL 이라 여기 넣지 않습니다.
#
# 상류 컬럼명이 바뀌면 `.get()` 이 조용히 None 을 돌려줘 그 컬럼이 통째로 빕니다.
# 실제로 `weekly_price_usd` -> `weekly_lease_fee` 통일 때 142행 전부 NULL 인 마스터가
# 검증을 통과했습니다 (#567). 그 값은 Gold 의 렌탈 객단가로 이어집니다.
VEHICLE_MASTER_REQUIRED_NON_NULL = frozenset(
    {
        "vendor",
        "make_key",
        "model_key",
        "weekly_lease_fee",
        "spec_match_level",
        "spec_trim_count",
        "catalog_bronze_path",
    }
)

"""[fueleconomy.gov 차종별 제원] Silver 스키마.

원본 84컬럼 중 소비자가 실제로 쓰는 것만 남깁니다. 나머지는 Bronze 에
그대로 있고 (source_id, bronze_path) 로 되짚을 수 있습니다.
source / collected_date 는 파티션 키라 컬럼으로 두지 않습니다.
"""
VEHICLE_SPECS_SCHEMA = pa.schema(
    [
        ("source_id", pa.string()),  # 원본 레코드 식별자 (계보)
        ("year", pa.int16()),
        ("make_key", pa.string()),  # 조인 키 (대문자 정규화)
        ("model_key", pa.string()),  # 조인 키 (대문자 정규화)
        ("base_model_key", pa.string()),  # 구동방식 접미사가 빠진 대안 조인 키
        ("combined_mpg", pa.float64()),  # 전기차는 MPGe
        ("combined_kwh_per_100mi", pa.float64()),
        ("range_miles", pa.float64()),
        ("atv_type", pa.string()),  # EV / Plug-in Hybrid / ...
        ("bronze_path", pa.string()),  # 계보
    ]
)

"""[월별 택시 운행 기록] 월별 API 스키마.

`schema/bronze` 의 같은 이름 스키마와 **완전히 같아야** 합니다. sub/ 는 schema/bronze
를 참조하지 않으므로 여기에 복제해 두고, 두 정의가 갈라지지 않는지는
`main/airflow/tests/test_schema_source_mirror.py` 가 확인합니다.
"""
MONTHLY_TAXI_TRIP_SCHEMA = pa.schema(
    [
        ("taxi_id", pa.string()),  # 운행한 택시 ID
        ("hvfhs_license_num", pa.string()),  # platform_name 역매핑 (Uber→HV0003, Lyft→HV0005)
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

"""[월 기사 차량 스냅샷] 월별 API 스키마.

한 행은 (기사, 대상 월) 하나입니다 — 리스 계약 단위가 아닙니다.
`exit_date` 만 nullable 이고(재직 중이면 NULL), 나머지는 값이 있어야 합니다.
"""
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

DRIVER_VEHICLE_MONTHLY_SNAPSHOT_REQUIRED_NON_NULL = frozenset(
    set(DRIVER_VEHICLE_MONTHLY_SNAPSHOT_SCHEMA.names) - {"exit_date"}
)

"""[리스 업체 보유 차량] 월별 API 스키마.

한 행은 제조사·모델·연식이 같은 보유 차량 묶음입니다. ``stock``은 그 묶음의
실제 차량 대수이고, ``fuel_efficiency``는 전기차도 MPGe로 통일합니다.
"""
LEASE_VEHICLE_INVENTORY_SCHEMA = pa.schema(
    [
        ("vehicle_model_id", pa.string()),
        ("manufacturer", pa.string()),
        ("model_name", pa.string()),
        ("model_year", pa.int16()),
        ("fuel_type", pa.string()),
        ("fuel_efficiency", pa.float64()),
        ("comfort_eligible", pa.bool_()),
        ("extra_comfort_eligible", pa.bool_()),
        ("weekly_lease_fee", pa.float64()),
        ("image_url", pa.string()),
        ("stock", pa.int32()),
    ]
)

LEASE_VEHICLE_INVENTORY_REQUIRED_NON_NULL = frozenset(LEASE_VEHICLE_INVENTORY_SCHEMA.names)

# 회사 원천 스냅샷의 저장 스키마.
#
# pandas 가 추론하게 두면 안 됩니다. 초기 스냅샷은 모든 계약이 진행 중이라
# `lease_ended_on` 이 전량 결측이고, 그러면 Parquet 타입이 `null` 로 굳어
# Spark 가 날짜로 못 읽습니다 — 기사 배정이 분석 단계에서 죽습니다(#353).
#
# 더 나쁜 건 타입이 스냅샷마다 달라진다는 점입니다. `evolve_company_snapshot` 이
# 일부 계약을 종료시키면 그때는 값이 생겨 날짜로 추론됩니다. 소비하는 쪽은
# 어느 달 스냅샷을 읽느냐에 따라 되기도 하고 안 되기도 합니다.
COMPANY_SNAPSHOT_SCHEMAS = {
    "customer": pa.schema(
        [
            ("customer_id", pa.string()),
            ("synthetic_driver_id", pa.string()),
            ("snapshot_date", pa.date32()),
        ]
    ),
    "taxi": pa.schema(
        [
            ("taxi_id", pa.string()),
            ("make_key", pa.string()),
            ("model_key", pa.string()),
            ("model_year", pa.int64()),
            ("weekly_lease_fee", pa.float64()),
            ("uber_comfort_eligible", pa.bool_()),
            ("lyft_extra_comfort_eligible", pa.bool_()),
            ("vehicle_group", pa.string()),
            ("snapshot_date", pa.date32()),
        ]
    ),
    "lease_contract": pa.schema(
        [
            ("lease_id", pa.string()),
            ("customer_id", pa.string()),
            ("taxi_id", pa.string()),
            ("lease_started_on", pa.date32()),
            # 진행 중이면 결측입니다. 전량 결측이어도 날짜여야 합니다.
            ("lease_ended_on", pa.date32()),
            ("snapshot_date", pa.date32()),
        ]
    ),
}
