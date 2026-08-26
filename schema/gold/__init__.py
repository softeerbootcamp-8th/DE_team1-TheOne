from dataclasses import dataclass
from datetime import datetime


"""
[Gold 적재 버전 메타데이터]
input: Gold 적재 실행의 지역·월·입력 fingerprint
output: PostgreSQL gold_load_versions
"""


@dataclass(frozen=True)
class GoldLoadVersion:
    service_area: str
    """서비스 지역 코드"""

    year_month: str
    """집계 대상 월 (YYYY-MM)"""

    version: int
    """같은 지역·월 안에서 증가하는 Gold 적재 버전"""

    load_fingerprint: str
    """동일한 Silver 입력과 추천 설정을 식별하는 SHA-256 fingerprint"""

    created_at: datetime
    """PostgreSQL이 CURRENT_TIMESTAMP로 기록하는 생성 시각"""


"""
[기사별 운행 순수익]
input: schema/silver.py - CLEAN_MONTHLY_TAXI_TRIP_SCHEMA, CLEAN_DRIVER_VEHICLE_MONTHLY_SNAPSHOT_SCHEMA, CLEAN_LEASE_VEHICLE_INVENTORY_SCHEMA, CLEAN_FUEL_PRICE_SCHEMA
output: schema/gold.py - DriverMonthlyProfit
"""
@dataclass(frozen=True)
class DriverMonthlyProfit:
    version: int
    """ 골드 데이터 버전 """

    driver_id: str
    """ 기사 ID """

    year_month: str
    """집계 대상 월 (YYYY-MM) """

    service_area: str
    """서비스 지역 코드 (예: NYC). AWS 리전이 아니라 운행 데이터의 지역 축입니다.

    driver_id 가 지역 간 유니크하지 않으므로(#805) 이 컬럼이 자연 키의 일부입니다 —
    빠지면 두 지역의 같은 기사 ID 가 한 행으로 취급됩니다.
    """

    comfort_eligible: bool
    """Comfort 등급 대상 여부"""

    extra_comfort_eligible: bool
    """Extra Comfort 등급 대상 여부"""

    taxi_id: str
    """택시 ID"""

    vehicle_model_id: str
    """차량 모델 ID"""

    manufacturer: str
    """현재 차량 제조사"""

    model_name: str
    """현재 차량 모델명"""

    model_year: int
    """현재 차량 연식"""

    fuel_efficiency: float
    """차량 연비"""

    monthly_mileage: float
    """월간 주행거리 (mile)"""

    monthly_driver_pay: float
    """월간 플랫폼 정산액 (USD)"""

    monthly_tips: float
    """월간 팁 (USD)"""

    monthly_fuel_cost: float
    """월간 연료비 (USD) = gas_price|ev_price * monthly_mileage / fuel_efficiency"""

    monthly_lease_fee: float
    """월간 리스료 (USD) = 주간 계약 리스료 / 7 * 대상 월 일수"""

    monthly_net_profit: float
    """월간 순수익 (USD) = monthly_driver_pay + monthly_tips - monthly_fuel_cost - monthly_lease_fee"""


"""
[재고를 반영한 기사별 차량 추천]
input: schema/silver.py - CLEAN_LEASE_VEHICLE_INVENTORY_SCHEMA, CLEAN_FUEL_PRICE_SCHEMA / schema/gold.py - DriverMonthlyProfit
output: schema/gold.py - DriverCarSuggestion
"""
@dataclass(frozen=True)
class DriverCarSuggestion:
    version: int
    """ 골드 데이터 버전 """
    
    driver_id: str
    """기사 ID"""

    year_month: str
    """집계 대상 월 (YYYY-MM)"""

    service_area: str
    """서비스 지역 코드 (예: NYC). AWS 리전이 아니라 운행 데이터의 지역 축입니다.

    driver_id 가 지역 간 유니크하지 않으므로(#805) 이 컬럼이 자연 키의 일부입니다 —
    빠지면 두 지역의 같은 기사 ID 가 한 행으로 취급됩니다.
    """

    recommendation_algorithm_version_id: int
    """추천 계산에 쓰인 알고리즘 버전. 알고리즘 로직이 바뀔 때만 사람이 올린다 (적재 시점마다
    바뀌는 `version`과 다른 축)"""

    threshold: int
    """회사 매출 증가를 1순위, 기사 순수익 증가가 이 값(USD) 이상인 것을 2순위로 배정하는
    알고리즘(v2)이 스윕한 임계값. threshold를 쓰지 않는 알고리즘(v1)은 `-1`로 고정한다 —
    실제 임계값은 항상 0 이상이라 `-1`은 "이 알고리즘엔 threshold 축이 없다"는 뜻으로 구분된다."""

    comfort_eligible: bool
    """Comfort 등급 대상 여부"""

    extra_comfort_eligible: bool
    """Extra Comfort 등급 대상 여부"""

    vehicle_model_id: str
    """배정한 차량 모델 ID"""

    manufacturer: str
    """추천 차량 제조사"""

    model_name: str
    """추천 차량 모델명"""

    model_year: int
    """추천 차량 연식 (가장 최근 연식)"""

    recommendation_reason: str
    """추천 이유. 현재 차량 대비 개선된 항목을 ", " 로 나열한 문자열. 예: "연비, Comfort 등급" """

    fuel_efficiency: float
    """추천 차량 연비."""

    recommended_monthly_lease_fee: float
    """추천 차량 월간 리스료 (USD)."""

    expected_monthly_fuel_cost: float
    """추천 차량 기준 예상 월간 연료비 (USD). monthly_mileage 는 현재와 동일하다고 가정"""

    expected_monthly_net_profit: float
    """예상 월간 순수익 (USD) = monthly_driver_pay + monthly_tips - expected_monthly_fuel_cost - recommended_monthly_lease_fee"""

    expected_net_profit_increase: float
    """예상 순수익 증가액 (USD) = expected_monthly_net_profit - DriverMonthlyProfit.monthly_net_profit"""

    expected_revenue_increase: float
    """예상 매출 증가액 (USD) = recommended_monthly_lease_fee - DriverMonthlyProfit.monthly_lease_fee. 회사가 추가로 받는 리스료 매출 증가분"""


"""
[Gold 실행이 읽은 Silver 4종의 계보]
input: (경로 문자열 — 계산 없음)
output: schema/gold.py - SilverLineage
"""
@dataclass(frozen=True)
class SilverLineage:
    version: int
    """ 골드 데이터 버전 """

    service_area: str
    """서비스 지역 코드 (예: NYC)"""

    year_month: str
    """집계 대상 월 (YYYY-MM)"""

    airflow_run_id: str
    """이 Gold 버전을 만든 Airflow DAG 실행 식별자"""

    code_sha: str
    """Gold Spark 이미지에 포함된 코드의 Git commit SHA"""

    config_hash: str
    """Silver 입력 경로와 추천 알고리즘·threshold 설정의 안정적 SHA-256"""

    silver_monthly_taxi_trip_s3_link: str
    """이 실행이 읽은 월별 택시 운행 기록 Silver 파티션의 S3 경로"""

    silver_driver_vehicle_monthly_snapshot_s3_link: str
    """이 실행이 읽은 기사 차량 월 스냅샷 Silver 파티션의 S3 경로"""

    silver_lease_vehicle_inventory_s3_link: str
    """이 실행이 읽은 리스 업체 보유 차량 Silver 파티션의 S3 경로"""

    silver_gas_ev_price_s3_link: str
    """이 실행이 읽은 연료비 Silver 파티션의 S3 경로"""


"""
[추천 알고리즘 버전 설명]
input: (사람이 직접 관리 — Gold 파이프라인이 적재하지 않음)
output: schema/gold.py - RecommendationAlgorithm
"""
@dataclass(frozen=True)
class RecommendationAlgorithm:
    recommendation_algorithm_version_id: int
    """추천 알고리즘 버전. DriverCarSuggestion.recommendation_algorithm_version_id 와 조인"""

    description: str
    """이 버전의 추천 로직 설명"""
