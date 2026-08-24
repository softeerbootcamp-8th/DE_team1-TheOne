from dataclasses import dataclass

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
