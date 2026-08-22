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
[기사별 차량 교체시 예상 수익]
input: schema/silver.py - CLEAN_LEASE_VEHICLE_INVENTORY_SCHEMA, CLEAN_FUEL_PRICE_SCHEMA / schema/gold.py - DriverMonthlyProfit
output: schema/gold.py - MonthlyVehicleRecommendation
"""
@dataclass(frozen=True)
class MonthlyVehicleRecommendation:
    version: int
    """ 골드 데이터 버전 """
    
    driver_id: str
    """기사 ID"""

    year_month: str
    """집계 대상 월 (YYYY-MM)"""

    comfort_eligible: bool
    """Comfort 등급 대상 여부"""

    extra_comfort_eligible: bool
    """Extra Comfort 등급 대상 여부"""

    vehicle_model_id: str
    """추천 차량 모델 ID"""

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
[월간 리포트]
input: schema/gold.py - MonthlyVehicleRecommendation
output: schema/gold.py - MonthlyReport
"""
@dataclass(frozen=True)
class MonthlyReport:
    version: int
    """ 골드 데이터 버전 """
    
    year_month: str
    """집계 대상 월 (YYYY-MM). PK"""

    threshold_profit_increase: float
    """차량 교체 추천 기준선 (USD)"""

    is_rerun: bool
    """이 실행이 최초 완료가 아니라, 이미 완료된 대상월이 다시 계산된 재트리거인지"""

    recommended_driver_count: int
    """추천 대상 기사 수 (expected_net_profit_increase >= threshold_profit_increase, expected_revenue_increase >= 0)"""

    avg_net_profit_increase_per_driver: float
    """추천된 기사들의 평균 순수익 증가액 (USD)"""

    avg_revenue_increase_per_driver: float
    """추천된 기사들의 평균 매출 증가액 (USD)"""

    total_revenue_increase: float
    """추천된 기사들의 매출 증가액 합계 (USD)"""
