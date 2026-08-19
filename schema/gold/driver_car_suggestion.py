"""[월간 추천 차량] 스키마.

기사 1명 × 1개월 단위로, 현재 차량 대비 교체를 추천하는 차량과 예상 손익을 담은 Gold 테이블.
driver_aggregation.DriverMonthlyAggregation 의 (driver_id, year_month) 와 1:1 로 대응.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class MonthlyVehicleRecommendation:
    driver_id: str
    """기사 ID. driver_master.driver_id 참조."""

    year_month: str
    """집계 대상 월 (YYYY-MM). driver_id 와 함께 PK."""

    service_tier: str
    """Uber/Lyft 서비스 등급 (예: Standard/Comfort/Extra Comfort/Premium/Black).

    hvfhv silver 의 estimated_service_tier 와 동일 도메인.
    """

    recommended_make_key: str
    """추천 차량 제조사. vehicle_master.make_key 참조."""

    recommended_model_key: str
    """추천 차량 모델. vehicle_master.model_key 참조."""

    recommended_model_year: int
    """추천 차량 연식. vehicle_master 는 taxi_id 가 없는 차종(스펙) 테이블이라
    실제 보유 차량이 아니라 (make_key, model_key, 연식) 3개로 추천 차량을 식별함.
    스펙 트림 범위(spec_year_min~spec_year_max) 중 가장 최신 연식(spec_year_max)."""

    recommendation_reason: str
    """추천 이유. 현재 차량 대비 개선된 항목을 ", " 로 나열한 문자열 —
    "연비"(combined_mpg 가 더 높음) / "차량등급"(vehicle_group 이 더 넓음, 예: SINGLE→BOTH) /
    "더 저렴한 렌트료"(weekly_lease_fee 가 더 낮음) 중 해당하는 것. 셋 다 아니면(추천 차량이
    현재 차량과 동일하거나 세 항목 모두 동률) "현재 차량 유지"."""

    combined_mpg: float
    """추천 차량 연비."""

    recommended_monthly_rental_fee: float
    """추천 차량 월간 렌탈료 (USD)."""

    expected_monthly_fuel_cost: float
    """추천 차량 기준 예상 월간 연료비 (USD). 현재 운행 패턴(주행거리 등)은 동일하다고 가정."""

    expected_monthly_net_profit: float
    """추천 차량 기준 예상 월간 순수익 (USD) = 예상 매출(driver_pay+Tip) - expected_monthly_fuel_cost
    - recommended_monthly_rental_fee. driver_aggregation.monthly_net_profit 과 동일하게 렌탈료 차감."""

    expected_net_profit_increase: float
    """예상 순수익 증가액 (USD) = expected_monthly_net_profit - 현재 monthly_net_profit."""

    expected_revenue_increase: float
    """예상 매출 증가액 (USD) = recommended_monthly_rental_fee - 현재 monthly_rental_fee.

    기사의 운행 요금 매출이 아니라, 차량을 교체했을 때 회사가 추가로 받는 렌탈료 매출 증가분.
    """
