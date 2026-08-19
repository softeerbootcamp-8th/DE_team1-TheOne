"""[기사 월단위 집계] 스키마.

기사 1명 × 1개월 단위로 운행 패턴과 수익성을 집계한 Gold 테이블.
"""

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class DriverMonthlyAggregation:
    driver_id: str
    """기사 ID. driver_master.driver_id 참조."""

    year_month: str
    """집계 대상 월 (YYYY-MM). driver_id 와 함께 PK."""

    # --- 시간대 별 운행 비중 (3시간 단위 8구간, 합계 1.0) ---
    ratio_00_03: float
    ratio_03_06: float
    ratio_06_09: float
    ratio_09_12: float
    ratio_12_15: float
    ratio_15_18: float
    ratio_18_21: float
    ratio_21_24: float

    # --- 운행 zone 비중 (승차 zone 기준 상위 3개) ---
    top1_zone_id: int
    """TLC taxi_zone_lookup.LocationID 참조."""
    top1_zone_ratio: float
    top2_zone_id: Optional[int]
    """3개월 미만 zone 에서만 운행했다면 None."""
    top2_zone_ratio: Optional[float]
    top3_zone_id: Optional[int]
    top3_zone_ratio: Optional[float]

    current_taxi_id: str
    """현재 차량 taxi_id. hvfhv_driver_trip.taxi_id 참조 (vehicle_master 에는 taxi_id 가 없음)."""

    current_make_key: str
    """현재 차량 제조사. `taxi_id` 만으로는 사람이 무슨 차인지 알 수 없어 함께 싣습니다 —
    콜 리스트에서 "지금 <현재 차량> 타시는데 <추천 차량> 으로" 를 쓰려면 필요합니다."""

    current_model_key: str
    """현재 차량 모델. `driver_car_suggestion.recommended_model_key` 와 같은 표기."""

    combined_mpg: float
    """차량 연비. EV 는 vehicle_master 관례대로 MPGe 로 정규화된 값."""

    monthly_mileage: float
    """월간 주행거리 (mile)."""

    monthly_fuel_cost: float
    """월간 연료비 (USD). 유종 차량은 휘발유 가격, 전기차는 충전 단가 적용."""

    monthly_rental_fee: float
    """월간 렌탈료 (USD). weekly_lease_fee 기반 환산."""

    monthly_net_profit: float
    """월간 순수익 (USD) = 하루 순수익(플랫폼 정산액 + Tip - 운행거리*연료단가) 의 합 - monthly_rental_fee."""
