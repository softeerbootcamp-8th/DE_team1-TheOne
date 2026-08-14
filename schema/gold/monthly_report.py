"""[월간 리포트] 스키마.

월 1회, 그 달의 차량 추천 결과를 요약한 Gold 테이블. 1개월 = 1행.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class MonthlyReport:
    year_month: str
    """집계 대상 월 (YYYY-MM). PK."""

    threshold_profit_increase: float
    """기준선 (USD). 순수익 증가액이 이 값 이상이어야 차량 교체를 추천 — 파이프라인 실행 시 파라미터로 입력받아 그대로 기록."""

    recommended_driver_count: int
    """expected_net_profit_increase >= threshold_profit_increase 로 추천된 기사 수."""

    avg_net_profit_increase_per_driver: float
    """추천된 기사들의 expected_net_profit_increase 평균 (USD)."""

    avg_revenue_increase_per_driver: float
    """추천된 기사들의 expected_revenue_increase(=객단가 증가액) 평균 (USD)."""

    total_revenue_increase: float
    """추천된 기사들의 expected_revenue_increase 합계 (USD). 회사가 얻는 총 렌탈료 매출 증가분."""
