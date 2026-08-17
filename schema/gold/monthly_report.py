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
    """expected_net_profit_increase >= threshold_profit_increase 이고 expected_revenue_increase
    가 0 이상인 기사 수. 매출 증가액이 음수인 추천(회사 렌탈료 매출이 줄어드는 교체)은 뺀다."""

    avg_net_profit_increase_per_driver: float
    """추천된 기사들의 expected_net_profit_increase 평균 (USD)."""

    avg_revenue_increase_per_driver: float
    """추천된 기사들의 expected_revenue_increase(=객단가 증가액) 평균 (USD)."""

    total_revenue_increase: float
    """추천된 기사들의 expected_revenue_increase 합계 (USD). 회사가 얻는 총 렌탈료 매출 증가분."""

    # --- 계보: 이 숫자가 어떤 입력으로 나왔는지 ---
    # 위 값들은 입력이 조금만 달라도 바뀝니다. 어느 시점 카탈로그·어느 달 연료비를 썼는지
    # 남기지 않으면, 두 벌의 Gold 를 놓고 무엇이 달랐는지 되짚을 수 없습니다.
    # 월 1행이라 컬럼을 늘려도 비용이 없습니다.
    #
    # 배정 버전(`assignment_version`)은 없습니다 (#471). 기사-운행 매칭이 가짜 데이터
    # API 로 옮겨가(#450) Silver 가 `taxi_id` + 리스 기간으로 결정적으로 조인만 하므로,
    # 같은 입력이면 같은 결과입니다 — 구분할 버전이 생기지 않습니다.

    vehicle_master_collected_date: str
    """쓴 `vehicle_master` 의 `collected_date`. 대상 월 이하 파티션이 없으면 이후 수집분으로
    물러서는데(hvfhv_silver_to_gold_dag), 그 사실이 로그에만 남아 있으면 결과만 보고는 모릅니다."""

    gas_ev_price_month: str
    """쓴 연료비의 `collected_month` (YYYY-MM). 대상 월과 다르면 다른 시점 단가로 계산된 것입니다."""
