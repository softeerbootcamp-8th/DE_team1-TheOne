"""HVFHV Silver → Gold 변환. 재작성 중 (#530).

Gold 입력을 원천 Silver 4종에서 직접 만드는 모델로 바뀌면서, 이 모듈이 의존하던
`hvfhv_driver_trip`·`schema/gold` 구스키마가 없어졌습니다. 새 모델에 맞춘 구현이
나오기 전까지 호출 지점(`job.py`)이 즉시 실패하도록 자리만 지킵니다.
"""

from __future__ import annotations

from pyspark.sql import DataFrame

_NOT_IMPLEMENTED = (
    "silver_to_gold 변환은 재작성 중입니다 (#530) — "
    "Gold 입력이 원천 Silver 4종 직접 결합으로 바뀌는 작업이 끝나야 다시 동작합니다."
)


def enrich_trips_with_fuel_cost(
    trips: DataFrame, gas_ev_price: DataFrame, vehicle_master: DataFrame
) -> DataFrame:
    raise NotImplementedError(_NOT_IMPLEMENTED)


def build_driver_monthly_aggregation(
    enriched: DataFrame, vehicle_master: DataFrame, year_month: str, days_in_month: int
) -> DataFrame:
    raise NotImplementedError(_NOT_IMPLEMENTED)


def build_monthly_vehicle_recommendation(
    enriched: DataFrame,
    vehicle_master: DataFrame,
    driver_aggregation: DataFrame,
    year_month: str,
    days_in_month: int,
) -> DataFrame:
    raise NotImplementedError(_NOT_IMPLEMENTED)


def build_monthly_report(
    recommendation: DataFrame,
    year_month: str,
    threshold_profit_increase: float,
    *,
    vehicle_master_collected_date: str,
    gas_ev_price_month: str,
) -> DataFrame:
    raise NotImplementedError(_NOT_IMPLEMENTED)
