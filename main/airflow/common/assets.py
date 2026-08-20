"""Gold 입력으로 쓰는 월별 Silver Asset 정의.

URI 는 저장 위치가 아니라 데이터 제품의 논리 이름입니다. 그래서 로컬 파일과 S3
어느 쪽에서 실행해도 생산자와 Gold DAG 가 같은 Asset 을 봅니다. 실제 대상 월은
각 이벤트의 ``year_month`` 파티션 키로 구분합니다.
"""

from airflow.sdk import Asset


HVFHV_SILVER = Asset("silver://hvfhv")
DRIVER_VEHICLE_MONTHLY_SNAPSHOT_SILVER = Asset(
    "silver://driver_vehicle_monthly_snapshot"
)
LEASE_VEHICLE_INVENTORY_SILVER = Asset("silver://lease_vehicle_inventory")
FUEL_PRICE_SILVER = Asset("silver://gas_ev_price")

# ponytail: Asset 이벤트를 변경으로 간주. 중복 실행 비용이 커질 때 입력 해시로 승격.
GOLD_INPUTS = (
    HVFHV_SILVER
    | DRIVER_VEHICLE_MONTHLY_SNAPSHOT_SILVER
    | LEASE_VEHICLE_INVENTORY_SILVER
    | FUEL_PRICE_SILVER
)


def publish_month_partition(outlet_events, asset: Asset, year_month: str) -> None:
    """태스크 검증을 통과한 월을 partition-aware Asset 이벤트로 기록합니다."""
    if outlet_events is not None:
        outlet_events[asset].add_partitions(year_month)
