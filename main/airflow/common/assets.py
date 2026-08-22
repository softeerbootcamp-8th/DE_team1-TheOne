"""Gold 입력으로 쓰는 월별 Silver Asset 정의.

URI 는 저장 위치가 아니라 데이터 제품의 논리 이름입니다. 그래서 로컬 파일과 S3
어느 쪽에서 실행해도 생산자와 Gold DAG 가 같은 Asset 을 봅니다. 실제 대상 월은
각 이벤트의 ``year_month`` 파티션 키로 구분합니다.
"""

from airflow.sdk import Asset


FUEL_PRICE_SILVER = Asset("silver://gas_ev_price")
API_SILVER_REFRESH_READY = Asset("silver://api_refresh_ready")

# API 3종은 감시 DAG가 변경된 Silver 실행을 모두 기다린 뒤 READY를 한 번만 냅니다.
GOLD_INPUTS = API_SILVER_REFRESH_READY | FUEL_PRICE_SILVER


def publish_month_partition(
    outlet_events,
    asset: Asset,
    year_month: str,
    *,
    dry_run: bool = False,
) -> None:
    """태스크 검증을 통과한 월을 partition-aware Asset 이벤트로 기록합니다."""
    if outlet_events is not None:
        event = outlet_events[asset]
        event.extra = {"dry_run": dry_run}
        event.add_partitions(year_month)
