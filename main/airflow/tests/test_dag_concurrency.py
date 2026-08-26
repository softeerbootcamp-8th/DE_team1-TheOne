"""지역별 출력 파티션을 쓰는 DAG의 동시 실행 제한 시나리오.

서로 다른 지역의 저장 경로는 ``service_area=`` 계층으로 격리되어 있으므로 세 지역
DagRun까지 병렬로 실행합니다. 네 번째 지역부터는 기존 실행이 끝날 때까지 대기합니다.

예외는 monthly_taxi_trip 입니다 — 같은 지역의 다른 달을 동시에 처리하면 #165 가드가
서로를 유실로 신고하므로 1 로 직렬화했습니다 (#1122).
"""

from main.airflow.common.assets import MAX_ACTIVE_SERVICE_AREA_RUNS
from dags.monthly_taxi_trip_raw_to_silver_dag import monthly_taxi_trip_dag
from dags.driver_vehicle_monthly_snapshot_raw_to_silver_dag import driver_vehicle_monthly_snapshot_raw_to_silver_dag
from dags.lease_vehicle_inventory_raw_to_silver_dag import (
    lease_vehicle_inventory_raw_to_silver_dag,
)


def test_HVFHV_DAG는_한_번에_한_run만_실행한다():
    """지역이 아니라 월이 문제입니다.

    이 DAG 은 대상 월이 수동 파라미터이고, validate_silver 의 #165 가드가 대상 월을
    뺀 나머지 월의 `_SUCCESS` 를 봅니다. 두 실행이 각자 다른 달을 처리하면 서로의
    마커가 사라진 것을 유실로 신고합니다 (#1122).
    """
    assert monthly_taxi_trip_dag.max_active_runs == 1


def test_기사마스터_DAG는_세_지역_run을_동시에_실행한다():
    assert (
        driver_vehicle_monthly_snapshot_raw_to_silver_dag.max_active_runs
        == MAX_ACTIVE_SERVICE_AREA_RUNS
    )


def test_보유차량_DAG는_세_지역_run을_동시에_실행한다():
    assert (
        lease_vehicle_inventory_raw_to_silver_dag.max_active_runs
        == MAX_ACTIVE_SERVICE_AREA_RUNS
    )
