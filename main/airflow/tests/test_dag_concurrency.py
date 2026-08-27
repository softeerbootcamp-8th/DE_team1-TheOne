"""지역별 출력 파티션을 쓰는 DAG의 동시 실행 제한 시나리오.

서로 다른 지역의 저장 경로는 ``service_area=`` 계층으로 격리되어 있으므로 세 지역
DagRun까지 병렬로 실행합니다. 네 번째 지역부터는 기존 실행이 끝날 때까지 대기합니다.
"""

from main.airflow.common.assets import MAX_ACTIVE_SERVICE_AREA_RUNS
from dags.monthly_taxi_trip_raw_to_silver_dag import monthly_taxi_trip_dag
from dags.driver_vehicle_monthly_snapshot_raw_to_silver_dag import driver_vehicle_monthly_snapshot_raw_to_silver_dag
from dags.lease_vehicle_inventory_raw_to_silver_dag import (
    lease_vehicle_inventory_raw_to_silver_dag,
)


def test_HVFHV_DAG는_세_지역_run을_동시에_실행한다():
    """지역은 병렬입니다. 막는 것은 같은 지역의 동시 실행이고, 그건 DAG 상한이 아니라
    첫 태스크가 직접 확인합니다 (#1124).
    """
    assert monthly_taxi_trip_dag.max_active_runs == MAX_ACTIVE_SERVICE_AREA_RUNS


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
