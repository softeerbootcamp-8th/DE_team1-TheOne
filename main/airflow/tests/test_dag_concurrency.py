"""같은 출력 파티션을 쓰는 DAG의 동시 실행 제한 시나리오.

1. HVFHV DAG는 한 번에 하나의 run만 실행
2. 기사 마스터 수집 DAG는 한 번에 하나의 run만 실행
3. 보유 차량 수집 DAG는 한 번에 하나의 run만 실행
"""

from dags.monthly_taxi_trip_raw_to_silver_dag import monthly_taxi_trip_dag
from dags.driver_vehicle_monthly_snapshot_raw_to_silver_dag import driver_vehicle_monthly_snapshot_raw_to_silver_dag
from dags.lease_vehicle_inventory_raw_to_silver_dag import (
    lease_vehicle_inventory_raw_to_silver_dag,
)


def test_HVFHV_DAG는_동시에_하나의_run만_실행한다():
    assert monthly_taxi_trip_dag.max_active_runs == 1


def test_기사마스터_DAG는_동시에_하나의_run만_실행한다():
    assert driver_vehicle_monthly_snapshot_raw_to_silver_dag.max_active_runs == 1


def test_보유차량_DAG는_동시에_하나의_run만_실행한다():
    assert lease_vehicle_inventory_raw_to_silver_dag.max_active_runs == 1
