"""같은 출력 파티션을 쓰는 DAG의 동시 실행 제한 시나리오.

1. 차량 대장 DAG는 한 번에 하나의 run만 실행
2. HVFHV DAG는 한 번에 하나의 run만 실행
"""

from dags.hvfhv_raw_to_silver_dag import hvfhv_dag
from dags.vehicle_catalog_raw_to_silver_dag import vehicle_catalog_dag


def test_차량_대장_DAG는_동시에_하나의_run만_실행한다():
    assert vehicle_catalog_dag.max_active_runs == 1


def test_HVFHV_DAG는_동시에_하나의_run만_실행한다():
    assert hvfhv_dag.max_active_runs == 1
