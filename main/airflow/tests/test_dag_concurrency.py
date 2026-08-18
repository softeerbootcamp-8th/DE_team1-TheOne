"""같은 출력 파티션을 쓰는 DAG의 동시 실행 제한 시나리오.

1. HVFHV DAG는 한 번에 하나의 run만 실행
2. 기사 마스터 수집 DAG는 한 번에 하나의 run만 실행
"""

from dags.hvfhv_raw_to_silver_dag import hvfhv_dag
from dags.driver_master_raw_to_silver_dag import driver_master_raw_to_silver_dag


def test_HVFHV_DAG는_동시에_하나의_run만_실행한다():
    assert hvfhv_dag.max_active_runs == 1


def test_기사마스터_DAG는_동시에_하나의_run만_실행한다():
    assert driver_master_raw_to_silver_dag.max_active_runs == 1
