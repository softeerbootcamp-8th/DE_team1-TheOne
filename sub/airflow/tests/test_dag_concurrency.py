"""원천 데이터 DAG의 출력 파티션 동시 실행 제한."""

from dags.vehicle_catalog_raw_to_curated_dag import vehicle_catalog_dag


def test_차량_대장_DAG는_동시에_하나의_run만_실행한다():
    assert vehicle_catalog_dag.max_active_runs == 1
