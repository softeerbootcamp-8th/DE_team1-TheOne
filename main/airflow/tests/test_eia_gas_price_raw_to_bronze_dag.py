"""EIA 휘발유 요금 Raw→Bronze DAG 계약. 이슈 #498.

`sub` 에 있던 이 DAG 이 `main` 으로 옮겨오면서, 거기서 지키던 계약을 그대로 가져옵니다
(`sub/airflow/tests/test_dag_module_contracts.py` 의 SCHEDULES·RETRY_CONTRACTS).
옮기면서 assert 가 줄면 "옮겼는데 검증은 사라진" 상태가 되고, 그건 실패가 아니라
아무도 모르는 채로 남습니다.

1. 월간 스케줄과 두 단계 task 구성
2. 수집은 재시도하고 검증은 재시도하지 않음 — 외부 원천은 흔들려도 다시 받으면 되지만,
   검증 실패는 다시 돌려도 같은 결과라 재시도가 시간만 씁니다
"""

from datetime import timedelta

from dags import eia_gas_price_raw_to_bronze_dag as dag_module


DAG = dag_module.eia_gas_price_raw_to_bronze_dag


def test_DAG는_월간_스케줄로_수집과_검증을_순서대로_처리한다():
    assert DAG.dag_id == "eia_gas_price_raw_to_bronze_pipeline"
    assert DAG.schedule == "0 5 1 * *"
    assert set(DAG.task_ids) == {"raw_to_bronze", "validate_bronze"}
    assert DAG.get_task("raw_to_bronze").downstream_task_ids == {"validate_bronze"}
    assert DAG.catchup is False and DAG.max_active_runs == 1


def test_수집은_재시도하고_검증은_재시도하지_않는다():
    collection = DAG.get_task("raw_to_bronze")
    assert collection.retries == 2
    assert collection.retry_delay == timedelta(minutes=5)
    assert collection.retry_exponential_backoff is True

    assert DAG.get_task("validate_bronze").retries == 0
