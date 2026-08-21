"""HVFHV+taxi_id 데이터 Raw→Bronze→Silver DAG 계약.

1. 기존 월 스케줄과 네 단계 의존성 유지
2. 데이터 제공 주소와 선택적 연월을 기존 HVFHV 수집 핸들러에 전달
3. Spark 명령은 검증 또는 재수집된 월만 정제
"""

from datetime import timedelta

from airflow.providers.standard.operators.python import ShortCircuitOperator

from dags import hvfhv_raw_to_silver_dag as dag_module
from main.airflow.scripts.hvfhv_raw_to_silver import tasks as task_module


DAG = dag_module.hvfhv_dag


def test_DAG는_HVFHV한종을_Raw부터_Silver까지_순서대로_처리한다():
    assert DAG.dag_id == "hvfhv_raw_to_silver_pipeline"
    assert DAG.schedule == "@daily"
    assert set(DAG.task_ids) == {
        "raw_to_bronze",
        "validate_bronze",
        "bronze_to_silver",
        "validate_silver",
    }
    assert DAG.get_task("raw_to_bronze").downstream_task_ids == {"validate_bronze"}
    assert DAG.get_task("validate_bronze").downstream_task_ids == {
        "bronze_to_silver",
        "validate_silver",
    }
    assert DAG.get_task("bronze_to_silver").downstream_task_ids == {
        "validate_silver"
    }
    assert DAG.get_task("raw_to_bronze").retries == 2
    assert DAG.get_task("raw_to_bronze").retry_delay == timedelta(minutes=5)
    assert DAG.get_task("validate_bronze").retries == 0
    assert DAG.get_task("validate_silver").retries == 0
    assert isinstance(DAG.get_task("validate_bronze"), ShortCircuitOperator)


def test_수집task는_데이터제공주소와_수동월을_HVFHV핸들러에_전달한다(monkeypatch):
    called = {}

    def handler(*, event):
        called.update(event)
        return {"year_month": "2026-08"}

    monkeypatch.setattr(task_module, "lambda_handler_for", lambda name: handler)
    raw = DAG.get_task("raw_to_bronze").python_callable
    raw(
        params={
            "api_base_url": "http://source",
            "base_dir": "/bronze",
            "year": "2026",
            "month": "8",
        }
    )

    assert called == {
        "api_base_url": "http://source",
        "base_dir": "/bronze",
        "year": "2026",
        "month": "8",
    }


def test_Spark명령은_수집결과의_정확한_HVFHV파일을_사용한다():
    command = DAG.get_task("bronze_to_silver").bash_command
    assert "task_ids='validate_bronze'" in command
    assert "['locations'][0]" in command
    assert "--zone_lookup_path" not in command
    assert "--error_threshold 0.05" in command
