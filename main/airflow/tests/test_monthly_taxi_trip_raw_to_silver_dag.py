"""월별 택시 운행 데이터 Raw→Bronze→Silver DAG 계약.

1. 감시 DAG만 스케줄을 갖고 이 DAG는 요청받은 월의 네 단계를 처리
2. 데이터 제공 주소와 선택적 연월을 월별 택시 운행 수집 핸들러에 전달
3. Spark 명령은 검증 또는 재수집된 월만 정제
4. 로컬은 Bash Spark, 운영은 공용 EMR Serverless에 같은 Spark job을 제출
5. 운영 EMR 대기는 배포 재시작에 안전한 deferrable 모드
6. 운영 필수 환경변수가 없으면 DAG 구성이 즉시 실패
7. Spark 성공·실패 뒤 GX 보고 task가 항상 실행되고 Silver 공개 전 완료
"""

from datetime import timedelta

import pytest
from airflow.task.trigger_rule import TriggerRule

from shared.airflow.common import lambda_invoke
from dags import monthly_taxi_trip_raw_to_silver_dag as dag_module


DAG = dag_module.monthly_taxi_trip_dag


def test_DAG는_HVFHV한종을_Raw부터_Silver까지_순서대로_처리한다():
    assert DAG.dag_id == "monthly_taxi_trip_raw_to_silver_pipeline"
    assert DAG.schedule is None
    assert set(DAG.task_ids) == {
        "raw_to_bronze",
        "validate_bronze",
        "bronze_to_silver",
        "report_gx_validation",
        "validate_silver",
    }
    assert DAG.get_task("raw_to_bronze").downstream_task_ids == {"validate_bronze"}
    assert DAG.get_task("validate_bronze").downstream_task_ids == {
        "bronze_to_silver",
        "report_gx_validation",
        "validate_silver",
    }
    assert DAG.get_task("bronze_to_silver").downstream_task_ids == {
        "report_gx_validation",
        "validate_silver"
    }
    assert DAG.get_task("report_gx_validation").downstream_task_ids == {
        "validate_silver"
    }
    assert DAG.get_task("report_gx_validation").trigger_rule == TriggerRule.ALL_DONE
    assert DAG.get_task("raw_to_bronze").retries == 2
    assert DAG.get_task("raw_to_bronze").retry_delay == timedelta(minutes=5)
    assert DAG.get_task("validate_bronze").retries == 0
    assert DAG.get_task("validate_silver").retries == 0


def test_API_주소는_환경변수_설정값을_사용한다():
    assert DAG.params["api_base_url"] == "http://source-api.test:8091"


def test_기본_서비스지역은_NYC다():
    assert DAG.params["service_area"] == "NYC"


def test_수집task는_데이터제공주소와_수동월을_HVFHV핸들러에_전달한다(monkeypatch):
    called = {}

    def handler(*, event):
        called.update(event)
        return {"year_month": "2026-08"}

    monkeypatch.setattr(lambda_invoke, "lambda_handler_for", lambda name, **_: handler)
    raw = DAG.get_task("raw_to_bronze").python_callable
    raw(
        params={
            "api_base_url": "http://source",
            "base_dir": "/bronze",
            "year": "2026",
            "month": "8",
            "service_area": "TX",
        }
    )

    assert called == {
        "api_base_url": "http://source",
        "base_dir": "/bronze",
        "year": "2026",
        "month": "8",
        "service_area": "TX",
    }


def test_Spark명령은_수집결과의_정확한_HVFHV파일을_사용한다():
    command = DAG.get_task("bronze_to_silver").bash_command
    assert "task_ids='validate_bronze'" in command
    assert "['locations'][0]" in command
    assert "['silver_version_path']" in command
    assert "--output_version" in command
    assert "--zone_lookup_path" not in command
    assert "--error_threshold {{ params.error_threshold }}" in command


def test_로컬은_기존_Bash_Spark_경로를_유지한다():
    operator = dag_module._local_bronze_to_silver()

    assert type(operator).__name__ == "BashOperator"
    assert "monthly_taxi_trip_bronze_to_silver/job.py" in operator.bash_command


def test_운영은_팀변수로_EMR_Serverless_job을_제출하고_완료까지_기다린다(
    monkeypatch,
):
    monkeypatch.setenv("EMR_APPLICATION_ID", "app-test")
    monkeypatch.setenv(
        "EMR_EXECUTION_ROLE_ARN",
        "arn:aws:iam::123456789012:role/theone-spark-emr-exec",
    )
    monkeypatch.setenv("DATA_LAKE_S3_BUCKET", "test-lake")

    operator = dag_module._emr_bronze_to_silver()
    spark_submit = operator.job_driver["sparkSubmit"]

    assert type(operator).__name__ == "EmrServerlessStartJobOperator"
    assert operator.application_id == "app-test"
    assert operator.wait_for_completion is True
    assert operator.deferrable is True
    assert operator.cancel_on_kill is True
    assert (
        operator.waiter_delay * operator.waiter_max_attempts
        < operator.execution_timeout.total_seconds()
    )
    assert spark_submit["entryPoint"] == dag_module.EMR_ENTRY_POINT
    assert "--env" in spark_submit["entryPointArguments"]
    assert "prod" in spark_submit["entryPointArguments"]
    assert operator.configuration_overrides["monitoringConfiguration"][
        "s3MonitoringConfiguration"
    ]["logUri"] == "s3://test-lake/logs/emr-serverless/"


def test_운영_EMR_필수변수가_없으면_누락된_이름으로_실패한다(monkeypatch):
    for name in (
        "EMR_APPLICATION_ID",
        "EMR_EXECUTION_ROLE_ARN",
        "DATA_LAKE_S3_BUCKET",
    ):
        monkeypatch.delenv(name, raising=False)

    with pytest.raises(ValueError, match="EMR_APPLICATION_ID"):
        dag_module._emr_bronze_to_silver()
