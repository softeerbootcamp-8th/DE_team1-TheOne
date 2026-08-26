"""월별 택시 운행 데이터 Raw → Bronze → Silver 파이프라인입니다."""

import os
from datetime import datetime, timedelta

from airflow.models import Variable
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Param, dag

# provider 구현은 실패 사유를 KeyError 로 덮습니다 — shared 쪽 하위 클래스를 씁니다.
from shared.airflow.common.emr_serverless import EmrServerlessStartJobOperator
from shared.airflow.common.slack_failure_callback import (
    slack_failure_callback,
    slack_retry_alert_callback,
)
from main.airflow.common.assets import (
    DEFAULT_SERVICE_AREA,
)
from main.airflow.scripts.monthly_taxi_trip_raw_to_silver.tasks import (
    DEFAULT_SILVER_DIR,
    MONTHLY_TAXI_TRIP_ERROR_THRESHOLD,
    PROJECT_ROOT,
    raw_to_bronze_task,
    report_gx_validation_task,
    validate_bronze_task,
    validate_silver_task,
)

JOB_ENV = os.getenv("SPARK_JOB_ENV", "local")
EMR_ENTRY_POINT = (
    "/home/hadoop/main/spark/jobs/bronze_to_silver/"
    "monthly_taxi_trip_bronze_to_silver/job.py"
)
EMR_SPARK_SUBMIT_PARAMETERS = (
    "--conf spark.driver.cores=2 --conf spark.driver.memory=6g "
    "--conf spark.executor.cores=2 --conf spark.executor.memory=6g "
    "--conf spark.dynamicAllocation.minExecutors=1 --conf spark.dynamicAllocation.initialExecutors=5 --conf spark.dynamicAllocation.maxExecutors=5 "
    "--conf spark.emr-serverless.driverEnv.PYTHONPATH=/home/hadoop "
    "--conf spark.executorEnv.PYTHONPATH=/home/hadoop"
)


default_args = {
    "owner": "DE_team1",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=30),
    "on_retry_callback": slack_retry_alert_callback,
    "on_failure_callback": slack_failure_callback,
}


@dag(
    dag_id="monthly_taxi_trip_raw_to_silver_pipeline",
    default_args=default_args,
    description="월별 택시 운행 데이터 Raw -> Bronze -> Silver 파이프라인",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    # 이 DAG 만 1 입니다. 지역은 `service_area=` 경로로 격리되지만(#674) **같은 지역의
    # 다른 달은 아닙니다** — validate_silver 의 #165 가드가 대상 월을 뺀 나머지 월의
    # `_SUCCESS` 를 보고, 옆 실행이 Spark 를 시작하며 자기 마커를 지우면 그것을 유실로
    # 신고합니다. 마커는 Spark 가 지우고 validate_silver 가 다시 붙이므로 없는 구간이
    # Spark 실행 시간만큼 넓습니다. 실제로 8초 차이로 띄운 두 수동 실행이 서로를
    # 유실로 신고하고 양쪽 다 격리됐습니다 (#1122).
    #
    # 지역이 둘 이상 생기면 DAG 단위 상한으로는 "지역은 병렬, 월은 직렬" 을 표현할 수
    # 없으므로 지역별 Pool 같은 수단을 그때 도입합니다.
    max_active_runs=1,
    tags=["main", "monthly_taxi_trip", "bronze", "silver", "spark", "emr", "lambda"],
    params={
        "year": Param(
            None,
            type=["string", "null"],
            pattern=r"^\d{4}$",
            description="수동 수집 연도 (예: '2024'). 비워두면 실행일 기준 직전 달 자동 계산",
        ),
        "month": Param(
            None,
            type=["string", "null"],
            pattern=r"^(0?[1-9]|1[0-2])$",
            description="수동 수집 월 (예: '03' 또는 '3'). 비워두면 실행일 기준 직전 달 자동 계산",
        ),
        "service_area": Param(
            DEFAULT_SERVICE_AREA,
            type="string",
            pattern=r"^[A-Z][A-Z0-9_]*$",
            description="대상 지역 코드 (예: NYC). AWS 리전과 무관합니다",
        ),
        "api_base_url": Param(
            os.environ["SOURCE_API_URL"],
            type="string",
            minLength=1,
            description="월별 택시 운행 데이터 제공 주소",
        ),
        # 기본값을 Variable(hvfhv_error_threshold)에서 가져옵니다 — DAG 파싱
        # 시점 코드라 task 실행 전용인 airflow.sdk가 아니라 DB에 직접 접근하는
        # airflow.models.Variable을 씁니다(#743).
        #
        # 새 파라미터를 추가하면 test_main_dag_params.py의 기대 집합도 함께
        # 고쳐야 합니다 — 그 테스트가 파라미터 집합 완전일치를 요구합니다.
        "error_threshold": Param(
            float(
                Variable.get(
                    "hvfhv_error_threshold",
                    default_var=MONTHLY_TAXI_TRIP_ERROR_THRESHOLD,
                )
            ),
            type="number",
            description="Spark Silver 후보 불합격 행 허용 비율. 이상이면 적재 중단",
        ),
    },
)
def monthly_taxi_trip_raw_to_silver_pipeline():
    bronze_to_silver_task = _bronze_to_silver_operator()

    raw_result = raw_to_bronze_task.override(
        retries=2,
        retry_delay=timedelta(minutes=5),
        retry_exponential_backoff=True,
    )()
    bronze_checked = validate_bronze_task.override(retries=0)(raw_result)
    bronze_checked >> bronze_to_silver_task

    gx_reported = report_gx_validation_task(bronze_checked)
    silver_checked = validate_silver_task.override(retries=0)(bronze_checked)
    bronze_to_silver_task >> gx_reported
    bronze_to_silver_task >> silver_checked
    gx_reported >> silver_checked


def _required_prod_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ValueError(f"SPARK_JOB_ENV=prod는 {name} 환경변수가 필요합니다")
    return value


def _local_bronze_to_silver() -> BashOperator:
    return BashOperator(
        task_id="bronze_to_silver",
        bash_command=(
            f"python {PROJECT_ROOT}/main/spark/jobs/bronze_to_silver/monthly_taxi_trip_bronze_to_silver/job.py "
            "--input_path \"{{ task_instance.xcom_pull(task_ids='validate_bronze')"
            "['locations'][0] }}\" "
            f"--output_path {DEFAULT_SILVER_DIR} "
            "--output_version \"{{ task_instance.xcom_pull(task_ids='validate_bronze')"
            "['silver_version_path'] }}\" "
            "--service_area {{ params.service_area }} "
            "--error_threshold {{ params.error_threshold }}"
        ),
        env={
            **os.environ,
            "PYTHONPATH": (
                f"{PROJECT_ROOT}:{PROJECT_ROOT}/main/spark"
                f":{PROJECT_ROOT}/libs/pipeline_core:"
                f"{os.getenv('PYTHONPATH', '')}"
            ),
        },
    )


def _emr_bronze_to_silver() -> EmrServerlessStartJobOperator:
    application_id = _required_prod_env("EMR_APPLICATION_ID")
    execution_role_arn = _required_prod_env("EMR_EXECUTION_ROLE_ARN")
    bucket = _required_prod_env("DATA_LAKE_S3_BUCKET")
    xcom = "task_instance.xcom_pull(task_ids='validate_bronze')"
    return EmrServerlessStartJobOperator(
        task_id="bronze_to_silver",
        application_id=application_id,
        execution_role_arn=execution_role_arn,
        # ds_nodash는 날짜뿐이라 같은 날 실행이 여러 건이면 잡 이름이 겹쳐 콘솔에서
        # 구분되지 않습니다. 지역 축이 들어가면(#674) 지역들이 같은 날 도는 게
        # 정상이므로 더 아픕니다. Gold DAG가 #746에서 같은 이유로 run_id를 택했고
        # (logical_date 없는 Asset 트리거에서도 항상 있음) 그 방식을 따릅니다.
        name="monthly-taxi-trip-bronze-to-silver-{{ run_id }}",
        job_driver={
            "sparkSubmit": {
                "entryPoint": EMR_ENTRY_POINT,
                "entryPointArguments": [
                    "--env",
                    "prod",
                    "--bucket",
                    bucket,
                    "--input_path",
                    f"{{{{ {xcom}['locations'][0] }}}}",
                    "--output_version",
                    f"{{{{ {xcom}['silver_version_path'] }}}}",
                    "--service_area",
                    "{{ params.service_area }}",
                    "--error_threshold",
                    "{{ params.error_threshold }}",
                ],
                "sparkSubmitParameters": EMR_SPARK_SUBMIT_PARAMETERS,
            }
        },
        configuration_overrides={
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {"logUri": f"s3://{bucket}/logs/emr-serverless/"}
            }
        },
        aws_conn_id="aws_default",
        region_name=os.getenv("AWS_DEFAULT_REGION", "ap-northeast-2"),
        wait_for_completion=True,
        waiter_delay=60,
        waiter_max_attempts=180,
        # 배포로 triggerer가 재시작돼도 deferred 상태는 메타DB에서 이어받습니다.
        # cancel_on_kill은 사용자 취소에만 EMR Job을 정리해 비용 누수를 막습니다.
        deferrable=True,
        cancel_on_kill=True,
        execution_timeout=timedelta(hours=3, minutes=10),
    )


def _bronze_to_silver_operator():
    if JOB_ENV == "local":
        return _local_bronze_to_silver()
    if JOB_ENV == "prod":
        return _emr_bronze_to_silver()
    raise ValueError(f"알 수 없는 SPARK_JOB_ENV: {JOB_ENV!r}")


monthly_taxi_trip_dag = monthly_taxi_trip_raw_to_silver_pipeline()
