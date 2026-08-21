"""Silver 4종으로 월별 Gold 3종을 만드는 파이프라인입니다."""

import os
from datetime import datetime, timedelta

from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Param, dag
from airflow.timetables.simple import IdentityMapper, PartitionedAssetTimetable

from main.airflow.common import assets
from shared.airflow.common.slack_failure_callback import (
    slack_failure_callback,
    slack_retry_alert_callback,
    slack_success_callback,
)
from main.airflow.scripts.hvfhv_silver_to_gold.tasks import (
    DEFAULT_PATHS,
    ROOT,
    validate_gold_task,
    validate_inputs_task,
)


default_args = {
    "owner": "DE_team1",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "retry_exponential_backoff": True,
    "on_retry_callback": slack_retry_alert_callback,
    "on_failure_callback": slack_failure_callback,
}

JOB_ENV = os.getenv("SPARK_JOB_ENV", "local")
EMR_ENTRY_POINT = "/home/hadoop/main/spark/jobs/silver_to_gold/job.py"
EMR_SPARK_SUBMIT_PARAMETERS = (
    "--conf spark.driver.cores=2 --conf spark.driver.memory=6g "
    "--conf spark.executor.cores=2 --conf spark.executor.memory=6g "
    "--conf spark.emr-serverless.driverEnv.PYTHONPATH=/home/hadoop "
    "--conf spark.executorEnv.PYTHONPATH=/home/hadoop"
)


def _required_prod_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ValueError(f"SPARK_JOB_ENV=prod는 {name} 환경변수가 필요합니다")
    return value


def _local_build_gold() -> BashOperator:
    return BashOperator(
        task_id="build_gold",
        bash_command=(
            f"python {ROOT}/main/spark/jobs/silver_to_gold/job.py "
            + "--monthly_taxi_trip_path "
            + "\"{{ task_instance.xcom_pull(task_ids='validate_inputs')['monthly_taxi_trip_path'] }}\" "
            + "--driver_vehicle_monthly_snapshot_path "
            + "\"{{ task_instance.xcom_pull(task_ids='validate_inputs')['driver_vehicle_monthly_snapshot_path'] }}\" "
            + "--lease_vehicle_inventory_path "
            + "\"{{ task_instance.xcom_pull(task_ids='validate_inputs')['lease_vehicle_inventory_path'] }}\" "
            + "--fuel_price_path "
            + "\"{{ task_instance.xcom_pull(task_ids='validate_inputs')['fuel_price_path'] }}\" "
            + "--year {{ task_instance.xcom_pull(task_ids='validate_inputs')['year'] }} "
            + "--month {{ task_instance.xcom_pull(task_ids='validate_inputs')['month'] }} "
            + "--threshold_profit_increase {{ params.threshold_profit_increase }} "
            + "--output_dir {{ params.output_dir }} "
            + "{% if params.dry_run %}--dry-run{% endif %}"
        ),
        env={
            **os.environ,
            "PYTHONPATH": (
                f"{ROOT}:{ROOT}/main/spark:{ROOT}/libs/pipeline_core"
                f":{os.getenv('PYTHONPATH', '')}"
            ),
        },
    )


def _emr_build_gold() -> EmrServerlessStartJobOperator:
    application_id = _required_prod_env("EMR_APPLICATION_ID")
    execution_role_arn = _required_prod_env("EMR_EXECUTION_ROLE_ARN")
    bucket = _required_prod_env("DATA_LAKE_S3_BUCKET")
    gold_secret_id = _required_prod_env("GOLD_DATABASE_SECRET_ID")
    xcom = "task_instance.xcom_pull(task_ids='validate_inputs')"
    return EmrServerlessStartJobOperator(
        task_id="build_gold",
        application_id=application_id,
        execution_role_arn=execution_role_arn,
        name="silver-to-gold-{{ ds_nodash }}",
        job_driver={
            "sparkSubmit": {
                "entryPoint": EMR_ENTRY_POINT,
                "entryPointArguments": [
                    "--env", "prod",
                    "--bucket", bucket,
                    "--gold_secret_id", gold_secret_id,
                    "--monthly_taxi_trip_path", f"{{{{ {xcom}['monthly_taxi_trip_path'] }}}}",
                    "--driver_vehicle_monthly_snapshot_path", f"{{{{ {xcom}['driver_vehicle_monthly_snapshot_path'] }}}}",
                    "--lease_vehicle_inventory_path", f"{{{{ {xcom}['lease_vehicle_inventory_path'] }}}}",
                    "--fuel_price_path", f"{{{{ {xcom}['fuel_price_path'] }}}}",
                    "--year", f"{{{{ {xcom}['year'] }}}}",
                    "--month", f"{{{{ {xcom}['month'] }}}}",
                    "--threshold_profit_increase", "{{ params.threshold_profit_increase }}",
                    "--dry-run", "{{ params.dry_run | lower }}",
                ],
                "sparkSubmitParameters": EMR_SPARK_SUBMIT_PARAMETERS,
            }
        },
        configuration_overrides={
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {"logUri": f"s3://{bucket}/emr-logs/"}
            }
        },
        aws_conn_id="aws_default",
        region_name=os.getenv("AWS_DEFAULT_REGION", "ap-northeast-2"),
        wait_for_completion=True,
        waiter_delay=60,
        waiter_max_attempts=180,
        # aiobotocore를 새로 추가하지 않고 LocalExecutor의 worker가 waiter를 폴링합니다.
        deferrable=False,
        execution_timeout=timedelta(hours=3),
    )


def _build_gold_operator():
    if JOB_ENV == "local":
        return _local_build_gold()
    if JOB_ENV == "prod":
        return _emr_build_gold()
    raise ValueError(f"알 수 없는 SPARK_JOB_ENV: {JOB_ENV!r}")


@dag(
    dag_id="hvfhv_silver_to_gold_pipeline",
    default_args=default_args,
    schedule=PartitionedAssetTimetable(
        assets=assets.GOLD_INPUTS,
        default_partition_mapper=IdentityMapper(),
    ),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["main", "hvfhv", "gold", "spark", "emr"],
    params={
        "year": Param(None, type=["string", "null"], pattern=r"^\d{4}$"),
        "month": Param(None, type=["string", "null"], pattern=r"^(0?[1-9]|1[0-2])$"),
        # 차량 교체 추천으로 집계할 최소 순수익 증가액(USD). Spark 잡이 required 로
        # 받는 값이라 기본값을 여기서 정합니다.
        #
        # 600 은 서비스 조건입니다 — "차를 바꿔서 월 $600 은 더 벌어야 기사가 움직인다"
        # 는 전제로 콜 리스트를 만듭니다. 낮추면 대상자가 늘지만 성사율이 떨어지고,
        # 높이면 반대입니다. 운영 기준이 바뀌면 코드가 아니라 이 파라미터로 조정하세요.
        # (근거: docs/METRICS.md - 4. 추천 기준선)
        "threshold_profit_increase": Param(600.0, type="number"),
        **{name: Param(path, type="string") for name, path in DEFAULT_PATHS.items()},
        "dry_run": Param(
            False,
            type="boolean",
            description="입력과 집계를 검증하되 Gold에는 적재하지 않음",
        ),
    },
)
def hvfhv_silver_to_gold_pipeline():
    build = _build_gold_operator()

    validate_inputs = validate_inputs_task.override(retries=0)()
    validate_gold = validate_gold_task.override(
        retries=0,
        on_success_callback=slack_success_callback,
    )()
    validate_inputs >> build >> validate_gold


# 다른 DAG 와 같은 규칙 — 팩토리 호출 결과를 모듈 속성으로 노출합니다.
# 계약 테스트가 `getattr(module, <변수명>)` 으로 DAG 객체를 찾습니다.
hvfhv_silver_to_gold_dag = hvfhv_silver_to_gold_pipeline()
