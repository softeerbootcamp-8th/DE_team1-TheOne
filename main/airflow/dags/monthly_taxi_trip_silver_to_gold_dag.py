"""Silver 4종으로 월별 Gold 2종을 만드는 파이프라인입니다."""

import os
from datetime import datetime, timedelta

from airflow.models import Variable
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Param, dag
from airflow.timetables.simple import IdentityMapper, PartitionedAssetTimetable

# provider 구현은 실패 사유를 KeyError 로 덮습니다 — shared 쪽 하위 클래스를 씁니다.
from shared.airflow.common.emr_serverless import EmrServerlessStartJobOperator
from main.airflow.common import assets
from main.airflow.common.assets import (
    DEFAULT_SERVICE_AREA,
    MAX_ACTIVE_SERVICE_AREA_RUNS,
)
from shared.airflow.common.slack_failure_callback import (
    slack_failure_callback,
    slack_retry_alert_callback,
    slack_success_callback,
)
from main.airflow.scripts.monthly_taxi_trip_silver_to_gold.tasks import (
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
    "--conf spark.dynamicAllocation.minExecutors=1 --conf spark.dynamicAllocation.initialExecutors=5 --conf spark.dynamicAllocation.maxExecutors=5 "
    "--conf spark.sql.shuffle.partitions=40 "
    "--conf spark.emr-serverless.driverEnv.PYTHONPATH=/home/hadoop "
    "--conf spark.executorEnv.PYTHONPATH=/home/hadoop"
)


def _required_prod_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ValueError(f"SPARK_JOB_ENV=prod는 {name} 환경변수가 필요합니다")
    return value


def _local_build_gold() -> BashOperator:
    common_tail = (
        "--year {{ task_instance.xcom_pull(task_ids='validate_inputs')['year'] }} "
        + "--month {{ task_instance.xcom_pull(task_ids='validate_inputs')['month'] }} "
        + "--service_area "
        + "{{ task_instance.xcom_pull(task_ids='validate_inputs')['service_area'] }}"
    )
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
            + f"{common_tail} "
            + "--output_dir {{ params.output_dir }} "
            + "--thresholds \"{{ params.thresholds }}\""
        ),
        # BashOperator 가 띄우는 별도 프로세스는 DAG 파싱 때의 sys.path 를 물려받지
        # 않습니다. spark/common/io.py 가 pipeline_core 를 import 하므로 그 경로까지
        # 넣어야 합니다 (#351, 앞서 #328 에서 같은 실수).
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
    gold_dsn = _required_prod_env("GOLD_DATABASE_URL")
    xcom = "task_instance.xcom_pull(task_ids='validate_inputs')"
    return EmrServerlessStartJobOperator(
        task_id="build_gold",
        application_id=application_id,
        execution_role_arn=execution_role_arn,
        # ds_nodash는 logical_date 기반이라 이 DAG의 Asset 트리거 실행에서 비어
        # 있을 수 있습니다(#746 배포 중 실제로 UndefinedError 확인). run_id는
        # 트리거 방식과 무관하게 항상 있습니다.
        name="silver-to-gold-{{ run_id }}",
        job_driver={
            "sparkSubmit": {
                "entryPoint": EMR_ENTRY_POINT,
                "entryPointArguments": [
                    "--env", "prod",
                    "--bucket", bucket,
                    "--gold_dsn", gold_dsn,
                    "--year", f"{{{{ {xcom}['year'] }}}}",
                    "--month", f"{{{{ {xcom}['month'] }}}}",
                    "--service_area", f"{{{{ {xcom}['service_area'] }}}}",
                    "--thresholds", "{{ params.thresholds }}",
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


def _build_gold_operator():
    if JOB_ENV == "local":
        return _local_build_gold()
    if JOB_ENV == "prod":
        return _emr_build_gold()
    raise ValueError(f"알 수 없는 SPARK_JOB_ENV: {JOB_ENV!r}")


@dag(
    dag_id="monthly_taxi_trip_silver_to_gold_pipeline",
    default_args=default_args,
    schedule=PartitionedAssetTimetable(
        assets=assets.GOLD_INPUTS,
        default_partition_mapper=IdentityMapper(),
    ),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=MAX_ACTIVE_SERVICE_AREA_RUNS,
    tags=["main", "taxi", "gold", "spark"],
    params={
        "year": Param(None, type=["string", "null"], pattern=r"^\d{4}$"),
        "month": Param(None, type=["string", "null"], pattern=r"^(0?[1-9]|1[0-2])$"),
        **{name: Param(path, type="string") for name, path in DEFAULT_PATHS.items()},
        # 수동 실행의 대상 지역. Asset 트리거 실행에서는 파티션 키가 이 값을
        # **덮어씁니다**(resolve_target_service_area 참고) — 이 파라미터는 기본값이
        # 있어서 우선하면 "TX:2026-08" 파티션을 NYC 로 적재하게 됩니다.
        #
        # 새 파라미터를 추가하면 test_main_dag_params.py의 기대 집합도 함께
        # 고쳐야 합니다 — 그 테스트가 파라미터 집합 완전일치를 요구합니다.
        "service_area": Param(
            DEFAULT_SERVICE_AREA,
            type="string",
            pattern=r"^[A-Z][A-Z0-9_]*$",
            description="수동 실행 대상 지역 코드 (예: NYC). AWS 리전과 무관합니다",
        ),
        # RevenueFirstAlgorithm(v2)이 스윕할 기사 순수익 증가 threshold 목록.
        # 기본값을 Variable(gold_recommendation_thresholds)에서 가져옵니다 — DAG
        # 파싱 시점 코드라 task 실행 전용인 airflow.sdk가 아니라 DB에 직접
        # 접근하는 airflow.models.Variable을 씁니다(#743 패턴).
        #
        # 새 파라미터를 추가하면 test_main_dag_params.py의 기대 집합도 함께
        # 고쳐야 합니다 — 그 테스트가 파라미터 집합 완전일치를 요구합니다.
        "thresholds": Param(
            Variable.get(
                "gold_recommendation_thresholds",
                default_var=[100, 200, 300, 400, 500],
                deserialize_json=True,
            ),
            type="array",
            items={"type": "integer"},
            description="v2가 스윕할 기사 순수익 증가 threshold(USD) 목록",
        ),
    },
)
def monthly_taxi_trip_silver_to_gold_pipeline():
    build = _build_gold_operator()

    validate_inputs = validate_inputs_task.override(retries=0)()
    validate_gold = validate_gold_task.override(
        retries=0,
        on_success_callback=slack_success_callback,
    )()
    validate_inputs >> build >> validate_gold


# 다른 DAG 와 같은 규칙 — 팩토리 호출 결과를 모듈 속성으로 노출합니다.
# 계약 테스트가 `getattr(module, <변수명>)` 으로 DAG 객체를 찾습니다.
monthly_taxi_trip_silver_to_gold_dag = monthly_taxi_trip_silver_to_gold_pipeline()
