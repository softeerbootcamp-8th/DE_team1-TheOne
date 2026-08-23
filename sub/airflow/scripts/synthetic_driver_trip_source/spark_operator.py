"""`build_source_release` 를 어디서 실행할지 고릅니다 — 로컬 Spark 또는 EMR Serverless.

EC2 에서 `local[3]` 로 돌리면 Airflow scheduler·Postgres 와 자원을 다투고, TLC 월별
운행 수백만 건에 t4g 인스턴스가 오래 붙잡힙니다. 그 시간에 배포가 끼면 컨테이너가
재생성되며 실행 중인 DAG 이 끊깁니다.

기본값은 `local` 입니다 — 로컬 pyspark 는 hadoop-aws jar 이 없어 `s3://` 를 읽지
못하므로(#712) 로컬 개발은 로컬 경로로 계속 돌려야 합니다.

DAG 파일이 아니라 여기 있는 이유 — `sub` 의 DAG 파일은 `@dag` 팩토리 하나만 정의하는
계약입니다(`test_dag_module_contracts.py`).
"""

import os
from datetime import timedelta

from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
from airflow.providers.standard.operators.bash import BashOperator

from sub.airflow.scripts.synthetic_driver_trip_source.tasks import ROOT

JOB_ENV = os.getenv("SPARK_JOB_ENV", "local")

# `storage` params 의 기본값도 같은 스위치를 따라야 합니다. prod 인데 local 이 기본이면
# `collect_source_input` 이 컨테이너 디스크에 내려받고, EMR 은 `--storage s3` 로 제출돼
# 워커가 있지도 않은 S3 키를 찾습니다. 사람이 매번 폼에서 s3 를 골라야 하는 구조는
# 한 번 잊으면 수십 분 뒤에 실패로 돌아옵니다.
DEFAULT_STORAGE = "s3" if JOB_ENV == "prod" else "local"

# theone-spark 이미지 안 경로입니다 (shared/spark/Dockerfile 이 /home/hadoop 에 COPY).
EMR_ENTRY_POINT = "/home/hadoop/sub/spark/jobs/driver_assignment/source_job.py"
# 월 수백만 트립의 join·window·groupBy와 (bucket × service_date)
# applyInPandas 그룹을 최대 10 task 로 병렬 처리합니다. executor 당 Python worker 를
# 2개로 제한하고, Python·Arrow 버퍼가 쓰는 heap 밖 메모리는 8 GB overhead 로
# 분리합니다. dynamic allocation 상한으로 한 Job이 사용하는 executor 비용도 제한합니다.
EMR_SPARK_SUBMIT_PARAMETERS = (
    "--conf spark.driver.cores=2 --conf spark.driver.memory=6g "
    "--conf spark.driver.memoryOverhead=2g "
    "--conf spark.executor.cores=2 --conf spark.executor.memory=8g "
    "--conf spark.executor.memoryOverhead=8g "
    "--conf spark.dynamicAllocation.minExecutors=1 "
    "--conf spark.dynamicAllocation.initialExecutors=3 "
    "--conf spark.dynamicAllocation.maxExecutors=5 "
    "--conf spark.emr-serverless.driverEnv.PYTHONPATH=/home/hadoop "
    "--conf spark.executorEnv.PYTHONPATH=/home/hadoop"
)
_XCOM = "task_instance.xcom_pull(task_ids='validate_inputs')"


def _required_prod_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ValueError(f"SPARK_JOB_ENV=prod는 {name} 환경변수가 필요합니다")
    return value


def local_build() -> BashOperator:
    """컨테이너 안에서 직접 실행합니다. 입출력은 params 의 로컬 경로."""
    return BashOperator(
        task_id="build_source_release",
        bash_command=(
            f"python {ROOT}/sub/spark/jobs/driver_assignment/source_job.py "
            + f"--hvfhv_input_path {{{{ {_XCOM}['hvfhv_input_path'] }}}} "
            + f"--zone_lookup_path {{{{ {_XCOM}['zone_lookup_path'] }}}} "
            + f"--vehicle_master_path {{{{ {_XCOM}['vehicle_master_path'] }}}} "
            + "--state_output_dir {{ params.state_output_dir }} "
            + "--attribution_output_dir {{ params.attribution_output_dir }} "
            + "--release_output_dir {{ params.release_output_dir }} "
            + f"--year_month {{{{ {_XCOM}['year_month'] }}}} "
            + "{% if params.seed is not none %}--seed {{ params.seed }} {% endif %}"
            + "{% if params.bucket_size is not none %}--bucket_size {{ params.bucket_size }} {% endif %}"
            + "--test_row_limit {{ params.test_row_limit }} "
            + "--env local "
            + "--storage {{ params.storage }} "
            + "{% if params.bucket %}--bucket {{ params.bucket }}{% endif %}"
        ),
        env={
            **os.environ,
            "PYTHONPATH": (
                f"{ROOT}:{ROOT}/main/spark:{ROOT}/libs/pipeline_core"
                f":{os.getenv('PYTHONPATH', '')}"
            ),
        },
    )


def emr_build() -> EmrServerlessStartJobOperator:
    """EMR Serverless 에 제출합니다.

    입출력이 전부 S3 여야 합니다 — 워커는 컨테이너 로컬 디스크를 볼 수 없습니다.
    그래서 `--storage` 를 params 가 아니라 `s3` 로 **고정**합니다. params 에서 local
    로 두고 제출하면 executor 가 빈 디스크를 보고 수십 분 뒤에 죽습니다.
    """
    application_id = _required_prod_env("EMR_APPLICATION_ID")
    execution_role_arn = _required_prod_env("EMR_EXECUTION_ROLE_ARN")
    bucket = _required_prod_env("DATA_LAKE_S3_BUCKET")
    data_bucket = f"{{{{ params.bucket if params.bucket else {bucket!r} }}}}"
    return EmrServerlessStartJobOperator(
        task_id="build_source_release",
        application_id=application_id,
        execution_role_arn=execution_role_arn,
        name="synthetic-driver-trip-source-{{ ds_nodash }}",
        job_driver={
            "sparkSubmit": {
                "entryPoint": EMR_ENTRY_POINT,
                "entryPointArguments": [
                    "--hvfhv_input_path", f"{{{{ {_XCOM}['hvfhv_input_path'] }}}}",
                    "--zone_lookup_path", f"{{{{ {_XCOM}['zone_lookup_path'] }}}}",
                    "--vehicle_master_path", f"{{{{ {_XCOM}['vehicle_master_path'] }}}}",
                    "--state_output_dir", "{{ params.state_output_dir }}",
                    "--attribution_output_dir", "{{ params.attribution_output_dir }}",
                    "--release_output_dir", "{{ params.release_output_dir }}",
                    "--year_month", f"{{{{ {_XCOM}['year_month'] }}}}",
                    "--seed",
                    "{{ params.seed if params.seed is not none else 'config' }}",
                    "--bucket_size",
                    "{{ params.bucket_size if params.bucket_size is not none else 'config' }}",
                    "--test_row_limit", "{{ params.test_row_limit }}",
                    "--env", "prod",
                    "--storage", "s3",
                    "--bucket", data_bucket,
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
        deferrable=False,
        execution_timeout=timedelta(hours=3),
    )


def build_operator():
    if JOB_ENV == "local":
        return local_build()
    if JOB_ENV == "prod":
        return emr_build()
    raise ValueError(f"알 수 없는 SPARK_JOB_ENV: {JOB_ENV!r}")
