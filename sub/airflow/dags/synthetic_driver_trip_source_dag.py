"""검증된 월별 HVFHV로 운행·리스·보유 차량 데이터를 생성합니다."""

from datetime import datetime, timedelta, timezone

from airflow.sdk import Param, dag

from shared.airflow.common.slack_failure_callback import (
    slack_failure_callback,
    slack_retry_alert_callback,
)
from sub.airflow.scripts.synthetic_driver_trip_source.spark_operator import (
    DEFAULT_STORAGE,
    build_operator,
)
from sub.airflow.scripts.synthetic_driver_trip_source.tasks import (
    DEFAULT_PATHS,
    collect_source_input_task,
    validate_inputs_task,
    validate_release_task,
)

default_args = {
    "owner": "DE_team1",
    "retries": 1,
    "retry_delay": timedelta(minutes=30),
    "on_retry_callback": slack_retry_alert_callback,
    "on_failure_callback": slack_failure_callback,
}


@dag(
    dag_id="synthetic_driver_trip_source_pipeline",
    default_args=default_args,
    description="월별 HVFHV에 기사·차량을 배정해 제공 데이터 3종 생성",
    schedule="0 0 10 * *",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["sub", "hvfhv", "driver", "synthetic", "source", "spark"],
    params={
        "year": Param(None, type=["string", "null"], pattern=r"^\d{4}$"),
        "month": Param(None, type=["string", "null"], pattern=r"^(0?[1-9]|1[0-2])$"),
        # 비우면(None) CLI 플래그 자체가 렌더링되지 않아 job 이 config/generation.json
        # 의 global_seed 를 읽습니다. 기본값을 두면 항상 이 값이 실려 설정을 가립니다.
        "seed": Param(None, type=["integer", "null"]),
        # 비우면 플래그 자체를 생략해 config 의 allocation.bucket_size 가 그대로 쓰입니다.
        "bucket_size": Param(
            None,
            type=["integer", "null"],
            description="기사 후보 버킷 크기. 비우면 config 의 allocation.bucket_size",
        ),
        # TEMPORARY(#452): 로컬 DAG smoke test용. 0이면 전체 월을 처리합니다.
        #
        # 0 이 아니면 생성 결과가 `<release_output_dir>/_temporary/test_row_limit=N/`
        # 아래로 갑니다. 가짜 데이터 API 는 그 위 디렉터리만 보므로, 하류 DAG 까지
        # 이어서 돌리려면 API 를 그 경로로 띄워야 합니다 — 안 그러면 404 입니다.
        #
        #   SOURCE_API_PORT=8091 \
        #   SOURCE_API_LOCAL_ROOT="data/source/synthetic_driver_trip_api/_temporary/test_row_limit=1000" \
        #     python -m sub.source_api.server
        "test_row_limit": Param(
            0,
            type="integer",
            minimum=0,
            description=(
                "임시 테스트 입력 행 수(0=전체). 0이 아니면 생성 결과가 "
                "_temporary/test_row_limit=N/ 아래로 가므로, API 를 그 경로를 "
                "--root 로 지정해 띄워야 하류 DAG 가 받을 수 있습니다"
            ),
        ),
        **{name: Param(path, type="string") for name, path in DEFAULT_PATHS.items()},
        # 읽기에도 씁니다. vehicle_master 를 어디서 찾을지가 이 값으로 갈립니다 —
        # EC2 는 바인드 마운트가 없어 local 로 두면 컨테이너 빈 디스크를 보게 됩니다.
        "storage": Param(
            DEFAULT_STORAGE,
            enum=["local", "s3"],
            description="입력(vehicle_master) 조회와 attribution·published 적재를 어디로 할지",
        ),
        # type 에 "null" 이 없으면 UI 트리거 폼이 **필수 입력**으로 취급해서 비워둘 수
        # 없습니다. 이건 드물게 쓰는 재정의값이고, 비우면 DATA_LAKE_S3_BUCKET 을 쓰는 게
        # 정상 경로입니다 (다른 DAG 들은 파라미터 없이 환경변수만 씁니다).
        "bucket": Param(
            None,
            type=["string", "null"],
            description="storage=s3일 때 버킷 재정의. 비우면 DATA_LAKE_S3_BUCKET",
        ),
    },
)
def synthetic_driver_trip_source_pipeline():
    build = build_operator()

    source = collect_source_input_task.override(
        retries=2,
        retry_delay=timedelta(minutes=5),
        retry_exponential_backoff=True,
    )()
    inputs = validate_inputs_task.override(retries=0)(source)
    inputs >> build >> validate_release_task.override(retries=0)()


synthetic_driver_trip_source_dag = synthetic_driver_trip_source_pipeline()
