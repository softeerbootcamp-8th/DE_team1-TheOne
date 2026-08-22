"""Silver 4종으로 월별 Gold 3종을 만드는 파이프라인입니다."""

import os
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Param, dag
from airflow.timetables.simple import IdentityMapper, PartitionedAssetTimetable

from main.airflow.common import assets
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


@dag(
    dag_id="monthly_taxi_trip_silver_to_gold_pipeline",
    default_args=default_args,
    schedule=PartitionedAssetTimetable(
        assets=assets.GOLD_INPUTS,
        default_partition_mapper=IdentityMapper(),
    ),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["main", "taxi", "gold", "spark"],
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
def monthly_taxi_trip_silver_to_gold_pipeline():
    build = BashOperator(
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

    validate_inputs = validate_inputs_task.override(retries=0)()
    validate_gold = validate_gold_task.override(
        retries=0,
        on_success_callback=slack_success_callback,
    )()
    validate_inputs >> build >> validate_gold


# 다른 DAG 와 같은 규칙 — 팩토리 호출 결과를 모듈 속성으로 노출합니다.
# 계약 테스트가 `getattr(module, <변수명>)` 으로 DAG 객체를 찾습니다.
monthly_taxi_trip_silver_to_gold_dag = monthly_taxi_trip_silver_to_gold_pipeline()
