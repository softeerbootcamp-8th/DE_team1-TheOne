"""월별 Gold 3종(기사 집계·차량 추천·월간 리포트) 파이프라인을 선언합니다.

Asset 스케줄을 쓰지 않는 이유
-----------------------------
상류 세 개의 주기가 다릅니다 — 배정 월 1회, `vehicle_master` 주 1회, 가격 월 1회.
Asset 스케줄은 "마지막 실행 이후 **모든** Asset 이 갱신되면" 실행이라, 주기가 어긋나면
영영 안 도는 조합이 생깁니다(`vehicle_master_silver_dag` 가 제원을 AND 에서 뺀 것과
같은 이유). 대신 **입력 검증을 Spark 잡 앞에 두어** 상류 누락을 즉시 드러냅니다.
Spark 잡 안에서 실패하면 어느 상류가 문제인지 로그를 파야 알 수 있습니다.
"""

import os
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Param, dag

from shared.airflow.common.slack_failure_callback import (
    slack_failure_callback,
    slack_retry_alert_callback,
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


@dag(
    dag_id="hvfhv_silver_to_gold_pipeline",
    default_args=default_args,
    # 배정 DAG 가 매월 12일 01:00 이라 그 뒤에 둡니다.
    schedule="0 3 13 * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["hvfhv", "gold", "spark"],
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
    },
)
def hvfhv_silver_to_gold_pipeline():
    build = BashOperator(
        task_id="build_gold",
        bash_command=(
            f"python {ROOT}/main/spark/jobs/silver_to_gold/job.py "
            + "--trips_path {{ task_instance.xcom_pull(task_ids='validate_inputs')['trips_path'] }} "
            + "--vehicle_master_path "
            + "{{ task_instance.xcom_pull(task_ids='validate_inputs')['vehicle_master_path'] }} "
            + "--gas_ev_price_path "
            + "{{ task_instance.xcom_pull(task_ids='validate_inputs')['gas_ev_price_path'] }} "
            + "--year {{ task_instance.xcom_pull(task_ids='validate_inputs')['year'] }} "
            + "--month {{ task_instance.xcom_pull(task_ids='validate_inputs')['month'] }} "
            + "--threshold_profit_increase {{ params.threshold_profit_increase }} "
            + "--output_dir {{ params.output_dir }}"
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

    validate_inputs_task.override(retries=0)() >> build >> validate_gold_task.override(
        retries=0
    )()


# 다른 DAG 와 같은 규칙 — 팩토리 호출 결과를 모듈 속성으로 노출합니다.
# 계약 테스트가 `getattr(module, <변수명>)` 으로 DAG 객체를 찾습니다.
hvfhv_silver_to_gold_dag = hvfhv_silver_to_gold_pipeline()
