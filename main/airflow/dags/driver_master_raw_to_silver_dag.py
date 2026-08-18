"""회사 원천 데이터(기사 계약·보유 차량) Raw → Bronze → Silver 파이프라인을 선언합니다."""

import os
from datetime import datetime, timedelta

from airflow.sdk import Param, dag

from shared.airflow.common.slack_failure_callback import (
    slack_failure_callback,
    slack_retry_alert_callback,
)
from main.airflow.common.monthly_bronze import (
    DEFAULT_API_BASE_URL,
    DEFAULT_BRONZE_DIR,
)
from main.airflow.scripts.driver_master_raw_to_silver.tasks import (
    DEFAULT_SILVER_DIR,
    bronze_to_silver_task,
    raw_to_bronze_task,
    validate_bronze_task,
    validate_silver_task,
)
from main.airflow.scripts.lease_vehicle_inventory_raw_to_silver.tasks import (
    DEFAULT_SILVER_DIR as DEFAULT_INVENTORY_SILVER_DIR,
    bronze_to_silver_task as inventory_bronze_to_silver_task,
    raw_to_bronze_task as inventory_raw_to_bronze_task,
    validate_bronze_task as validate_inventory_bronze_task,
    validate_silver_task as validate_inventory_silver_task,
)


default_args = {
    "owner": "DE_team1",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=15),
    "on_retry_callback": slack_retry_alert_callback,
    "on_failure_callback": slack_failure_callback,
}


@dag(
    dag_id="driver_master_raw_to_silver_pipeline",
    default_args=default_args,
    description="회사 원천 데이터(기사 계약·보유 차량) Raw→Bronze→Silver 파이프라인",
    schedule="0 0 10 * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["driver", "taxi", "master", "inventory", "bronze", "silver"],
    params={
        "year": Param(None, type=["string", "null"]),
        "month": Param(None, type=["string", "null"]),
        "api_base_url": Param(
            os.getenv("SYNTHETIC_SOURCE_API_URL", DEFAULT_API_BASE_URL),
            type="string",
        ),
        "base_dir": Param(DEFAULT_BRONZE_DIR, type="string"),
        "silver_dir": Param(DEFAULT_SILVER_DIR, type="string"),
        "inventory_silver_dir": Param(DEFAULT_INVENTORY_SILVER_DIR, type="string"),
    },
)
def driver_master_raw_to_silver_pipeline():
    raw = raw_to_bronze_task.override(
        retries=2,
        retry_delay=timedelta(minutes=5),
        retry_exponential_backoff=True,
    )()
    checked = validate_bronze_task.override(retries=0)(raw)
    silver = bronze_to_silver_task(checked)
    validate_silver_task.override(retries=0)(silver, checked)

    # 같은 원천 API의 다른 데이터셋이라 한 DAG에 두되, 한쪽 실패가 다른 쪽을
    # 막지 않도록 의존성 없는 별도 분기로 둡니다.
    inventory_raw = inventory_raw_to_bronze_task.override(
        retries=2,
        retry_delay=timedelta(minutes=5),
        retry_exponential_backoff=True,
    )()
    inventory_checked = validate_inventory_bronze_task.override(retries=0)(
        inventory_raw
    )
    inventory_silver = inventory_bronze_to_silver_task(inventory_checked)
    validate_inventory_silver_task.override(retries=0)(
        inventory_silver, inventory_checked
    )


driver_master_raw_to_silver_dag = driver_master_raw_to_silver_pipeline()
