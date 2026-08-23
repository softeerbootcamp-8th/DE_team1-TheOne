"""리스 업체 보유 차량 Raw → Bronze → Silver 파이프라인을 선언합니다.

기사 계약(`driver_vehicle_leases`)과 같은 원천 API·같은 릴리스 월에서 오지만,
한쪽 원천이 늦어도 다른 쪽이 멈추지 않도록 DAG 를 나눠 둡니다. 두 DAG 는 서로 다른
Bronze·Silver 데이터셋 디렉터리에 쓰므로 같은 파티션을 다투지 않습니다.
"""

import os
from datetime import datetime, timedelta

from airflow.sdk import Param, dag

from shared.airflow.common.slack_failure_callback import (
    slack_failure_callback,
    slack_retry_alert_callback,
)
from main.airflow.common.assets import DEFAULT_SERVICE_AREA
from main.airflow.scripts.lease_vehicle_inventory_raw_to_silver.tasks import (
    DEFAULT_API_BASE_URL,
    DEFAULT_BRONZE_DIR,
    DEFAULT_SILVER_DIR,
    bronze_to_silver_task,
    raw_to_bronze_task,
    validate_bronze_task,
    validate_silver_task,
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
    dag_id="lease_vehicle_inventory_raw_to_silver_pipeline",
    default_args=default_args,
    description="리스 업체 보유 차량 Raw→Bronze→Silver 파이프라인",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["main", "lease", "vehicle", "inventory", "bronze", "silver"],
    params={
        "year": Param(None, type=["string", "null"], pattern=r"^\d{4}$"),
        "month": Param(None, type=["string", "null"], pattern=r"^(0?[1-9]|1[0-2])$"),
        "api_base_url": Param(
            os.getenv("SOURCE_API_URL", DEFAULT_API_BASE_URL),
            type="string",
        ),
        "base_dir": Param(DEFAULT_BRONZE_DIR, type="string"),
        "silver_dir": Param(DEFAULT_SILVER_DIR, type="string"),
        "service_area": Param(
            DEFAULT_SERVICE_AREA,
            type="string",
            pattern=r"^[A-Z][A-Z0-9_]*$",
            description="대상 지역 코드 (예: NYC). AWS 리전과 무관합니다",
        ),
    },
)
def lease_vehicle_inventory_raw_to_silver_pipeline():
    raw = raw_to_bronze_task.override(
        retries=2,
        retry_delay=timedelta(minutes=5),
        retry_exponential_backoff=True,
    )()
    checked = validate_bronze_task.override(retries=0)(raw)
    silver = bronze_to_silver_task(checked)
    validate_silver_task.override(retries=0)(silver, checked)


lease_vehicle_inventory_raw_to_silver_dag = lease_vehicle_inventory_raw_to_silver_pipeline()
