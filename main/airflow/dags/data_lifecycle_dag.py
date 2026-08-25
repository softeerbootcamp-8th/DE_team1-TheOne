"""Bronze·Silver·Gold의 만료된 구버전을 매일 정리합니다."""

from datetime import datetime, timedelta

from airflow.sdk import Param, dag

from main.airflow.scripts.data_lifecycle.tasks import (
    DEFAULT_RETENTION_DAYS,
    cleanup_expired_gold_versions_task,
    cleanup_expired_s3_versions_task,
)
from shared.airflow.common.slack_failure_callback import (
    slack_failure_callback,
    slack_retry_alert_callback,
)


default_args = {
    "owner": "DE_team1",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "on_retry_callback": slack_retry_alert_callback,
    "on_failure_callback": slack_failure_callback,
}


@dag(
    dag_id="data_lifecycle_cleanup",
    default_args=default_args,
    description="90일이 지난 Bronze·Silver·Gold 구버전과 격리 버전 정리",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["main", "maintenance", "s3", "postgres"],
    params={
        "retention_days": Param(
            DEFAULT_RETENTION_DAYS,
            type="integer",
            minimum=1,
        ),
        "dry_run": Param(False, type="boolean"),
    },
)
def data_lifecycle_cleanup():
    cleanup_expired_s3_versions_task()
    cleanup_expired_gold_versions_task()


data_lifecycle_dag = data_lifecycle_cleanup()
