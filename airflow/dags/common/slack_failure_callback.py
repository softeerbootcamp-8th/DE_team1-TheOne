"""Slack 재시도 Alert와 최종 Fail 알림 콜백."""

import logging


logger = logging.getLogger(__name__)

SLACK_WEBHOOK_CONN_ID = "slack_webhook"
SLACK_RETRY_ALERT_TEXT = (
    ":warning: *Airflow Task Alert*\n"
    "*상태*: `재시도 예정`\n"
    "*DAG*: `{{ dag.dag_id }}`\n"
    "*Task*: `{{ ti.task_id }}`\n"
    "*Run*: `{{ run_id }}`\n"
    "*시도*: `{{ ti.try_number }} / {{ task.retries + 1 }}`\n"
    "*로그*: <{{ ti.log_url }}|Airflow 로그 열기>"
)

SLACK_FAILURE_TEXT = (
    ":red_circle: *Airflow Task Fail*\n"
    "*상태*: `최종 실패`\n"
    "*DAG*: `{{ dag.dag_id }}`\n"
    "*Task*: `{{ ti.task_id }}`\n"
    "*Run*: `{{ run_id }}`\n"
    "*시도*: `{{ ti.try_number }} / {{ task.retries + 1 }}`\n"
    "*로그*: <{{ ti.log_url }}|Airflow 로그 열기>"
)

try:
    from airflow.providers.slack.notifications.slack_webhook import (
        send_slack_webhook_notification,
    )
except ImportError as exc:
    logger.warning("Slack provider를 불러오지 못해 로깅 콜백을 사용합니다: %s", exc)

    def slack_retry_alert_callback(context):
        task_instance = context.get("task_instance")
        logger.warning(
            "Task 재시도 예정: %s",
            task_instance.task_id if task_instance else "unknown",
        )

    def slack_failure_callback(context):
        task_instance = context.get("task_instance")
        logger.error(
            "Task 최종 실패: %s",
            task_instance.task_id if task_instance else "unknown",
        )

    slack_retry_alert_callback.is_fallback = True
    slack_failure_callback.is_fallback = True
else:
    slack_retry_alert_callback = send_slack_webhook_notification(
        slack_webhook_conn_id=SLACK_WEBHOOK_CONN_ID,
        text=SLACK_RETRY_ALERT_TEXT,
    )
    slack_failure_callback = send_slack_webhook_notification(
        slack_webhook_conn_id=SLACK_WEBHOOK_CONN_ID,
        text=SLACK_FAILURE_TEXT,
    )
