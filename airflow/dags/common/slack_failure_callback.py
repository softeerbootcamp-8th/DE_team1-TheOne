"""Slack 실패 알림 콜백."""

from airflow.providers.slack.notifications.slack_webhook import (
    send_slack_webhook_notification,
)

slack_failure_callback = send_slack_webhook_notification(
    slack_webhook_conn_id="slack_webhook",
    text=(
        ":red_circle: *Airflow Task 실패*\n"
        "*DAG*: `{{ dag.dag_id }}`\n"
        "*Task*: `{{ ti.task_id }}`\n"
        "*Run*: `{{ run_id }}`\n"
        "*로그*: <{{ ti.log_url }}|Airflow 로그 열기>"
    ),
)
