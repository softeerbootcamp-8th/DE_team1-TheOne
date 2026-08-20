"""Slack 재시도·최종 실패·Gold 성공 알림 콜백."""

import logging


logger = logging.getLogger(__name__)

SLACK_WEBHOOK_CONN_ID = "slack_webhook"
# 분모는 `task.retries` 가 아니라 `ti.max_tries` 입니다. 앞은 정적 설정값이고 뒤는
# clear·재실행마다 `try_number + retries` 로 누적되는 값이라, 정적 값을 쓰면 분자만
# 올라가 `2 / 1` 처럼 어긋납니다 (#550). Airflow 자신도 "Starting attempt X of Y" 를
# `ti.max_tries + 1` 로 찍고, 재시도 여부도 `try_number <= max_tries` 로 판정합니다.
#
# 사유를 한 줄 싣는 이유는 알림만 보고 1차 판단을 하기 위해서입니다. 이 저장소의
# 검증 실패는 대부분 한 줄이지만 Spark `Py4JJavaError` 는 수백 줄이라 잘라냅니다.
REASON_MAX_CHARS = 400
SLACK_RETRY_ALERT_TEXT = (
    ":warning: *Airflow Task Alert*\n"
    "*상태*: `재시도 예정`\n"
    "*DAG*: `{{ dag.dag_id }}`\n"
    "*Task*: `{{ ti.task_id }}`\n"
    "*Run*: `{{ run_id }}`\n"
    "*시도*: `{{ ti.try_number }} / {{ ti.max_tries + 1 }}`\n"
    f"*사유*: `{{{{ (exception or '(사유 없음)') | string | truncate({REASON_MAX_CHARS}, True) }}}}`\n"
    "*로그*: <{{ ti.log_url }}|Airflow 로그 열기>"
)

SLACK_FAILURE_TEXT = (
    ":red_circle: *Airflow Task Fail*\n"
    "*상태*: `최종 실패`\n"
    "*DAG*: `{{ dag.dag_id }}`\n"
    "*Task*: `{{ ti.task_id }}`\n"
    "*Run*: `{{ run_id }}`\n"
    "*시도*: `{{ ti.try_number }} / {{ ti.max_tries + 1 }}`\n"
    f"*사유*: `{{{{ (exception or '(사유 없음)') | string | truncate({REASON_MAX_CHARS}, True) }}}}`\n"
    "*로그*: <{{ ti.log_url }}|Airflow 로그 열기>"
)

SLACK_SUCCESS_TEXT = (
    ":white_check_mark: *Airflow Gold Success*\n"
    "*상태*: `Gold 생성 완료`\n"
    "*DAG*: `{{ dag.dag_id }}`\n"
    "*Task*: `{{ ti.task_id }}`\n"
    "*Run*: `{{ run_id }}`\n"
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

    def slack_success_callback(context):
        task_instance = context.get("task_instance")
        logger.info(
            "Task 성공: %s",
            task_instance.task_id if task_instance else "unknown",
        )

    slack_retry_alert_callback.is_fallback = True
    slack_failure_callback.is_fallback = True
    slack_success_callback.is_fallback = True
else:
    slack_retry_alert_callback = send_slack_webhook_notification(
        slack_webhook_conn_id=SLACK_WEBHOOK_CONN_ID,
        text=SLACK_RETRY_ALERT_TEXT,
    )
    slack_failure_callback = send_slack_webhook_notification(
        slack_webhook_conn_id=SLACK_WEBHOOK_CONN_ID,
        text=SLACK_FAILURE_TEXT,
    )
    slack_success_callback = send_slack_webhook_notification(
        slack_webhook_conn_id=SLACK_WEBHOOK_CONN_ID,
        text=SLACK_SUCCESS_TEXT,
    )
