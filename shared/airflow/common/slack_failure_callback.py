"""Slack 재시도·최종 실패·Gold 성공/skip/staleness 알림 콜백."""

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
RUN_TYPE_TEXT = (
    "{% if run_id.startswith('manual__') %}수동 실행"
    "{% elif run_id.startswith('asset_triggered__') %}Asset 트리거"
    "{% elif run_id.startswith('scheduled__') %}정기 실행"
    "{% else %}기타 실행{% endif %}"
)
REASON_TEXT = (
    f"{{{{ (exception or '(사유 없음)') | string | truncate({REASON_MAX_CHARS}, True) }}}}"
)
# 파티션 DAG(Gold)는 이 값으로 어느 파티션이 문제인지 바로 알 수 있습니다. 지역
# 축이 들어가면(#674) 키가 "{service_area}:{year_month}"가 되어 알림만 보고 어느
# 지역이 죽었는지 구분됩니다 — 지역마다 DAG를 새로 만들지 않는 설계라 이 정보가
# 없으면 온콜이 지역을 못 가립니다.
#
# 비파티션 DAG에서는 컨텍스트에 키가 아예 없거나 None입니다
# (`airflow.sdk.definitions.context`: `partition_key: NotRequired[str | None]`).
# `default(..., true)`가 두 경우를 모두 '-'로 처리합니다.
PARTITION_KEY_TEXT = "{{ partition_key | default('-', true) }}"
SLACK_RETRY_ALERT_TEXT = (
    "⏳ *Airflow 태스크 재시도 예정*\n"
    "*DAG*: `{{ dag.dag_id }}`\n"
    "*Task*: `{{ ti.task_id }}`\n"
    f"*파티션*: `{PARTITION_KEY_TEXT}`\n"
    f"*실행 유형*: `{RUN_TYPE_TEXT}`\n"
    "*시도*: `{{ ti.try_number }} / {{ ti.max_tries + 1 }}`\n"
    f"*원인*: `{REASON_TEXT}`\n"
    "*Run ID*: `{{ run_id | truncate(80, True) }}`\n"
    "*로그*: <{{ ti.log_url }}|Airflow 로그 열기>"
)

SLACK_FAILURE_TEXT = (
    "🚨 *Airflow 파이프라인 최종 실패*\n"
    "*DAG*: `{{ dag.dag_id }}`\n"
    "*Task*: `{{ ti.task_id }}`\n"
    f"*파티션*: `{PARTITION_KEY_TEXT}`\n"
    f"*실행 유형*: `{RUN_TYPE_TEXT}`\n"
    "*시도*: `{{ ti.try_number }} / {{ ti.max_tries + 1 }}`\n"
    f"*원인*: `{REASON_TEXT}`\n"
    "*Run ID*: `{{ run_id | truncate(80, True) }}`\n"
    "*로그*: <{{ ti.log_url }}|Airflow 로그 열기>"
)

SLACK_SUCCESS_TEXT = (
    "✅ *Gold 생성 완료*\n"
    "*대상 연월*: `{{ (ti.xcom_pull(task_ids='validate_inputs') or {}).get('year_month', '확인 필요') }}`\n"
    f"*파티션*: `{PARTITION_KEY_TEXT}`\n"
    f"*실행 유형*: `{RUN_TYPE_TEXT}`\n"
    "*파이프라인*: `{{ dag.dag_id }}`\n"
    "*Run ID*: `{{ run_id | truncate(80, True) }}`\n"
    "*로그*: <{{ ti.log_url }}|Airflow 로그 열기>"
)

SLACK_SKIP_TEXT = (
    "⚠️ *Gold 파이프라인 입력 대기 (skip)*\n"
    "*DAG*: `{{ dag.dag_id }}`\n"
    "*Task*: `{{ ti.task_id }}`\n"
    f"*파티션*: `{PARTITION_KEY_TEXT}`\n"
    f"*실행 유형*: `{RUN_TYPE_TEXT}`\n"
    f"*원인*: `{REASON_TEXT}`\n"
    "*Run ID*: `{{ run_id | truncate(80, True) }}`\n"
    "*로그*: <{{ ti.log_url }}|Airflow 로그 열기>"
)

SLACK_STALE_TEXT = (
    "⏰ *Gold 파이프라인 staleness 경고*\n"
    "*DAG*: `{{ dag.dag_id }}`\n"
    f"*파티션*: `{PARTITION_KEY_TEXT}`\n"
    f"*실행 유형*: `{RUN_TYPE_TEXT}`\n"
    "*마지막 성공 이후*: `{{ days_since_success }}일` (SLA `{{ stale_days }}일`)\n"
    "*Run ID*: `{{ run_id | truncate(80, True) }}`\n"
    "*로그*: <{{ ti.log_url }}|Airflow 로그 열기>"
)


def _failure_blocks(title: str, next_action: str) -> list[dict]:
    return [
        {"type": "header", "text": {"type": "plain_text", "text": title}},
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*원인*\n`{REASON_TEXT}`"},
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*실행 유형*\n`{RUN_TYPE_TEXT}`"},
                {
                    "type": "mrkdwn",
                    "text": "*시도*\n`{{ ti.try_number }} / {{ ti.max_tries + 1 }}`",
                },
                {"type": "mrkdwn", "text": f"*다음 조치*\n`{next_action}`"},
            ],
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": "*DAG*\n`{{ dag.dag_id }}`"},
                {"type": "mrkdwn", "text": "*Task*\n`{{ ti.task_id }}`"},
                {"type": "mrkdwn", "text": f"*파티션*\n`{PARTITION_KEY_TEXT}`"},
            ],
        },
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": "Run ID · `{{ run_id | truncate(80, True) }}`",
                }
            ],
        },
        {
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "Airflow 로그 열기"},
                    "url": "{{ ti.log_url }}",
                }
            ],
        },
    ]


SLACK_RETRY_ALERT_BLOCKS = _failure_blocks(
    "⏳ 태스크 재시도 예정", "Airflow 자동 재시도 대기"
)
SLACK_FAILURE_BLOCKS = _failure_blocks(
    "🚨 파이프라인 최종 실패", "입력·파라미터 확인 후 재실행"
)
SLACK_SUCCESS_BLOCKS = [
    {"type": "header", "text": {"type": "plain_text", "text": "✅ Gold 생성 완료"}},
    {
        "type": "section",
        "fields": [
            {
                "type": "mrkdwn",
                "text": "*대상 연월*\n`{{ (ti.xcom_pull(task_ids='validate_inputs') or {}).get('year_month', '확인 필요') }}`",
            },
            {"type": "mrkdwn", "text": f"*파티션*\n`{PARTITION_KEY_TEXT}`"},
            {"type": "mrkdwn", "text": f"*실행 유형*\n`{RUN_TYPE_TEXT}`"},
            {"type": "mrkdwn", "text": "*파이프라인*\n`{{ dag.dag_id }}`"},
            {"type": "mrkdwn", "text": "*처리 결과*\n`월간 Gold 검증 완료`"},
        ],
    },
    {
        "type": "context",
        "elements": [
            {
                "type": "mrkdwn",
                "text": "Run ID · `{{ run_id | truncate(80, True) }}`",
            }
        ],
    },
    {
        "type": "actions",
        "elements": [
            {
                "type": "button",
                "text": {"type": "plain_text", "text": "Airflow 로그 열기"},
                "url": "{{ ti.log_url }}",
            }
        ],
    },
]
SLACK_SKIP_BLOCKS = _failure_blocks(
    "⚠️ Gold 파이프라인 입력 대기 (skip)", "Silver 소스 적재 확인, 준비되면 자동 재트리거"
)
SLACK_STALE_BLOCKS = [
    {
        "type": "header",
        "text": {"type": "plain_text", "text": "⏰ Gold 파이프라인 staleness 경고"},
    },
    {
        "type": "section",
        "fields": [
            {
                "type": "mrkdwn",
                "text": "*마지막 성공 이후*\n`{{ days_since_success }}일`",
            },
            {"type": "mrkdwn", "text": "*SLA 기준*\n`{{ stale_days }}일`"},
            {"type": "mrkdwn", "text": f"*파티션*\n`{PARTITION_KEY_TEXT}`"},
            {"type": "mrkdwn", "text": f"*실행 유형*\n`{RUN_TYPE_TEXT}`"},
            {"type": "mrkdwn", "text": "*DAG*\n`{{ dag.dag_id }}`"},
        ],
    },
    {
        "type": "context",
        "elements": [
            {
                "type": "mrkdwn",
                "text": "Run ID · `{{ run_id | truncate(80, True) }}`",
            }
        ],
    },
    {
        "type": "actions",
        "elements": [
            {
                "type": "button",
                "text": {"type": "plain_text", "text": "Airflow 로그 열기"},
                "url": "{{ ti.log_url }}",
            }
        ],
    },
]

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

    def slack_skip_alert_callback(context):
        task_instance = context.get("task_instance")
        logger.warning(
            "Task skip: %s",
            task_instance.task_id if task_instance else "unknown",
        )

    def slack_stale_alert_callback(context):
        logger.warning(
            "Gold staleness 경고: 마지막 성공 이후 %s일 (SLA %s일)",
            context.get("days_since_success"),
            context.get("stale_days"),
        )

    slack_retry_alert_callback.is_fallback = True
    slack_failure_callback.is_fallback = True
    slack_success_callback.is_fallback = True
    slack_skip_alert_callback.is_fallback = True
    slack_stale_alert_callback.is_fallback = True
else:
    slack_retry_alert_callback = send_slack_webhook_notification(
        slack_webhook_conn_id=SLACK_WEBHOOK_CONN_ID,
        text=SLACK_RETRY_ALERT_TEXT,
        blocks=SLACK_RETRY_ALERT_BLOCKS,
    )
    slack_failure_callback = send_slack_webhook_notification(
        slack_webhook_conn_id=SLACK_WEBHOOK_CONN_ID,
        text=SLACK_FAILURE_TEXT,
        blocks=SLACK_FAILURE_BLOCKS,
    )
    slack_success_callback = send_slack_webhook_notification(
        slack_webhook_conn_id=SLACK_WEBHOOK_CONN_ID,
        text=SLACK_SUCCESS_TEXT,
        blocks=SLACK_SUCCESS_BLOCKS,
    )
    slack_skip_alert_callback = send_slack_webhook_notification(
        slack_webhook_conn_id=SLACK_WEBHOOK_CONN_ID,
        text=SLACK_SKIP_TEXT,
        blocks=SLACK_SKIP_BLOCKS,
    )
    slack_stale_alert_callback = send_slack_webhook_notification(
        slack_webhook_conn_id=SLACK_WEBHOOK_CONN_ID,
        text=SLACK_STALE_TEXT,
        blocks=SLACK_STALE_BLOCKS,
    )
