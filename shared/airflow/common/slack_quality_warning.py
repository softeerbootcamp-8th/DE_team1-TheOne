"""Silver 진행을 막지 않는 데이터 품질 경고 Slack 메시지."""

import logging

from airflow.providers.slack.notifications.slack_webhook import (
    send_slack_webhook_notification,
)

from shared.airflow.common.slack_failure_callback import SLACK_WEBHOOK_CONN_ID


logger = logging.getLogger(__name__)


def build_quality_warning(
    *,
    dataset: str,
    year_month: str,
    invalid_rows: int,
    row_count: int,
    invalid_ratio: float,
    extra_columns: list[str],
) -> str:
    """운영자가 경고만 보고 판정 근거와 처리 결과를 확인하게 합니다."""
    invalid = f"{invalid_rows:,} / {row_count:,}"
    ratio = f"{invalid_ratio:.1%}"
    extras = ", ".join(extra_columns) or "없음"
    return (
        "⚠️ *데이터 품질 경고*\n"
        f"*데이터셋*: `{dataset}`\n"
        f"*대상 연월*: `{year_month}`\n"
        f"*불량 레코드*: `{invalid}`\n"
        f"*불량률*: `{ratio}`\n"
        f"*추가 컬럼*: `{extras}`\n"
        "*처리 결과*: `Silver 진행`\n"
        "*로그*: <{{ ti.log_url }}|Airflow 로그 열기>"
    )


def send_quality_warning(context: dict, **values) -> None:
    """경고 전송 장애가 검증 성공을 하드 실패로 바꾸지 않게 격리합니다."""
    text = build_quality_warning(**values)
    try:
        send_slack_webhook_notification(
            slack_webhook_conn_id=SLACK_WEBHOOK_CONN_ID,
            text=text,
        )(context)
    except Exception:
        logger.exception("품질 경고 Slack 전송 실패: %s", text)
