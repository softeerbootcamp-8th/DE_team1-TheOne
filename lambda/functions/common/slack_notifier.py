"""Slack 알림 전송 공용 모듈 (클래스 기반)."""

import logging
import os
import requests

logger = logging.getLogger(__name__)


class SlackNotifier:
    """Slack Incoming Webhook을 통한 알림 발송을 담당하는 공용 클래스."""

    def __init__(self, webhook_url: str | None = None, timeout: int = 5):
        self._webhook_url = webhook_url or os.getenv("SLACK_WEBHOOK_URL")
        self._timeout = timeout

    @property
    def is_enabled(self) -> bool:
        """Webhook URL이 설정되어 있어 알림 발송이 가능한지 여부."""
        return bool(self._webhook_url)

    def send_schema_drift_alert(
        self, dataset: str, identifier: str, diffs: list[str]
    ) -> bool:
        """스키마 변동(Schema Drift) 감지 시 Slack Alert를 발송합니다.

        Args:
            dataset: 데이터셋 명 (예: "hvfhv")
            identifier: 데이터 식별 정보 (예: "2026-08")
            diffs: 검증 결과 발견된 스키마 변동 내역 리스트

        Returns:
            알림 전송 성공 여부 (bool)
        """
        if not diffs:
            return False

        if not self.is_enabled:
            logger.warning(
                "SLACK_WEBHOOK_URL 환경 변수가 설정되지 않아 스키마 변경 Slack 알림을 건너뜁니다."
            )
            return False

        diff_details = "\n".join(f"  - {d}" for d in diffs)
        text = (
            f":warning: *[{dataset.upper()}] Schema Drift 감지 알림*\n"
            f"*식별자 / 파티션*: `{identifier}`\n"
            f"*감지된 스키마 변동사항 ({len(diffs)}건)*:\n"
            f"{diff_details}\n\n"
            f"_참고: Raw Data는 Bronze 파티션에 정상 적재되었습니다._"
        )

        try:
            response = requests.post(
                self._webhook_url, json={"text": text}, timeout=self._timeout
            )
            response.raise_for_status()
            logger.info("Slack 스키마 Alert 알림 전송 성공: dataset=%s", dataset)
            return True
        except Exception as exc:
            logger.error("Slack 스키마 Alert 알림 전송 실패: %s", exc)
            return False
