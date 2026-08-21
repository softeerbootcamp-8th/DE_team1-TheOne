"""EIA 원본 파일을 수집일 파티션에 그대로 적재합니다."""

import logging
import os
from datetime import date

from pipeline_core.loader import Loader, WriteResult

from main.aws_lambda.common import eia_fuel_price_layout as layout
from shared.aws_lambda.common.atomic_write import atomic_write
from shared.aws_lambda.common.s3_loader import BUCKET_ENV_VAR, S3Loader, S3Object
from shared.common.env import load_local_env

logger = logging.getLogger(__name__)


class EiaElectricityPriceBronzeLoader(Loader):
    """받은 bytes 를 변형 없이 로컬에 씁니다."""

    def __init__(
        self,
        base_dir: str,
        collected_date: date,
        *,
        dry_run: bool = False,
    ):
        self._base_dir = base_dir
        self._collected_date = collected_date
        self._dry_run = dry_run
        self.byte_count = 0

    def write(self, data: dict) -> WriteResult:
        body = data["body"]
        self.byte_count = len(body)
        path = layout.electricity_bronze_file(self._base_dir, self._collected_date)
        duplicate = layout.is_duplicate_of_newest(
            self._base_dir,
            layout.ELECTRICITY_DATASET,
            layout.ELECTRICITY_FILE_NAME,
            body,
        )
        if self._dry_run:
            if duplicate is None:
                raise ValueError(
                    "dry_run 원본이 기존 EIA 전력 Bronze와 다릅니다. "
                    "변경 원본은 정상 실행으로 확인하세요."
                )
            logger.info("dry-run 적재 생략: %s (%d bytes)", path, len(body))
            return WriteResult(location=str(path), row_count=1)

        # 전력은 3개월에 한 번만 실제로 갱신되므로 월 1회 수집분 대부분이 바이트까지
        # 같습니다. 같은 것을 새 파티션으로 쌓지 않습니다.
        if duplicate is not None:
            logger.info("최신 수집분과 동일해 건너뜁니다: %s (%d bytes)", duplicate, len(body))
            return WriteResult(location=str(duplicate), row_count=1)

        path.parent.mkdir(parents=True, exist_ok=True)
        atomic_write(path, lambda temporary: temporary.write_bytes(body))

        logger.info("적재 완료: %s (%d bytes)", path, len(body))
        # 이력 파일이라 "행 수"가 대상 월과 무관합니다. 파일 1건으로 셉니다.
        return WriteResult(location=str(path), row_count=1)


class EiaElectricityPriceS3BronzeLoader(Loader):
    """받은 bytes 를 변형 없이 S3 에 씁니다."""

    def __init__(
        self,
        collected_date: date,
        bucket: str | None = None,
        *,
        dry_run: bool = False,
    ):
        load_local_env()
        self._collected_date = collected_date
        self._bucket = bucket or os.environ[BUCKET_ENV_VAR]
        self._dry_run = dry_run
        self.byte_count = 0

    def write(self, data: dict) -> WriteResult:
        body = data["body"]
        self.byte_count = len(body)
        key = layout.electricity_bronze_key(self._collected_date)

        if self._dry_run and layout.is_duplicate_of_newest_s3(
            self._bucket,
            layout.ELECTRICITY_DATASET,
            layout.ELECTRICITY_FILE_NAME,
            body,
        ) is None:
            raise ValueError(
                "dry_run 원본이 기존 EIA 전력 Bronze와 다릅니다. "
                "변경 원본은 정상 실행으로 확인하세요."
            )

        result = S3Loader(
            key=key,
            bucket=self._bucket,
            dry_run=self._dry_run,
        ).write(
            S3Object(body=body, row_count=1)
        )
        logger.info(
            "%s: %s (%d bytes)",
            "dry-run 적재 생략" if self._dry_run else "적재 완료",
            result.location,
            len(body),
        )
        return result


def build_bronze_loader(
    storage: str,
    base_dir: str,
    collected_date: date,
    bucket: str | None = None,
    *,
    dry_run: bool = False,
) -> Loader:
    if storage == "local":
        return EiaElectricityPriceBronzeLoader(
            base_dir,
            collected_date,
            dry_run=dry_run,
        )
    if storage == "s3":
        return EiaElectricityPriceS3BronzeLoader(
            collected_date,
            bucket=bucket,
            dry_run=dry_run,
        )
    raise ValueError(f"알 수 없는 storage: {storage!r} (local 또는 s3)")
