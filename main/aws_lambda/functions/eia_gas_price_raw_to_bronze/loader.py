"""EIA 원본 파일을 월·수집 시각 버전에 그대로 적재합니다."""

import logging
from pipeline_core.loader import Loader, WriteResult

from main.aws_lambda.common import eia_fuel_price_layout as layout
from shared.aws_lambda.common.atomic_write import atomic_write, invalidate_success_marker
from shared.aws_lambda.common.s3_loader import S3Loader, S3Object

logger = logging.getLogger(__name__)


class EiaGasPriceBronzeLoader(Loader):
    """받은 bytes 를 변형 없이 로컬에 씁니다."""

    def __init__(
        self,
        base_dir: str,
        collected_at: str,
        service_area: str,
    ):
        self._base_dir = base_dir
        self._collected_at = collected_at
        self._service_area = service_area

    def write(self, data: dict) -> WriteResult:
        body = data["body"]
        path = layout.gas_bronze_file(
            self._base_dir, self._collected_at, self._service_area
        )
        duplicate = layout.is_duplicate_of_newest(
            self._base_dir,
            layout.GAS_DATASET,
            layout.GAS_FILE_NAME,
            body,
            self._service_area,
        )
        # 내용이 최신 수집분과 같으면 새 파티션을 만들지 않습니다. 그러면 파티션 개수
        # 자체가 "언제 실제로 바뀌었는지" 를 말해주는 기록이 됩니다.
        if duplicate is not None:
            logger.info("최신 수집분과 동일해 건너뜁니다: %s (%d bytes)", duplicate, len(body))
            return WriteResult(location=str(duplicate), row_count=1)

        path.parent.mkdir(parents=True, exist_ok=True)
        invalidate_success_marker(path.parent)
        atomic_write(path, lambda temporary: temporary.write_bytes(body))

        logger.info("적재 완료: %s (%d bytes)", path, len(body))
        # 이력 파일이라 "행 수"가 대상 월과 무관합니다. 파일 1건으로 셉니다.
        return WriteResult(location=str(path), row_count=1)


class EiaGasPriceS3BronzeLoader(Loader):
    """받은 bytes 를 변형 없이 S3 에 씁니다."""

    def __init__(
        self,
        collected_at: str,
        service_area: str,
        bucket: str | None = None,
    ):
        self._collected_at = collected_at
        self._bucket = bucket
        self._service_area = service_area

    def write(self, data: dict) -> WriteResult:
        body = data["body"]
        key = layout.gas_bronze_key(self._collected_at, self._service_area)

        result = S3Loader(
            key=key,
            bucket=self._bucket,
            invalidate_parent_success=True,
        ).write(
            S3Object(body=body, row_count=1)
        )
        logger.info("적재 완료: %s (%d bytes)", result.location, len(body))
        return result


def build_bronze_loader(
    storage: str,
    base_dir: str,
    collected_at: str,
    service_area: str,
    bucket: str | None = None,
) -> Loader:
    if storage == "local":
        return EiaGasPriceBronzeLoader(base_dir, collected_at, service_area)
    if storage == "s3":
        return EiaGasPriceS3BronzeLoader(
            collected_at,
            service_area,
            bucket=bucket,
        )
    raise ValueError(f"알 수 없는 storage: {storage!r} (local 또는 s3)")
