"""정제된 기사 차량 월별 스냅샷을 월 파티션 Silver Parquet 으로 적재합니다."""

import io
import logging
import re
from pathlib import Path, PurePosixPath
from urllib.parse import urlsplit

import pyarrow as pa
import pyarrow.parquet as pq
from pipeline_core.loader import Loader, WriteResult

from schema.silver import CLEAN_DRIVER_VEHICLE_MONTHLY_SNAPSHOT_SCHEMA as SCHEMA
from shared.aws_lambda.common.atomic_write import atomic_write
from shared.aws_lambda.common.s3_loader import S3Loader, S3Object
logger = logging.getLogger(__name__)
DATASET = "driver_vehicle_monthly_snapshot"
DATA_FILE_NAME = "data.parquet"
OUTPUT_VERSION_PATTERN = re.compile(
    r"^source_collected_at=\d{8}T\d{12}Z$"
)


def _validate_output_version(path: Path | PurePosixPath) -> None:
    if path.parent.name != ".staging" or not OUTPUT_VERSION_PATTERN.fullmatch(path.name):
        raise ValueError("silver_output_path가 Silver staging 버전 경로가 아닙니다")


class DriverVehicleMonthlySnapshotSilverLoader(Loader):
    """검증 전 Silver 버전 디렉터리에 단일 data 파일을 원자적으로 씁니다."""

    def __init__(
        self,
        output_dir: str,
    ):
        self._output_dir = Path(output_dir)
        _validate_output_version(self._output_dir)
        self.path: Path | None = None

    def write(self, data: pa.Table) -> WriteResult:
        if data.schema != SCHEMA:
            raise ValueError("적재할 기사 차량 스냅샷 데이터가 Silver 스키마와 다릅니다")
        path = self._output_dir / DATA_FILE_NAME
        path.parent.mkdir(parents=True, exist_ok=True)
        atomic_write(
            path,
            lambda temporary: pq.write_table(data, temporary, compression="snappy"),
        )
        self.path = path
        logger.info("silver_load done path=%s rows=%d", path, data.num_rows)
        return WriteResult(location=str(path), row_count=data.num_rows)


class DriverVehicleMonthlySnapshotS3SilverLoader(Loader):
    """검증 전 S3 Silver 버전 prefix에 단일 data 파일을 씁니다."""

    def __init__(
        self,
        output_dir: str,
        bucket: str | None = None,
    ):
        parsed = urlsplit(output_dir)
        if parsed.scheme != "s3" or not parsed.netloc:
            raise ValueError("silver_output_path가 S3 URI가 아닙니다")
        output_path = PurePosixPath(parsed.path.lstrip("/"))
        _validate_output_version(output_path)
        if bucket and bucket != parsed.netloc:
            raise ValueError("silver_output_path bucket이 입력 bucket과 다릅니다")
        self._bucket = parsed.netloc
        self._key = str(output_path / DATA_FILE_NAME)

    def write(self, data: pa.Table) -> WriteResult:
        if data.schema != SCHEMA:
            raise ValueError("적재할 기사 차량 스냅샷 데이터가 Silver 스키마와 다릅니다")
        buffer = io.BytesIO()
        pq.write_table(data, buffer, compression="snappy")

        result = S3Loader(
            key=self._key,
            bucket=self._bucket,
        ).write(
            S3Object(body=buffer.getvalue(), row_count=data.num_rows)
        )
        logger.info(
            "silver_load done location=%s rows=%d", result.location, data.num_rows
        )
        return result


def build_silver_loader(
    storage: str,
    output_dir: str,
    bucket: str | None,
) -> Loader:
    if storage == "local":
        return DriverVehicleMonthlySnapshotSilverLoader(output_dir)
    if storage == "s3":
        return DriverVehicleMonthlySnapshotS3SilverLoader(output_dir, bucket=bucket)
    raise ValueError(f"알 수 없는 storage: {storage!r} (local 또는 s3)")
