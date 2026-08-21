"""정제된 보유 차량을 월 파티션 Silver Parquet 으로 적재합니다."""

import io
import logging
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pipeline_core.loader import Loader, WriteResult

from schema.silver import CLEAN_LEASE_VEHICLE_INVENTORY_SCHEMA as SCHEMA
from shared.aws_lambda.common.atomic_write import atomic_write
from shared.aws_lambda.common.s3_loader import S3Loader, S3Object
from main.aws_lambda.common.monthly_dataset import TIMESTAMP_FILE_PATTERN


logger = logging.getLogger(__name__)
DATASET = "lease_vehicle_inventory"


def silver_key(year_month: str, file_name: str) -> str:
    return f"silver/{DATASET}/year_month={year_month}/{file_name}"


class LeaseVehicleInventorySilverLoader(Loader):
    """월 파티션의 수집 버전 파일을 원자적으로 교체합니다.

    새 수집 시각은 새 파일로 보존하고 같은 수집본 재시도는 동일 파일만 교체합니다.
    """

    def __init__(
        self,
        base_dir: str,
        year_month: str,
        file_name: str,
        *,
        dry_run: bool = False,
    ):
        if not TIMESTAMP_FILE_PATTERN.fullmatch(file_name):
            raise ValueError("silver_file_name이 수집 시각 Parquet 형식이 아닙니다")
        self._base_dir = Path(base_dir)
        self._year_month = year_month
        self._file_name = file_name
        self._dry_run = dry_run
        self.path: Path | None = None

    def write(self, data: pa.Table) -> WriteResult:
        if data.schema != SCHEMA:
            raise ValueError("적재할 보유 차량 데이터가 Silver 스키마와 다릅니다")
        path = (
            self._base_dir
            / f"year_month={self._year_month}"
            / self._file_name
        )
        if self._dry_run:
            self.path = path
            logger.info("dry_run: Silver 적재 생략 path=%s rows=%d", path, data.num_rows)
            return WriteResult(location=str(path), row_count=data.num_rows)
        path.parent.mkdir(parents=True, exist_ok=True)
        atomic_write(
            path,
            lambda temporary: pq.write_table(data, temporary, compression="snappy"),
        )
        self.path = path
        logger.info("silver_load done path=%s rows=%d", path, data.num_rows)
        return WriteResult(location=str(path), row_count=data.num_rows)


class LeaseVehicleInventoryS3SilverLoader(Loader):
    def __init__(
        self,
        year_month: str,
        file_name: str,
        bucket: str | None = None,
        *,
        dry_run: bool = False,
    ):
        if not TIMESTAMP_FILE_PATTERN.fullmatch(file_name):
            raise ValueError("silver_file_name이 수집 시각 Parquet 형식이 아닙니다")
        self._year_month = year_month
        self._file_name = file_name
        self._bucket = bucket
        self._dry_run = dry_run

    def write(self, data: pa.Table) -> WriteResult:
        if data.schema != SCHEMA:
            raise ValueError("적재할 보유 차량 데이터가 Silver 스키마와 다릅니다")
        buffer = io.BytesIO()
        pq.write_table(data, buffer, compression="snappy")
        result = S3Loader(
            key=silver_key(self._year_month, self._file_name),
            bucket=self._bucket,
            dry_run=self._dry_run,
        ).write(S3Object(body=buffer.getvalue(), row_count=data.num_rows))
        logger.info("silver_load done location=%s rows=%d", result.location, data.num_rows)
        return result


def build_silver_loader(
    storage: str,
    base_dir: str,
    bucket: str | None,
    year_month: str,
    file_name: str,
    *,
    dry_run: bool = False,
) -> Loader:
    if storage == "local":
        return LeaseVehicleInventorySilverLoader(
            base_dir,
            year_month,
            file_name,
            dry_run=dry_run,
        )
    if storage == "s3":
        return LeaseVehicleInventoryS3SilverLoader(
            year_month,
            file_name,
            bucket=bucket,
            dry_run=dry_run,
        )
    raise ValueError(f"알 수 없는 storage: {storage!r} (local 또는 s3)")
