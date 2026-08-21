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


logger = logging.getLogger(__name__)
DATASET = "lease_vehicle_inventory"


def silver_key(year_month: str) -> str:
    return f"silver/{DATASET}/year_month={year_month}/{DATASET}.parquet"


class LeaseVehicleInventorySilverLoader(Loader):
    """월 파티션에 파일 하나를 원자적으로 교체합니다.

    Bronze 와 달리 Silver 는 재실행하면 덮어씁니다. 같은 달의 변환 결과가 여러 개
    남으면 읽는 쪽이 무엇이 맞는지 알 수 없기 때문입니다. 임시 파일을 완성한 뒤
    rename 하므로, 쓰다가 죽어도 직전 달치가 반쯤 덮인 상태로 남지 않습니다.
    """

    def __init__(self, base_dir: str, year_month: str):
        self._base_dir = Path(base_dir)
        self._year_month = year_month
        self.path: Path | None = None

    def write(self, data: pa.Table) -> WriteResult:
        if data.schema != SCHEMA:
            raise ValueError("적재할 보유 차량 데이터가 Silver 스키마와 다릅니다")
        path = (
            self._base_dir / f"year_month={self._year_month}" / f"{DATASET}.parquet"
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        atomic_write(
            path,
            lambda temporary: pq.write_table(data, temporary, compression="snappy"),
        )
        self.path = path
        logger.info("silver_load done path=%s rows=%d", path, data.num_rows)
        return WriteResult(location=str(path), row_count=data.num_rows)


class LeaseVehicleInventoryS3SilverLoader(Loader):
    """월 파티션 파일 하나를 S3 에 통째로 교체합니다.

    S3 PutObject 는 객체 단위로 원자적이라 로컬처럼 임시 파일을 거쳐 rename 할
    필요가 없습니다.
    """

    def __init__(self, year_month: str, bucket: str | None = None):
        self._year_month = year_month
        self._bucket = bucket

    def write(self, data: pa.Table) -> WriteResult:
        if data.schema != SCHEMA:
            raise ValueError("적재할 보유 차량 데이터가 Silver 스키마와 다릅니다")
        buffer = io.BytesIO()
        pq.write_table(data, buffer, compression="snappy")

        result = S3Loader(key=silver_key(self._year_month), bucket=self._bucket).write(
            S3Object(body=buffer.getvalue(), row_count=data.num_rows)
        )
        logger.info(
            "silver_load done location=%s rows=%d", result.location, data.num_rows
        )
        return result


def build_silver_loader(
    storage: str, base_dir: str, bucket: str | None, year_month: str
) -> Loader:
    if storage == "local":
        return LeaseVehicleInventorySilverLoader(base_dir, year_month)
    if storage == "s3":
        return LeaseVehicleInventoryS3SilverLoader(year_month, bucket=bucket)
    raise ValueError(f"알 수 없는 storage: {storage!r} (local 또는 s3)")
