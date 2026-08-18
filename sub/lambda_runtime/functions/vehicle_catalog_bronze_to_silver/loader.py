"""정제된 리스 업체 보유 차량 대장을 Silver Parquet 으로 적재합니다."""

import io
import logging
import os

import pyarrow as pa
import pyarrow.parquet as pq
from pipeline_core.loader import Loader, WriteResult

from schema.silver.vehicle_catalog import SCHEMA

from shared.lambda_runtime.common.atomic_write import atomic_write
from shared.lambda_runtime.common import vehicle_catalog_layout as layout
from shared.lambda_runtime.common.env import load_local_env
from shared.lambda_runtime.common.s3_loader import BUCKET_ENV_VAR, S3Loader, S3Object

logger = logging.getLogger(__name__)


def _group_by_vendor(data: list[dict]) -> dict[str, list[dict]]:
    if not data:
        raise ValueError("적재할 차량 대장 Silver 데이터가 없습니다.")
    by_vendor: dict[str, list[dict]] = {}
    for row in data:
        by_vendor.setdefault(row[layout.VENDOR_PARTITION_KEY], []).append(row)
    return by_vendor


def _ensure_collected_date_matches(vendor_rows: list[dict], expect_collected_date: str | None):
    collected_date = vendor_rows[0]["collected_at"].date()
    if expect_collected_date and collected_date.isoformat() != expect_collected_date:
        raise ValueError(
            f"요청한 수집일과 변환된 수집일이 다릅니다: "
            f"{expect_collected_date} != {collected_date.isoformat()}"
        )
    return collected_date


class VehicleCatalogSilverLoader(Loader):
    """업체별로 Parquet 하나씩 씁니다. 같은 파티션은 덮어씁니다.

    Bronze 와 달리 Silver 는 재실행하면 덮어씁니다. 같은 수집일을 다시 변환한
    결과가 여러 개 남으면 읽는 쪽에서 무엇이 맞는지 알 수 없기 때문입니다.
    """

    def __init__(self, base_dir: str, expect_collected_date: str | None = None):
        self._base_dir = base_dir
        self._expect_collected_date = expect_collected_date
        # 이번 실행이 쓴 업체별 경로. Pipeline 이 중간 데이터를 감추므로
        # 핸들러가 반환값을 만들 때 여기서 읽습니다.
        self.paths: list[str] = []

    def write(self, data: list[dict]) -> WriteResult:
        by_vendor = _group_by_vendor(data)

        written_rows = 0
        for vendor, vendor_rows in sorted(by_vendor.items()):
            collected_date = _ensure_collected_date_matches(
                vendor_rows, self._expect_collected_date
            )

            path = layout.silver_file(self._base_dir, collected_date, vendor)
            path.parent.mkdir(parents=True, exist_ok=True)

            table = pa.Table.from_pylist(vendor_rows, schema=SCHEMA)
            atomic_write(
                path,
                lambda temporary: pq.write_table(
                    table, temporary, compression="snappy"
                ),
            )

            logger.info(
                "silver_load done path=%s vendor=%s rows=%d",
                path,
                vendor,
                table.num_rows,
            )
            self.paths.append(str(path))
            written_rows += table.num_rows

        return WriteResult(
            location=str(layout.dataset_path(self._base_dir)),
            row_count=written_rows,
        )


class VehicleCatalogS3SilverLoader(Loader):
    """업체별 Silver Parquet 하나씩 S3에 씁니다. 같은 key는 재실행 시 덮어씁니다."""

    def __init__(self, expect_collected_date: str | None = None, bucket: str | None = None):
        load_local_env()
        self._bucket = bucket or os.environ[BUCKET_ENV_VAR]
        self._expect_collected_date = expect_collected_date
        self.paths: list[str] = []

    def write(self, data: list[dict]) -> WriteResult:
        by_vendor = _group_by_vendor(data)

        written_rows = 0
        for vendor, vendor_rows in sorted(by_vendor.items()):
            collected_date = _ensure_collected_date_matches(
                vendor_rows, self._expect_collected_date
            )

            key = layout.silver_key(collected_date, vendor)
            table = pa.Table.from_pylist(vendor_rows, schema=SCHEMA)
            buffer = io.BytesIO()
            pq.write_table(table, buffer, compression="snappy")

            result = S3Loader(key=key, bucket=self._bucket).write(
                S3Object(body=buffer.getvalue(), row_count=table.num_rows)
            )
            logger.info(
                "silver_load done location=%s vendor=%s rows=%d",
                result.location,
                vendor,
                table.num_rows,
            )
            self.paths.append(result.location)
            written_rows += table.num_rows

        return WriteResult(
            location=f"s3://{self._bucket}/silver/{layout.DATASET}/",
            row_count=written_rows,
        )


def build_silver_loader(
    storage: str, base_dir: str, collected_date: str, bucket: str | None = None
) -> Loader:
    """storage 파라미터로 로컬/S3 Loader 중 하나를 고릅니다."""
    if storage == "local":
        return VehicleCatalogSilverLoader(base_dir, expect_collected_date=collected_date)
    if storage == "s3":
        return VehicleCatalogS3SilverLoader(expect_collected_date=collected_date, bucket=bucket)
    raise ValueError(f"알 수 없는 storage: {storage!r} (local 또는 s3)")
