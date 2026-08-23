"""실행일의 리스 업체 보유 차량 대장 Raw 스냅샷을 읽습니다."""

import io
import logging
import os
import re
from datetime import date

import pyarrow as pa
import pyarrow.parquet as pq
from pipeline_core.extractor import Extractor

from sub.aws_lambda.common import vehicle_catalog_layout as layout
from shared.common.env import load_local_env
from shared.aws_lambda.common.s3_loader import BUCKET_ENV_VAR
from shared.common.s3_reader import get_object_bytes, list_keys

logger = logging.getLogger(__name__)

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _validate_collected_date(collected_date: str) -> None:
    if not DATE_RE.fullmatch(collected_date):
        raise ValueError("collected_date는 YYYY-MM-DD 형식이어야 합니다.")
    try:
        date.fromisoformat(collected_date)
    except ValueError as exc:
        raise ValueError("유효하지 않은 collected_date입니다.") from exc


class VehicleCatalogRawExtractor(Extractor):
    """해당 collected_date 파티션에서 업체별 가장 최신 Parquet 을 읽어 합칩니다.

    Raw 는 collected_date 아래에 vendor 파티션이 한 단계 더 있습니다.
    같은 날 여러 번 수집하면 파일이 쌓이므로 업체별로 최신 것만 씁니다.
    """

    name = "vehicle_catalog_raw"

    def __init__(self, base_dir: str, collected_date: str):
        _validate_collected_date(collected_date)
        self._base_dir = base_dir
        self.collected_date = collected_date

    def extract(self) -> list[dict]:
        partition = layout.date_partition(self._base_dir, self.collected_date)
        vendor_dirs = sorted(
            d
            for d in partition.glob(f"{layout.VENDOR_PARTITION_KEY}=*")
            if d.is_dir()
        )
        if not vendor_dirs:
            raise FileNotFoundError(f"Raw 파티션이 없습니다: {partition}")

        rows: list[dict] = []
        for vendor_dir in vendor_dirs:
            paths = sorted(vendor_dir.glob("*.parquet"))
            if not paths:
                raise FileNotFoundError(f"Raw Parquet 파일이 없습니다: {vendor_dir}")

            path = paths[-1]
            try:
                table = pq.ParquetFile(path).read()
            except (OSError, pa.ArrowInvalid) as exc:
                raise RuntimeError(f"Raw Parquet을 읽지 못했습니다: {path}") from exc
            if not table.num_rows:
                raise RuntimeError(f"Raw Parquet이 비어 있습니다: {path}")

            # vendor 는 파티션 키라서 파일 안에 없습니다. 디렉터리명에서 되살립니다.
            vendor = layout.vendor_from_partition(vendor_dir)
            rows += [
                {**row, "vendor": vendor, "bronze_path": str(path)}
                for row in table.to_pylist()
            ]

        logger.info(
            "raw_extract done vendors=%d rows=%d", len(vendor_dirs), len(rows)
        )
        return rows


class VehicleCatalogS3RawExtractor(Extractor):
    """S3 Raw에서 해당 날짜 파티션의 업체별 최신 Parquet을 읽어 합칩니다."""

    name = "vehicle_catalog_raw"

    def __init__(self, collected_date: str, bucket: str | None = None):
        _validate_collected_date(collected_date)
        load_local_env()
        self._bucket = bucket or os.environ[BUCKET_ENV_VAR]
        self.collected_date = collected_date

    def extract(self) -> list[dict]:
        prefix = layout.raw_date_prefix(self.collected_date)
        keys = list_keys(self._bucket, prefix)
        if not keys:
            raise FileNotFoundError(f"Raw 파티션이 없습니다: s3://{self._bucket}/{prefix}")

        by_vendor: dict[str, list[str]] = {}
        for key in keys:
            by_vendor.setdefault(layout.vendor_from_key(key), []).append(key)

        rows: list[dict] = []
        for vendor, vendor_keys in sorted(by_vendor.items()):
            key = sorted(vendor_keys)[-1]
            body = get_object_bytes(self._bucket, key)
            try:
                table = pq.ParquetFile(io.BytesIO(body)).read()
            except (OSError, pa.ArrowInvalid) as exc:
                raise RuntimeError(
                    f"Raw Parquet을 읽지 못했습니다: s3://{self._bucket}/{key}"
                ) from exc
            if not table.num_rows:
                raise RuntimeError(f"Raw Parquet이 비어 있습니다: s3://{self._bucket}/{key}")

            rows += [
                {**row, "vendor": vendor, "bronze_path": f"s3://{self._bucket}/{key}"}
                for row in table.to_pylist()
            ]

        logger.info("raw_extract done vendors=%d rows=%d", len(by_vendor), len(rows))
        return rows


def build_raw_extractor(
    storage: str, base_dir: str, collected_date: str, bucket: str | None = None
) -> Extractor:
    """storage 파라미터로 로컬/S3 Extractor 중 하나를 고릅니다."""
    if storage == "local":
        return VehicleCatalogRawExtractor(base_dir, collected_date)
    if storage == "s3":
        return VehicleCatalogS3RawExtractor(collected_date, bucket=bucket)
    raise ValueError(f"알 수 없는 storage: {storage!r} (local 또는 s3)")
