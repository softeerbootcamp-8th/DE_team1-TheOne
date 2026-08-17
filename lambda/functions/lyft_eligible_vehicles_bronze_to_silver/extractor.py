"""실행일의 Lyft 배차 가능 차량 Bronze 스냅샷을 읽습니다."""

import io
import logging
import os
import re
from datetime import date

import pyarrow as pa
import pyarrow.parquet as pq
from pipeline_core.extractor import Extractor

from ..common import lyft_eligible_vehicles_layout as layout
from ..common.env import load_local_env
from ..common.s3_loader import BUCKET_ENV_VAR
from ..common.s3_reader import get_object_bytes, list_keys

logger = logging.getLogger(__name__)

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _validate_collected_date(collected_date: str) -> None:
    if not DATE_RE.fullmatch(collected_date):
        raise ValueError("collected_date는 YYYY-MM-DD 형식이어야 합니다.")
    try:
        date.fromisoformat(collected_date)
    except ValueError as exc:
        raise ValueError("유효하지 않은 collected_date입니다.") from exc


class LyftEligibleVehiclesBronzeExtractor(Extractor):
    """해당 날짜 파티션에서 도시별 최신 Bronze Parquet을 읽습니다."""

    name = "lyft_eligible_vehicles_bronze"

    def __init__(self, base_dir: str, collected_date: str):
        _validate_collected_date(collected_date)
        self._base_dir = base_dir
        self.collected_date = collected_date

    def extract(self) -> list[dict]:
        partition = layout.date_partition(self._base_dir, self.collected_date)
        city_dirs = sorted(
            d for d in partition.glob(f"{layout.CITY_PARTITION_KEY}=*") if d.is_dir()
        )
        if not city_dirs:
            raise FileNotFoundError(f"Bronze 파티션이 없습니다: {partition}")

        rows: list[dict] = []
        for city_dir in city_dirs:
            paths = sorted(city_dir.glob("*.parquet"))
            if not paths:
                raise FileNotFoundError(f"Bronze Parquet 파일이 없습니다: {city_dir}")

            path = paths[-1]
            try:
                table = pq.ParquetFile(path).read()
            except (OSError, pa.ArrowInvalid) as exc:
                raise RuntimeError(f"Bronze Parquet을 읽지 못했습니다: {path}") from exc
            if not table.num_rows:
                raise RuntimeError(f"Bronze Parquet이 비어 있습니다: {path}")

            city = layout.city_from_partition(city_dir)
            rows += [
                {**row, "city": city, "bronze_path": str(path)}
                for row in table.to_pylist()
            ]

        logger.info("bronze_extract done cities=%d rows=%d", len(city_dirs), len(rows))
        return rows


class LyftEligibleVehiclesS3BronzeExtractor(Extractor):
    """S3 Bronze에서 해당 날짜 파티션의 도시별 최신 Parquet을 읽어 합칩니다."""

    name = "lyft_eligible_vehicles_bronze"

    def __init__(self, collected_date: str, bucket: str | None = None):
        _validate_collected_date(collected_date)
        load_local_env()
        self._bucket = bucket or os.environ[BUCKET_ENV_VAR]
        self.collected_date = collected_date

    def extract(self) -> list[dict]:
        prefix = layout.bronze_date_prefix(self.collected_date)
        keys = list_keys(self._bucket, prefix)
        if not keys:
            raise FileNotFoundError(f"Bronze 파티션이 없습니다: s3://{self._bucket}/{prefix}")

        by_city: dict[str, list[str]] = {}
        for key in keys:
            by_city.setdefault(layout.city_from_key(key), []).append(key)

        rows: list[dict] = []
        for city, city_keys in sorted(by_city.items()):
            key = sorted(city_keys)[-1]
            body = get_object_bytes(self._bucket, key)
            try:
                table = pq.ParquetFile(io.BytesIO(body)).read()
            except (OSError, pa.ArrowInvalid) as exc:
                raise RuntimeError(
                    f"Bronze Parquet을 읽지 못했습니다: s3://{self._bucket}/{key}"
                ) from exc
            if not table.num_rows:
                raise RuntimeError(f"Bronze Parquet이 비어 있습니다: s3://{self._bucket}/{key}")

            rows += [
                {**row, "city": city, "bronze_path": f"s3://{self._bucket}/{key}"}
                for row in table.to_pylist()
            ]

        logger.info("bronze_extract done cities=%d rows=%d", len(by_city), len(rows))
        return rows


def build_bronze_extractor(
    storage: str, base_dir: str, collected_date: str, bucket: str | None = None
) -> Extractor:
    """storage 파라미터로 로컬/S3 Extractor 중 하나를 고릅니다."""
    if storage == "local":
        return LyftEligibleVehiclesBronzeExtractor(base_dir, collected_date)
    if storage == "s3":
        return LyftEligibleVehiclesS3BronzeExtractor(collected_date, bucket=bucket)
    raise ValueError(f"알 수 없는 storage: {storage!r} (local 또는 s3)")
