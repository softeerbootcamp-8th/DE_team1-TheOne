"""실행일의 차종별 제원 Bronze 스냅샷을 읽습니다."""

import io
import logging
import os
import re
from datetime import date

import pyarrow as pa
import pyarrow.parquet as pq
from pipeline_core.extractor import Extractor

from sub.aws_lambda.common import vehicle_specs_layout as layout
from shared.aws_lambda.common.env import load_local_env
from shared.aws_lambda.common.s3_loader import BUCKET_ENV_VAR
from shared.aws_lambda.common.s3_reader import get_object_bytes, list_keys

logger = logging.getLogger(__name__)

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _validate_collected_date(collected_date: str) -> None:
    if not DATE_RE.fullmatch(collected_date):
        raise ValueError("collected_date는 YYYY-MM-DD 형식이어야 합니다.")
    try:
        date.fromisoformat(collected_date)
    except ValueError as exc:
        raise ValueError("유효하지 않은 collected_date입니다.") from exc

# Silver 를 만드는 데 실제로 필요한 컬럼만 읽습니다. Bronze 는 원본 84컬럼
# 5만 행이라 전부 dict 로 펼치면 수백 MB 를 씁니다. 나머지 컬럼은 버리는 게
# 아니라 Bronze 에 그대로 남아 있고, 필요해지면 여기에 이름만 추가하면 됩니다.
NEEDED_COLUMNS = (
    "id",
    "year",
    "make",
    "model",
    "baseModel",
    "comb08",
    "combE",
    "range",
    "atvType",
    "collected_at",
)


class VehicleSpecsBronzeExtractor(Extractor):
    """해당 collected_date 파티션에서 출처별 가장 최신 Parquet 을 읽어 합칩니다.

    Bronze 는 collected_date 아래에 source 파티션이 한 단계 더 있습니다.
    매 실행이 전량 스냅샷이라 같은 날 여러 번 돌면 파일이 쌓이므로
    출처별로 최신 것만 씁니다.
    """

    name = "fueleconomy_vehicle_specs_bronze"

    def __init__(self, base_dir: str, collected_date: str):
        _validate_collected_date(collected_date)
        self._base_dir = base_dir
        self.collected_date = collected_date

    def extract(self) -> list[dict]:
        partition = layout.date_partition(self._base_dir, self.collected_date)
        source_dirs = sorted(
            d for d in partition.glob(f"{layout.SOURCE_PARTITION_KEY}=*") if d.is_dir()
        )
        if not source_dirs:
            raise FileNotFoundError(f"Bronze 파티션이 없습니다: {partition}")

        rows: list[dict] = []
        for source_dir in source_dirs:
            paths = sorted(source_dir.glob("*.parquet"))
            if not paths:
                raise FileNotFoundError(f"Bronze Parquet 파일이 없습니다: {source_dir}")

            path = paths[-1]
            try:
                parquet = pq.ParquetFile(path)
                available = set(parquet.schema_arrow.names)
                missing = [c for c in NEEDED_COLUMNS if c not in available]
                if missing:
                    raise RuntimeError(
                        f"Bronze 에 필요한 컬럼이 없습니다: {missing} ({path})"
                    )
                table = parquet.read(columns=list(NEEDED_COLUMNS))
            except (OSError, pa.ArrowInvalid) as exc:
                raise RuntimeError(f"Bronze Parquet을 읽지 못했습니다: {path}") from exc
            if not table.num_rows:
                raise RuntimeError(f"Bronze Parquet이 비어 있습니다: {path}")

            # source 는 파티션 키라서 파일 안에 없습니다. 디렉터리명에서 되살립니다.
            source = layout.source_from_partition(source_dir)
            rows += [
                {**row, "source": source, "bronze_path": str(path)}
                for row in table.to_pylist()
            ]

        logger.info(
            "bronze_extract done sources=%d rows=%d", len(source_dirs), len(rows)
        )
        return rows


class VehicleSpecsS3BronzeExtractor(Extractor):
    """S3 Bronze에서 해당 날짜 파티션의 출처별 최신 Parquet을 읽어 합칩니다."""

    name = "fueleconomy_vehicle_specs_bronze"

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

        by_source: dict[str, list[str]] = {}
        for key in keys:
            by_source.setdefault(layout.source_from_key(key), []).append(key)

        rows: list[dict] = []
        for source, source_keys in sorted(by_source.items()):
            key = sorted(source_keys)[-1]
            body = get_object_bytes(self._bucket, key)
            try:
                parquet = pq.ParquetFile(io.BytesIO(body))
                available = set(parquet.schema_arrow.names)
                missing = [c for c in NEEDED_COLUMNS if c not in available]
                if missing:
                    raise RuntimeError(
                        f"Bronze 에 필요한 컬럼이 없습니다: {missing} (s3://{self._bucket}/{key})"
                    )
                table = parquet.read(columns=list(NEEDED_COLUMNS))
            except (OSError, pa.ArrowInvalid) as exc:
                raise RuntimeError(
                    f"Bronze Parquet을 읽지 못했습니다: s3://{self._bucket}/{key}"
                ) from exc
            if not table.num_rows:
                raise RuntimeError(f"Bronze Parquet이 비어 있습니다: s3://{self._bucket}/{key}")

            rows += [
                {**row, "source": source, "bronze_path": f"s3://{self._bucket}/{key}"}
                for row in table.to_pylist()
            ]

        logger.info("bronze_extract done sources=%d rows=%d", len(by_source), len(rows))
        return rows


def build_bronze_extractor(
    storage: str, base_dir: str, collected_date: str, bucket: str | None = None
) -> Extractor:
    """storage 파라미터로 로컬/S3 Extractor 중 하나를 고릅니다."""
    if storage == "local":
        return VehicleSpecsBronzeExtractor(base_dir, collected_date)
    if storage == "s3":
        return VehicleSpecsS3BronzeExtractor(collected_date, bucket=bucket)
    raise ValueError(f"알 수 없는 storage: {storage!r} (local 또는 s3)")
