"""fueleconomy.gov 차종별 제원 적재(load).

extract 가 만든 행 목록을 parquet 으로 씁니다.
지금은 로컬 경로만 지원하고, S3 적재는 다음 이슈에서 붙입니다.

원본 컬럼을 버리지 않는 게 목적이라 스키마를 고정하지 않고 들어온 컬럼에서
만듭니다. 원본이 컬럼을 추가해도 그대로 실립니다. 값은 전부 문자열이고
타입 변환은 실버 단계에서 합니다.
"""

import logging
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pipeline_core.loader import Loader, WriteResult

from ..common import vehicle_specs_layout as layout
from ..common.atomic_write import atomic_write
from ..common.s3_loader import S3Loader, S3Object

logger = logging.getLogger(__name__)

# source 는 파티션 키(source=)로만 남깁니다. 파일 안에 같은 이름의 컬럼을 또 두면
# 읽을 때 파티션 값(dictionary)과 타입이 충돌합니다.
PARTITION_KEY = layout.SOURCE_PARTITION_KEY
# 유일하게 문자열이 아닌 컬럼. 나머지는 원본 그대로 문자열입니다.
TIMESTAMP_COLUMN = "collected_at"


def build_schema(row: dict) -> pa.Schema:
    """행 하나를 보고 스키마를 만듭니다. collected_at 만 timestamp, 나머지는 string."""
    return pa.schema(
        [
            (name, pa.timestamp("us", tz="UTC") if name == TIMESTAMP_COLUMN else pa.string())
            for name in row
            if name != PARTITION_KEY
        ]
    )


class VehicleSpecsBronzeLoader(Loader):
    """행 목록을 파티션 하나에 parquet 한 개로 로컬에 씁니다."""

    def __init__(self, base_dir: str, collected_at: datetime):
        self._base_dir = base_dir
        self._collected_at = collected_at

    def partition_path(self, source: str) -> Path:
        """collected_date / source 로 나눈 Hive 파티션 경로.

        경로 규칙은 `common.vehicle_specs_layout` 이 단독으로 정합니다. 여기서 따로
        조립하면 읽는 쪽·검증하는 쪽과 조용히 어긋납니다.
        """
        return layout.source_partition(
            self._base_dir, f"{self._collected_at:%Y-%m-%d}", source
        )

    def write(self, data: list[dict]) -> WriteResult:
        source = data[0][PARTITION_KEY]
        path = layout.bronze_file(self._base_dir, source, self._collected_at)
        path.parent.mkdir(parents=True, exist_ok=True)

        table = pa.Table.from_pylist(data, schema=build_schema(data[0]))
        atomic_write(
            path,
            lambda temporary: pq.write_table(
                table, temporary, compression="snappy"
            ),
        )

        logger.info(
            "bronze_load done path=%s rows=%d columns=%d bytes=%d",
            path,
            table.num_rows,
            table.num_columns,
            path.stat().st_size,
        )
        return WriteResult(location=str(path), row_count=table.num_rows)


class VehicleSpecsS3BronzeLoader(Loader):
    """행 목록을 파티션 하나에 parquet 한 개로 S3에 씁니다."""

    def __init__(self, collected_at: datetime, bucket: str | None = None):
        self._collected_at = collected_at
        self._bucket = bucket

    def write(self, data: list[dict]) -> WriteResult:
        source = data[0][PARTITION_KEY]
        key = layout.bronze_key(source, self._collected_at)

        table = pa.Table.from_pylist(data, schema=build_schema(data[0]))
        buffer = pa.BufferOutputStream()
        pq.write_table(table, buffer, compression="snappy")
        body = buffer.getvalue().to_pybytes()

        result = S3Loader(key=key, bucket=self._bucket).write(
            S3Object(body=body, row_count=table.num_rows)
        )
        logger.info(
            "bronze_load done location=%s rows=%d columns=%d bytes=%d",
            result.location,
            table.num_rows,
            table.num_columns,
            len(body),
        )
        return result


def build_bronze_loader(
    storage: str, base_dir: str, collected_at: datetime, bucket: str | None = None
) -> Loader:
    """storage 파라미터로 로컬/S3 Loader 중 하나를 고릅니다."""
    if storage == "local":
        return VehicleSpecsBronzeLoader(base_dir, collected_at)
    if storage == "s3":
        return VehicleSpecsS3BronzeLoader(collected_at, bucket=bucket)
    raise ValueError(f"알 수 없는 storage: {storage!r} (local 또는 s3)")
