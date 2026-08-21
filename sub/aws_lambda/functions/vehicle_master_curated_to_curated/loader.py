"""차량 마스터를 도시별 Curated Parquet 으로 적재합니다."""

import io
import logging
import os
from datetime import date

import pyarrow as pa
import pyarrow.parquet as pq
from pipeline_core.loader import Loader, WriteResult

from schema.source import VEHICLE_MASTER_SCHEMA as SCHEMA

from shared.aws_lambda.common.atomic_write import atomic_write
from sub.aws_lambda.common import vehicle_master_layout as layout
from shared.common.env import load_local_env
from shared.aws_lambda.common.s3_loader import BUCKET_ENV_VAR, S3Loader, S3Object

logger = logging.getLogger(__name__)


def _group_by_city(data: list[dict]) -> dict[str, list[dict]]:
    if not data:
        raise ValueError("적재할 차량 마스터 Curated 데이터가 없습니다.")
    by_city: dict[str, list[dict]] = {}
    for row in data:
        by_city.setdefault(row[layout.CITY_PARTITION_KEY], []).append(row)
    return by_city


def _build_table(city_rows: list[dict]) -> pa.Table:
    # city 는 파티션 키라 컬럼에서 뺍니다. 스키마에 없는 키가 남아 있으면
    # from_pylist 가 조용히 무시하지 않고 실패합니다.
    return pa.Table.from_pylist(
        [{name: row.get(name) for name in SCHEMA.names} for row in city_rows],
        schema=SCHEMA,
    )


class VehicleMasterCuratedLoader(Loader):
    """도시별로 Parquet 하나씩 씁니다. 같은 파티션은 덮어씁니다.

    재실행하면 덮어씁니다. 같은 날 다시 만든 결과가 여러 개 남으면 읽는 쪽에서
    무엇이 맞는지 알 수 없기 때문입니다.
    """

    def __init__(self, base_dir: str, collected_date: str):
        self._base_dir = base_dir
        self._collected_date = date.fromisoformat(collected_date)
        # 이번 실행이 쓴 도시별 경로. Pipeline 이 중간 데이터를 감추므로
        # 핸들러가 반환값을 만들 때 여기서 읽습니다.
        self.paths: list[str] = []

    def write(self, data: list[dict]) -> WriteResult:
        by_city = _group_by_city(data)

        written_rows = 0
        for city, city_rows in sorted(by_city.items()):
            path = layout.curated_file(self._base_dir, self._collected_date, city)
            path.parent.mkdir(parents=True, exist_ok=True)

            table = _build_table(city_rows)
            atomic_write(
                path,
                lambda temporary: pq.write_table(
                    table, temporary, compression="snappy"
                ),
            )

            logger.info(
                "curated_load done path=%s city=%s rows=%d", path, city, table.num_rows
            )
            self.paths.append(str(path))
            written_rows += table.num_rows

        return WriteResult(
            location=str(layout.dataset_path(self._base_dir)),
            row_count=written_rows,
        )


class VehicleMasterCuratedS3Loader(Loader):
    """도시별 Curated Parquet 하나씩 S3에 씁니다. 같은 key는 재실행 시 덮어씁니다."""

    def __init__(self, collected_date: str, bucket: str | None = None):
        load_local_env()
        self._bucket = bucket or os.environ[BUCKET_ENV_VAR]
        self._collected_date = date.fromisoformat(collected_date)
        self.paths: list[str] = []

    def write(self, data: list[dict]) -> WriteResult:
        by_city = _group_by_city(data)

        written_rows = 0
        for city, city_rows in sorted(by_city.items()):
            key = layout.curated_key(self._collected_date, city)
            table = _build_table(city_rows)
            buffer = io.BytesIO()
            pq.write_table(table, buffer, compression="snappy")

            result = S3Loader(key=key, bucket=self._bucket).write(
                S3Object(body=buffer.getvalue(), row_count=table.num_rows)
            )
            logger.info(
                "curated_load done location=%s city=%s rows=%d",
                result.location,
                city,
                table.num_rows,
            )
            self.paths.append(result.location)
            written_rows += table.num_rows

        return WriteResult(
            location=f"s3://{self._bucket}/source/curated/{layout.DATASET}/",
            row_count=written_rows,
        )


def build_loader(storage: str, base_dir: str, collected_date: str, bucket: str | None = None) -> Loader:
    """storage 파라미터로 로컬/S3 Loader 중 하나를 고릅니다."""
    if storage == "local":
        return VehicleMasterCuratedLoader(base_dir, collected_date)
    if storage == "s3":
        return VehicleMasterCuratedS3Loader(collected_date, bucket=bucket)
    raise ValueError(f"알 수 없는 storage: {storage!r} (local 또는 s3)")
