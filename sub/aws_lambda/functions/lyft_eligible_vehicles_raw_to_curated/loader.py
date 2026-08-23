"""정제된 Lyft 배차 가능 차량 목록을 Curated Parquet으로 적재합니다."""

import io
import logging
import os

import pyarrow as pa
import pyarrow.parquet as pq
from pipeline_core.loader import Loader, WriteResult

from schema.source import ELIGIBLE_VEHICLES_SCHEMA as SCHEMA

from shared.aws_lambda.common.atomic_write import atomic_write
from sub.aws_lambda.common import lyft_eligible_vehicles_layout as layout
from shared.common.env import load_local_env
from shared.aws_lambda.common.s3_loader import BUCKET_ENV_VAR, S3Loader, S3Object

logger = logging.getLogger(__name__)


def _group_by_city(data: list[dict]) -> dict[str, list[dict]]:
    if not data:
        raise ValueError("적재할 Lyft 배차 가능 목록 Curated 데이터가 없습니다.")
    by_city: dict[str, list[dict]] = {}
    for row in data:
        by_city.setdefault(row[layout.CITY_PARTITION_KEY], []).append(row)
    return by_city


def _ensure_collected_date_matches(city_rows: list[dict], expect_collected_date: str | None):
    collected_date = city_rows[0]["collected_at"].date()
    if expect_collected_date and collected_date.isoformat() != expect_collected_date:
        raise ValueError(
            f"요청한 수집일과 변환된 수집일이 다릅니다: "
            f"{expect_collected_date} != {collected_date.isoformat()}"
        )
    return collected_date


class LyftEligibleVehiclesCuratedLoader(Loader):
    """도시별 Curated 파일 하나를 쓰고 재실행 시 같은 파일을 덮어씁니다."""

    def __init__(self, base_dir: str, expect_collected_date: str | None = None):
        self._base_dir = base_dir
        self._expect_collected_date = expect_collected_date
        self.paths: list[str] = []

    def write(self, data: list[dict]) -> WriteResult:
        by_city = _group_by_city(data)

        written_rows = 0
        for city, city_rows in sorted(by_city.items()):
            collected_date = _ensure_collected_date_matches(
                city_rows, self._expect_collected_date
            )

            path = layout.curated_file(self._base_dir, collected_date, city)
            path.parent.mkdir(parents=True, exist_ok=True)

            table = pa.Table.from_pylist(city_rows, schema=SCHEMA)
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


class LyftEligibleVehiclesS3CuratedLoader(Loader):
    """도시별 Curated Parquet 하나씩 S3에 씁니다. 같은 key는 재실행 시 덮어씁니다."""

    def __init__(self, expect_collected_date: str | None = None, bucket: str | None = None):
        load_local_env()
        self._bucket = bucket or os.environ[BUCKET_ENV_VAR]
        self._expect_collected_date = expect_collected_date
        self.paths: list[str] = []

    def write(self, data: list[dict]) -> WriteResult:
        by_city = _group_by_city(data)

        written_rows = 0
        for city, city_rows in sorted(by_city.items()):
            collected_date = _ensure_collected_date_matches(
                city_rows, self._expect_collected_date
            )

            key = layout.curated_key(collected_date, city)
            table = pa.Table.from_pylist(city_rows, schema=SCHEMA)
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


def build_curated_loader(
    storage: str, base_dir: str, collected_date: str, bucket: str | None = None
) -> Loader:
    """storage 파라미터로 로컬/S3 Loader 중 하나를 고릅니다."""
    if storage == "local":
        return LyftEligibleVehiclesCuratedLoader(base_dir, expect_collected_date=collected_date)
    if storage == "s3":
        return LyftEligibleVehiclesS3CuratedLoader(expect_collected_date=collected_date, bucket=bucket)
    raise ValueError(f"알 수 없는 storage: {storage!r} (local 또는 s3)")
