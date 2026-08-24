"""EIA 기반 일별 연료비를 통합 Silver Parquet 으로 적재합니다.

Gold 가 읽는 자리에 공용 스키마(`schema/silver/gas_ev_price.py`)로 씁니다.
Gold 는 어느 경로로 만들어졌는지 몰라도 되고, 구분이 필요하면 `price_source` 를 봅니다.
"""

import io
import logging
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pipeline_core.loader import Loader, WriteResult

from main.aws_lambda.common.monthly_dataset import join_segments, service_area_segment

from schema.silver import CLEAN_FUEL_PRICE_SCHEMA as SCHEMA

from shared.aws_lambda.common.atomic_write import atomic_write, invalidate_success_marker
from shared.aws_lambda.common.s3_loader import S3Loader, S3Object

logger = logging.getLogger(__name__)

DATASET = "gas_ev_price"
# 데이터가 나타내는 달입니다. 전에는 `collected_month` 였는데, 값은 데이터의 달인데
# 이름은 "수집" 이라 구조를 오해하게 만들었습니다. TLC Silver 와 같은 이름으로 맞춥니다.
PARTITION_KEY = "year_month"
FILE_NAME = "gas_ev_price.parquet"


def silver_file(
    base_dir: str, year_month: str, service_area: str
) -> Path:
    dataset_root = Path(base_dir) / DATASET
    area = service_area_segment(service_area)
    return (
        (dataset_root / area)
        / f"{PARTITION_KEY}={year_month}"
        / FILE_NAME
    )


def silver_key(year_month: str, service_area: str) -> str:
    return join_segments(
        "silver",
        DATASET,
        service_area_segment(service_area),
        f"{PARTITION_KEY}={year_month}",
        FILE_NAME,
    )


class EiaFuelPriceSilverLoader(Loader):
    """대상 월 한 달치를 고정 경로의 로컬 Parquet 하나로 저장합니다."""

    def __init__(
        self,
        base_dir: str,
        year_month: str,
        service_area: str,
    ):
        self._base_dir = base_dir
        self._year_month = year_month
        self._service_area = service_area

    def write(self, data: list[dict]) -> WriteResult:
        if not data:
            raise ValueError("적재할 연료비 Silver 데이터가 없습니다.")

        table = pa.Table.from_pylist(data, schema=SCHEMA)
        path = silver_file(self._base_dir, self._year_month, self._service_area)
        path.parent.mkdir(parents=True, exist_ok=True)
        invalidate_success_marker(path.parent)
        atomic_write(
            path,
            lambda temporary: pq.write_table(table, temporary, compression="snappy"),
        )

        logger.info(
            "silver_load done path=%s year_month=%s rows=%d",
            path, self._year_month, table.num_rows,
        )
        return WriteResult(location=str(path), row_count=table.num_rows)


class EiaFuelPriceS3SilverLoader(Loader):
    """대상 월 한 달치를 고정 key의 S3 Parquet 하나로 저장합니다."""

    def __init__(
        self,
        year_month: str,
        service_area: str,
        bucket: str | None = None,
    ):
        self._year_month = year_month
        self._bucket = bucket
        self._service_area = service_area

    def write(self, data: list[dict]) -> WriteResult:
        if not data:
            raise ValueError("적재할 연료비 Silver 데이터가 없습니다.")

        table = pa.Table.from_pylist(data, schema=SCHEMA)
        buffer = io.BytesIO()
        pq.write_table(table, buffer, compression="snappy")

        result = S3Loader(
            key=silver_key(self._year_month, self._service_area),
            bucket=self._bucket,
            invalidate_parent_success=True,
        ).write(
            S3Object(body=buffer.getvalue(), row_count=table.num_rows)
        )
        logger.info(
            "silver_load done location=%s year_month=%s rows=%d",
            result.location,
            self._year_month,
            table.num_rows,
        )
        return result


def build_silver_loader(
    storage: str,
    base_dir: str,
    bucket: str | None,
    year_month: str,
    service_area: str,
) -> Loader:
    if storage == "local":
        return EiaFuelPriceSilverLoader(base_dir, year_month, service_area)
    if storage == "s3":
        return EiaFuelPriceS3SilverLoader(
            year_month,
            service_area,
            bucket=bucket,
        )
    raise ValueError(f"알 수 없는 storage: {storage!r} (local 또는 s3)")
