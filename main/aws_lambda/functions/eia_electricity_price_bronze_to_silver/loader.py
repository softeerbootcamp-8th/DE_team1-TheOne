"""일별 충전 단가를 CLEAN Silver Parquet 으로 적재합니다."""

import io
import logging
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pipeline_core.loader import Loader, WriteResult

from schema.silver import CLEAN_EV_CHARGING_PRICE_SCHEMA
from shared.aws_lambda.common.atomic_write import atomic_write
from shared.aws_lambda.common.s3_loader import S3Loader, S3Object

logger = logging.getLogger(__name__)

DATASET = "eia_electricity_price"
# 데이터가 나타내는 달입니다(수집한 달이 아닙니다). Bronze 는 `collected_date` 로
# 나뉘는데 — 한 파일에 이력이 통째로 들어 있어서 — Silver 부터는 데이터의 달로 나뉩니다.
PARTITION_KEY = "year_month"
FILE_NAME = f"{DATASET}.parquet"


def silver_file(base_dir: str, year_month: str) -> Path:
    return Path(base_dir) / DATASET / f"{PARTITION_KEY}={year_month}" / FILE_NAME


def silver_key(year_month: str) -> str:
    return f"silver/{DATASET}/{PARTITION_KEY}={year_month}/{FILE_NAME}"


class EiaElectricityPriceSilverLoader(Loader):
    """대상 월 한 달치를 고정 경로의 로컬 Parquet 하나로 저장합니다."""

    def __init__(self, base_dir: str, year_month: str):
        self._base_dir = base_dir
        self._year_month = year_month

    def write(self, data: list[dict]) -> WriteResult:
        if not data:
            raise ValueError("적재할 충전 단가 Silver 데이터가 없습니다.")

        path = silver_file(self._base_dir, self._year_month)
        path.parent.mkdir(parents=True, exist_ok=True)
        table = pa.Table.from_pylist(data, schema=CLEAN_EV_CHARGING_PRICE_SCHEMA)
        atomic_write(
            path,
            lambda temporary: pq.write_table(table, temporary, compression="snappy"),
        )

        logger.info(
            "silver_load done path=%s year_month=%s rows=%d",
            path, self._year_month, table.num_rows,
        )
        return WriteResult(location=str(path), row_count=table.num_rows)


class EiaElectricityPriceS3SilverLoader(Loader):
    """대상 월 한 달치를 고정 key의 S3 Parquet 하나로 저장합니다."""

    def __init__(self, year_month: str, bucket: str | None = None):
        self._year_month = year_month
        self._bucket = bucket

    def write(self, data: list[dict]) -> WriteResult:
        if not data:
            raise ValueError("적재할 충전 단가 Silver 데이터가 없습니다.")

        table = pa.Table.from_pylist(data, schema=CLEAN_EV_CHARGING_PRICE_SCHEMA)
        buffer = io.BytesIO()
        pq.write_table(table, buffer, compression="snappy")

        result = S3Loader(key=silver_key(self._year_month), bucket=self._bucket).write(
            S3Object(body=buffer.getvalue(), row_count=table.num_rows)
        )
        logger.info(
            "silver_load done location=%s year_month=%s rows=%d",
            result.location, self._year_month, table.num_rows,
        )
        return result


def build_silver_loader(
    storage: str, base_dir: str, bucket: str | None, year_month: str
) -> Loader:
    if storage == "local":
        return EiaElectricityPriceSilverLoader(base_dir, year_month)
    if storage == "s3":
        return EiaElectricityPriceS3SilverLoader(year_month, bucket=bucket)
    raise ValueError(f"알 수 없는 storage: {storage!r} (local 또는 s3)")
