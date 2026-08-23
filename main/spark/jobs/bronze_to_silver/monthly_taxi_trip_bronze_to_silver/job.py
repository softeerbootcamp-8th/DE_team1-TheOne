import argparse
import logging
import os
import re
import shutil
from pathlib import Path
from typing import Optional
from urllib.parse import urlsplit
from uuid import uuid4

import boto3

from shared.common.s3_reader import list_keys
from shared.spark.common.io import SparkParquetExtractor, SparkParquetLoader
from shared.spark.common.session import get_or_create_spark_session
from pipeline_core.loader import Loader, WriteResult
from pipeline_core.pipeline import Pipeline, PipelineResult
from main.spark.jobs.bronze_to_silver.monthly_taxi_trip_bronze_to_silver.transformer import (
    FINAL_SCHEMA,
    MonthlyTaxiTripCleanTransformer,
)

logger = logging.getLogger(__name__)

CURRENT_FILE = Path(__file__).resolve()
# spark/jobs/bronze_to_silver/monthly_taxi_trip_bronze_to_silver/job.py -> project root
PROJECT_ROOT = CURRENT_FILE.parents[5]
TIMESTAMP_FILE_PATTERN = re.compile(r"^\d{8}T\d{12}Z\.parquet$")


DEFAULT_LOCAL_INPUT = "data/bronze/monthly_taxi_trip"
DEFAULT_LOCAL_OUTPUT = "data/silver/monthly_taxi_trip"


def _silver_file_payload(data):
    """파티션 컬럼을 파일 본문에서 빼고 Silver 물리 스키마를 확인합니다."""
    # Loader 계약 테스트의 최소 fake 객체는 Spark 스키마를 제공하지 않습니다.
    if not hasattr(data, "columns") or not hasattr(data, "schema"):
        return data
    if "year_month" not in data.columns:
        raise ValueError("Silver 변환 결과에 year_month 파티션 컬럼이 없습니다")
    payload = data.drop("year_month")
    expected = [
        (field.name, field.dataType)
        for field in FINAL_SCHEMA
        if field.name != "year_month"
    ]
    actual = [(field.name, field.dataType) for field in payload.schema]
    if actual != expected:
        raise ValueError("Silver 변환 결과 스키마가 물리 파일 계약과 다릅니다")
    return payload


def is_s3_path(path: str) -> bool:
    return path.startswith("s3://") or path.startswith("s3a://")


def default_paths(env: str, bucket: Optional[str]) -> tuple[str, str]:
    """`--env`로 로컬/배포 기본 입출력 경로를 고릅니다. `--input_path`/`--output_path`로 덮어쓸 수 있습니다."""
    if env == "local":
        return DEFAULT_LOCAL_INPUT, DEFAULT_LOCAL_OUTPUT
    if env == "prod":
        if not bucket:
            raise ValueError("--env prod는 --bucket(또는 DATA_LAKE_S3_BUCKET 환경변수)이 필요합니다")
        return (
            f"s3://{bucket}/bronze/monthly_taxi_trip",
            f"s3://{bucket}/silver/monthly_taxi_trip",
        )
    raise ValueError(f"알 수 없는 --env: {env!r} (local 또는 prod)")


def resolve_path(path_str: str) -> str:
    if is_s3_path(path_str):
        return path_str
    path = Path(path_str)
    if not path.is_absolute():
        return str(PROJECT_ROOT / path)
    return str(path)


class SingleParquetFileLoader(Loader):
    """Spark part 하나를 `path`로 받은 파일에 원자적으로 교체합니다.

    `path`가 최종 collected_at 이름인지 검증 전 staging 이름(#742)인지는
    호출부(Airflow)가 정합니다 — 이 Loader는 신경 쓰지 않습니다.
    """

    def __init__(self, path: str):
        self._path = path

    def write(self, data) -> WriteResult:
        payload = _silver_file_payload(data)
        row_count = data.count()
        if is_s3_path(self._path):
            self._write_s3(payload)
            return WriteResult(location=self._path, row_count=row_count)

        target = Path(self._path)
        temporary = target.with_name(f".{target.name}.{uuid4().hex}.tmp")
        try:
            payload.coalesce(1).write.mode("overwrite").parquet(str(temporary))
            parts = list(temporary.glob("part-*.parquet"))
            if len(parts) != 1:
                raise ValueError(f"Spark Parquet part 파일은 하나여야 합니다: {temporary}")
            target.parent.mkdir(parents=True, exist_ok=True)
            parts[0].replace(target)
        finally:
            shutil.rmtree(temporary, ignore_errors=True)
        return WriteResult(location=self._path, row_count=row_count)

    def _write_s3(self, data) -> None:
        parsed = urlsplit(self._path)
        bucket, final_key = parsed.netloc, parsed.path.lstrip("/")
        if not bucket or not final_key:
            raise ValueError(f"S3 Silver 경로가 올바르지 않습니다: {self._path}")

        temporary_prefix = f"{final_key}.tmp-{uuid4().hex}/"
        temporary_uri = f"s3://{bucket}/{temporary_prefix}"
        client = boto3.client("s3")
        try:
            data.coalesce(1).write.mode("overwrite").parquet(temporary_uri)
            keys = list_keys(bucket, temporary_prefix)
            parts = [
                key
                for key in keys
                if Path(key).name.startswith("part-") and key.endswith(".parquet")
            ]
            if len(parts) != 1:
                raise ValueError(
                    f"Spark Parquet part 파일은 하나여야 합니다: {temporary_uri}"
                )
            client.copy({"Bucket": bucket, "Key": parts[0]}, bucket, final_key)
        finally:
            for key in list_keys(bucket, temporary_prefix):
                client.delete_object(Bucket=bucket, Key=key)


def year_month_range(start_year_month: str, end_year_month: str) -> list[str]:
    """start_year_month 부터 end_year_month 까지(양끝 포함) "YYYY-MM" 목록을 순서대로 반환."""
    start_year, start_month = (int(part) for part in start_year_month.split("-"))
    end_year, end_month = (int(part) for part in end_year_month.split("-"))
    if (start_year, start_month) > (end_year, end_month):
        raise ValueError(f"start_year_month가 end_year_month보다 늦습니다: {start_year_month} > {end_year_month}")

    months = []
    year, month = start_year, start_month
    while (year, month) <= (end_year, end_month):
        months.append(f"{year}-{month:02d}")
        month += 1
        if month > 12:
            month = 1
            year += 1
    return months


def latest_partition_file(input_path: str, year_month: str) -> Optional[str]:
    """`year_month=` 파티션 안의 최신 Parquet 파일 경로. 파티션/파일이 없으면 None."""
    if is_s3_path(input_path):
        return _latest_s3_partition_file(input_path, year_month)
    partition_dir = Path(input_path) / f"year_month={year_month}"
    if not partition_dir.exists():
        return None
    parquet_files = sorted(partition_dir.glob("*.parquet"))
    if not parquet_files:
        return None
    timestamp_files = [
        path for path in parquet_files if TIMESTAMP_FILE_PATTERN.fullmatch(path.name)
    ]
    return str((timestamp_files or parquet_files)[-1])


def latest_partition_files(input_path: str) -> list[str]:
    """Bronze 루트에서 월별 최신 수집본 하나씩만 고릅니다."""
    selected = []
    for partition in sorted(Path(input_path).glob("year_month=????-??")):
        year_month = partition.name.removeprefix("year_month=")
        latest = latest_partition_file(input_path, year_month)
        if latest is not None:
            selected.append(latest)
    return selected


def _latest_s3_partition_file(input_path: str, year_month: str) -> Optional[str]:
    scheme = input_path.split("://", 1)[0]
    parsed = urlsplit(input_path)
    bucket = parsed.netloc
    partition_prefix = f"{parsed.path.lstrip('/').rstrip('/')}/year_month={year_month}/"
    parquet_keys = sorted(key for key in list_keys(bucket, partition_prefix) if key.endswith(".parquet"))
    if not parquet_keys:
        return None
    timestamp_keys = [
        key for key in parquet_keys if TIMESTAMP_FILE_PATTERN.fullmatch(Path(key).name)
    ]
    return f"{scheme}://{bucket}/{(timestamp_keys or parquet_keys)[-1]}"


def main(args_list: Optional[list[str]] = None) -> PipelineResult:
    parser = argparse.ArgumentParser(description="Monthly Taxi Trip Bronze to Silver Pipeline Job")
    parser.add_argument(
        "--env", choices=["local", "prod"], default=os.getenv("SPARK_JOB_ENV", "local"),
        help="local이면 로컬 폴더, prod면 S3에서 읽고 씀 (기본 SPARK_JOB_ENV 환경변수, 없으면 local)",
    )
    parser.add_argument(
        "--bucket", default=os.getenv("DATA_LAKE_S3_BUCKET"),
        help="--env prod일 때 쓸 S3 버킷 (기본 DATA_LAKE_S3_BUCKET 환경변수)",
    )
    parser.add_argument("--input_path", default=None, help="Path to bronze raw data. 비우면 --env 기본 경로")
    parser.add_argument("--output_path", default=None, help="Path to save silver clean data. 비우면 --env 기본 경로")
    parser.add_argument(
        "--output_file",
        default=None,
        help="수집 시각 파일명으로 쓸 단일 Silver Parquet 경로",
    )
    parser.add_argument(
        "--error_threshold", type=float, default=0.05,
        help=(
            "불합격 행 허용 비율 (기본 0.05). DAG 는 error_threshold Param 으로 넘기고, "
            "그 기본값은 Variable(hvfhv_error_threshold) 에서 옵니다 (#743)."
        ),
    )
    parser.add_argument("--spark_memory", default="4g", help="Spark driver memory")
    parser.add_argument("--start_year_month", default=None, help="시작 연월 (예: 2024-01). 한 달만 처리하려면 end와 동일하게")
    parser.add_argument("--end_year_month", default=None, help="종료 연월 (예: 2024-12, 포함)")
    args = parser.parse_args(args_list)

    input_path, output_path = args.input_path, args.output_path
    if input_path is None or output_path is None:
        default_input, default_output = default_paths(args.env, args.bucket)
        input_path = input_path or default_input
        output_path = output_path or default_output
    input_path = resolve_path(input_path)
    output_path = resolve_path(output_path)
    output_file = resolve_path(args.output_file) if args.output_file else None

    if output_file:
        output_name = Path(urlsplit(output_file).path).name
        # Airflow가 검증 전에는 "<수집시각>.staged.parquet" 이름을 넘깁니다(#742) —
        # 검증을 통과해야 validate_silver가 수집 시각 이름으로 승격합니다.
        stem, _, suffix = output_name.partition(".staged.")
        canonical_name = f"{stem}.{suffix}" if suffix else output_name
        if not TIMESTAMP_FILE_PATTERN.fullmatch(canonical_name):
            raise ValueError(
                "--output_file은 수집 시각 Parquet 파일명(또는 그 staging 이름)이어야 합니다"
            )

    if bool(args.start_year_month) != bool(args.end_year_month):
        raise ValueError("--start_year_month와 --end_year_month는 함께 줘야 합니다")
    if output_file and args.start_year_month:
        raise ValueError("--output_file은 여러 월 range 적재와 함께 쓸 수 없습니다")

    if args.start_year_month and args.end_year_month:
        target_input_path = []
        missing_year_months = []
        for year_month in year_month_range(args.start_year_month, args.end_year_month):
            resolved = latest_partition_file(input_path, year_month)
            if resolved is None:
                missing_year_months.append(year_month)
                continue
            target_input_path.append(resolved)

        if missing_year_months:
            raise FileNotFoundError(f"Bronze 파티션이 없거나 비어 있습니다: year_month={missing_year_months}")

        logger.info("선택된 Bronze 파일 %d개: %s", len(target_input_path), target_input_path)
    elif Path(input_path).is_dir():
        target_input_path = latest_partition_files(input_path)
        if not target_input_path:
            raise FileNotFoundError(f"Bronze 월 파티션이 없거나 비어 있습니다: {input_path}")
        logger.info("선택된 월별 최신 Bronze 파일 %d개: %s", len(target_input_path), target_input_path)
    else:
        target_input_path = input_path

    spark = get_or_create_spark_session(
        "monthly_taxi_trip_bronze_to_silver",
        driver_memory=args.spark_memory,
        local_mode=args.env == "local",
    )
    spark.sparkContext.setLogLevel("WARN")

    extractor = SparkParquetExtractor(spark, target_input_path)
    loader = (
        SingleParquetFileLoader(output_file)
        if output_file
        else SparkParquetLoader(output_path, partition_by=["year_month"])
    )
    transformer = MonthlyTaxiTripCleanTransformer(error_threshold=args.error_threshold)

    result = Pipeline(extractor, loader, transformer=transformer).run()
    logger.info("Monthly Taxi Trip Bronze to Silver Pipeline completed: %s", result)
    return result


if __name__ == "__main__":
    main()
