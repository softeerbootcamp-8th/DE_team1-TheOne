import argparse
import logging
import os
import re
from pathlib import Path
from typing import Optional
from urllib.parse import urlsplit

from shared.aws_lambda.common.s3_reader import list_keys
from shared.spark.common.io import SparkParquetExtractor, SparkParquetLoader
from shared.spark.common.session import get_or_create_spark_session
from pipeline_core.pipeline import Pipeline, PipelineResult
from main.spark.jobs.bronze_to_silver.monthly_taxi_trip_bronze_to_silver.transformer import (
    MonthlyTaxiTripCleanTransformer,
)

logger = logging.getLogger(__name__)

CURRENT_FILE = Path(__file__).resolve()
# spark/jobs/bronze_to_silver/monthly_taxi_trip_bronze_to_silver/job.py -> project root
PROJECT_ROOT = CURRENT_FILE.parents[5]
TIMESTAMP_FILE_PATTERN = re.compile(r"^\d{8}T\d{12}Z\.parquet$")


DEFAULT_LOCAL_INPUT = "data/bronze/monthly_taxi_trip"
DEFAULT_LOCAL_OUTPUT = "data/silver/monthly_taxi_trip"


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
        "--error_threshold", type=float, default=0.05,
        help="불합격 행 허용 비율 (기본 0.05). DAG 는 HVFHV_ERROR_THRESHOLD 로 넘깁니다.",
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

    if bool(args.start_year_month) != bool(args.end_year_month):
        raise ValueError("--start_year_month와 --end_year_month는 함께 줘야 합니다")

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

    spark = get_or_create_spark_session("monthly_taxi_trip_bronze_to_silver", driver_memory=args.spark_memory)
    spark.sparkContext.setLogLevel("WARN")

    extractor = SparkParquetExtractor(spark, target_input_path)
    loader = SparkParquetLoader(output_path, partition_by=["year_month"])
    transformer = MonthlyTaxiTripCleanTransformer(error_threshold=args.error_threshold)

    result = Pipeline(extractor, loader, transformer=transformer).run()
    logger.info("Monthly Taxi Trip Bronze to Silver Pipeline completed: %s", result)
    return result


if __name__ == "__main__":
    main()
