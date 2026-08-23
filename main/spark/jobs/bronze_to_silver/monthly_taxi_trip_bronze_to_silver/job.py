import argparse
import logging
import os
import re
from pathlib import Path
from typing import Optional
from urllib.parse import urlsplit

from shared.common.s3_reader import list_keys
from shared.common.service_area_path import candidate_prefixes, candidate_roots
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
BRONZE_DATA_FILE_NAME = "data.parquet"
COLLECTED_AT_DIR_PATTERN = re.compile(r"^collected_at=(\d{8}T\d{12}Z)$")
TIMESTAMP_FILE_PATTERN = re.compile(r"^\d{8}T\d{12}Z\.parquet$")
SOURCE_COLLECTED_AT_PATTERN = re.compile(r"^source_collected_at=(\d{8}T\d{12}Z)$")


DEFAULT_LOCAL_INPUT = "data/bronze/monthly_taxi_trip"
DEFAULT_LOCAL_OUTPUT = "data/silver/monthly_taxi_trip"


def bronze_collection_token(path: Path) -> str | None:
    if TIMESTAMP_FILE_PATTERN.fullmatch(path.name):
        return path.stem
    if path.name != BRONZE_DATA_FILE_NAME:
        return None
    match = COLLECTED_AT_DIR_PATTERN.fullmatch(path.parent.name)
    return match.group(1) if match else None


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


class SilverVersionDirectoryLoader(Loader):
    """검증 전 버전 디렉터리에 Spark part 파일을 그대로 씁니다."""

    def __init__(self, path: str):
        self._path = path

    def write(self, data) -> WriteResult:
        payload = _silver_file_payload(data)
        row_count = data.count()
        payload.write.mode("overwrite").parquet(self._path)
        return WriteResult(location=self._path, row_count=row_count)


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


def latest_partition_file(
    input_path: str, year_month: str, service_area: str | None = None
) -> Optional[str]:
    """`year_month=` 파티션 안의 최신 Parquet 파일 경로. 파티션/파일이 없으면 None.

    지역 경로를 먼저 보고, 없으면 지역 없는 경로를 봅니다(#851).
    """
    if is_s3_path(input_path):
        return _latest_s3_partition_file(input_path, year_month, service_area)
    for root in candidate_roots(input_path, service_area):
        partition_dir = root / f"year_month={year_month}"
        if not partition_dir.exists():
            continue
        parquet_files = sorted(
            (
                *partition_dir.glob("*.parquet"),
                *partition_dir.glob("collected_at=*/data.parquet"),
            )
        )
        if not parquet_files:
            continue
        versioned = [
            (path, bronze_collection_token(path)) for path in parquet_files
        ]
        versioned = [(path, token) for path, token in versioned if token]
        selected = (
            max(versioned, key=lambda item: item[1])[0]
            if versioned
            else parquet_files[-1]
        )
        return str(selected)
    return None


def latest_partition_files(
    input_path: str, service_area: str | None = None
) -> list[str]:
    """Bronze 루트에서 월별 최신 수집본 하나씩만 고릅니다.

    한 레벨 glob 이라 지역 계층이 들어가면 **조용히 빈 목록**이 됩니다. 후보 루트를
    모두 훑어 합집합으로 모읍니다(#851).
    """
    year_months = {
        partition.name.removeprefix("year_month=")
        for root in candidate_roots(input_path, service_area)
        for partition in root.glob("year_month=????-??")
    }
    selected = []
    for year_month in sorted(year_months):
        latest = latest_partition_file(input_path, year_month, service_area)
        if latest is not None:
            selected.append(latest)
    return selected


def _latest_s3_partition_file(
    input_path: str, year_month: str, service_area: str | None = None
) -> Optional[str]:
    scheme = input_path.split("://", 1)[0]
    parsed = urlsplit(input_path)
    bucket = parsed.netloc
    base_key = parsed.path.lstrip("/").rstrip("/")
    for area_prefix in candidate_prefixes(base_key, service_area=service_area):
        partition_prefix = f"{area_prefix}/year_month={year_month}/"
        parquet_keys = sorted(
            key for key in list_keys(bucket, partition_prefix)
            if key.endswith(".parquet")
        )
        if not parquet_keys:
            continue
        versioned = [
            (key, bronze_collection_token(Path(key))) for key in parquet_keys
        ]
        versioned = [(key, token) for key, token in versioned if token]
        selected = (
            max(versioned, key=lambda item: item[1])[0]
            if versioned
            else parquet_keys[-1]
        )
        return f"{scheme}://{bucket}/{selected}"
    return None


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
        "--output_version",
        default=None,
        help="검증 전 source_collected_at Silver 디렉터리 경로",
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
    output_version = (
        resolve_path(args.output_version) if args.output_version else None
    )
    if output_version:
        version_path = Path(urlsplit(output_version).path)
        if (
            version_path.parent.name != ".staging"
            or not SOURCE_COLLECTED_AT_PATTERN.fullmatch(version_path.name)
        ):
            raise ValueError(
                "--output_version은 .staging/source_collected_at=<UTC> 디렉터리여야 합니다"
            )

    if bool(args.start_year_month) != bool(args.end_year_month):
        raise ValueError("--start_year_month와 --end_year_month는 함께 줘야 합니다")
    if output_version and args.start_year_month:
        raise ValueError("--output_version은 여러 월 range 적재와 함께 쓸 수 없습니다")

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
        SilverVersionDirectoryLoader(output_version)
        if output_version
        else SparkParquetLoader(output_path, partition_by=["year_month"])
    )
    transformer = MonthlyTaxiTripCleanTransformer(error_threshold=args.error_threshold)

    result = Pipeline(extractor, loader, transformer=transformer).run()
    logger.info("Monthly Taxi Trip Bronze to Silver Pipeline completed: %s", result)
    return result


if __name__ == "__main__":
    main()
