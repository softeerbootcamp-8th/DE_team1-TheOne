import argparse
import logging
from pathlib import Path
from typing import Optional

from shared.spark.common.io import SparkParquetExtractor, SparkParquetLoader
from shared.spark.common.session import get_or_create_spark_session
from pipeline_core.pipeline import Pipeline, PipelineResult
from main.spark.jobs.bronze_to_silver.hvfhv.transformer import HVFHVCleanTransformer

logger = logging.getLogger(__name__)

CURRENT_FILE = Path(__file__).resolve()
# spark/jobs/bronze_to_silver/hvfhv/job.py -> project root
PROJECT_ROOT = CURRENT_FILE.parents[5]


def resolve_path(path_str: str) -> str:
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
    partition_dir = Path(input_path) / f"year_month={year_month}"
    if not partition_dir.exists():
        return None
    parquet_files = sorted(partition_dir.glob("*.parquet"))
    if not parquet_files:
        return None
    return str(parquet_files[-1])


def main(args_list: Optional[list[str]] = None) -> PipelineResult:
    parser = argparse.ArgumentParser(description="HVFHV Bronze to Silver Pipeline Job")
    parser.add_argument("--input_path", default="data/bronze/hvfhv", help="Path to bronze raw data")
    parser.add_argument("--output_path", default="data/silver/hvfhv", help="Path to save silver clean data")
    parser.add_argument(
        "--error_threshold", type=float, default=0.05,
        help="불합격 행 허용 비율 (기본 0.05). DAG 는 HVFHV_ERROR_THRESHOLD 로 넘깁니다.",
    )
    parser.add_argument("--spark_memory", default="4g", help="Spark driver memory")
    parser.add_argument("--start_year_month", default=None, help="시작 연월 (예: 2024-01). 한 달만 처리하려면 end와 동일하게")
    parser.add_argument("--end_year_month", default=None, help="종료 연월 (예: 2024-12, 포함)")

    args = parser.parse_args(args_list)

    input_path = resolve_path(args.input_path)
    output_path = resolve_path(args.output_path)

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
    else:
        target_input_path = input_path

    spark = get_or_create_spark_session("hvfhv_bronze_to_silver", driver_memory=args.spark_memory)
    spark.sparkContext.setLogLevel("WARN")

    extractor = SparkParquetExtractor(spark, target_input_path)
    loader = SparkParquetLoader(output_path, partition_by=["year_month"])
    transformer = HVFHVCleanTransformer(error_threshold=args.error_threshold)

    result = Pipeline(extractor, loader, transformer=transformer).run()
    logger.info("HVFHV Bronze to Silver Pipeline completed: %s", result)
    return result


if __name__ == "__main__":
    main()
