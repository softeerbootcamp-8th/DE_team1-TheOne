import argparse
import logging
from pathlib import Path
from typing import Optional

from common.io import SparkParquetExtractor, SparkParquetLoader
from common.session import get_or_create_spark_session
from pipeline_core.pipeline import Pipeline, PipelineResult
from jobs.bronze_to_silver.hvfhv.transformer import HVFHVCleanTransformer

logger = logging.getLogger(__name__)

CURRENT_FILE = Path(__file__).resolve()
# spark/jobs/bronze_to_silver/hvfhv/job.py -> project root
PROJECT_ROOT = CURRENT_FILE.parents[4]


def resolve_path(path_str: str) -> str:
    path = Path(path_str)
    if not path.is_absolute():
        return str(PROJECT_ROOT / path)
    return str(path)


def main(args_list: Optional[list[str]] = None) -> PipelineResult:
    parser = argparse.ArgumentParser(description="HVFHV Bronze to Silver Pipeline Job")
    parser.add_argument("--input_path", default="data/bronze/hvfhv", help="Path to bronze raw data")
    parser.add_argument("--output_path", default="data/silver/hvfhv", help="Path to save silver clean data")
    parser.add_argument("--error_log_path", default="data/silver/hvfhv_errors", help="Path to save invalid data logs")
    parser.add_argument("--zone_lookup_path", default="data/bronze/taxi_zone_lookup.csv", help="Path to taxi_zone_lookup.csv")
    parser.add_argument("--error_threshold", type=float, default=0.2, help="Validation error threshold (default: 0.2)")
    parser.add_argument("--spark_memory", default="4g", help="Spark driver memory")
    parser.add_argument("--year", default=None, help="Target year (e.g. 2024)")
    parser.add_argument("--month", default=None, help="Target month (e.g. 03)")

    args = parser.parse_args(args_list)

    input_path = resolve_path(args.input_path)
    output_path = resolve_path(args.output_path)
    error_log_path = resolve_path(args.error_log_path)
    zone_lookup_path = resolve_path(args.zone_lookup_path)

    if args.year and args.month:
        year_month = f"{args.year}-{str(args.month).zfill(2)}"
        partition_dir = Path(input_path) / f"year_month={year_month}"
        if not partition_dir.exists():
            raise FileNotFoundError(f"Bronze 파티션 경로가 존재하지 않습니다: {partition_dir}")

        parquet_files = sorted(partition_dir.glob("*.parquet"))
        if not parquet_files:
            raise FileNotFoundError(f"Bronze 파티션 내 Parquet 파일이 없습니다: {partition_dir}")

        target_input_path = str(parquet_files[-1])
        logger.info("선택된 최신 Bronze 파일: %s", target_input_path)
    else:
        target_input_path = input_path

    spark = get_or_create_spark_session("hvfhv_bronze_to_silver")
    spark.sparkContext.setLogLevel("WARN")

    extractor = SparkParquetExtractor(spark, target_input_path)
    loader = SparkParquetLoader(output_path, partition_by=["year_month"])
    transformer = HVFHVCleanTransformer(
        zone_lookup_path=zone_lookup_path,
        error_threshold=args.error_threshold,
        error_log_path=error_log_path,
    )

    result = Pipeline(extractor, loader, transformer=transformer).run()
    logger.info("HVFHV Bronze to Silver Pipeline completed: %s", result)
    return result


if __name__ == "__main__":
    main()
