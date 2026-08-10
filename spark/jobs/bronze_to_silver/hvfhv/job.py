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

    args = parser.parse_args(args_list)

    input_path = resolve_path(args.input_path)
    output_path = resolve_path(args.output_path)
    error_log_path = resolve_path(args.error_log_path)
    zone_lookup_path = resolve_path(args.zone_lookup_path)

    spark = get_or_create_spark_session("hvfhv_bronze_to_silver")
    spark.sparkContext.setLogLevel("WARN")

    extractor = SparkParquetExtractor(spark, input_path)
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
