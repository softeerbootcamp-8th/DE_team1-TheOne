"""두 Clean Silver 를 기간 조인해 기사 운행 이력 Silver 를 적재합니다.

사용 예:
    cd spark && PYTHONPATH="$(pwd):$(pwd)/..:$(pwd)/../libs/pipeline_core" \\
      uv run --frozen python jobs/driver_trip/job.py \\
      --trips_path ../data/silver/hvfhv/year_month=2026-06 \\
      --leases_path ../data/silver/driver_vehicle_leases/year_month=2026-06 \\
      --output_path ../data/silver/hvfhv_driver_trip \\
      --year_month 2026-06
"""

import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from common.io import SparkParquetLoader
from common.session import get_or_create_spark_session
from jobs.driver_trip.transformer import build_driver_trip
from pipeline_core.loader import WriteResult


def read_trips(spark: SparkSession, trips_path: str) -> DataFrame:
    """HVFHV Clean Silver 를 읽되 `year_month` 파티션 컬럼을 살립니다.

    DAG 는 `.../hvfhv/year_month=2026-06` 처럼 파티션 디렉터리를 직접 넘깁니다.
    그 경로를 그대로 읽으면 `year_month` 는 **디렉터리 이름에만 있고 parquet 안에는
    없어서** 컬럼이 사라집니다. 대상 월 검증과 출력 파티셔닝이 그 컬럼을 쓰므로
    UNRESOLVED_COLUMN 으로 죽습니다.

    `basePath` 로 부모를 알려주면 Spark 가 디렉터리 이름에서 값을 되살립니다.
    """
    path = Path(trips_path)
    if path.name.startswith("year_month="):
        return spark.read.option("basePath", str(path.parent)).parquet(str(path))
    return spark.read.parquet(trips_path)


def main(args_list: list[str] | None = None) -> WriteResult:
    parser = argparse.ArgumentParser(description="기사 운행 이력 Silver Spark job")
    parser.add_argument("--trips_path", required=True, help="HVFHV Clean Silver 파티션 경로")
    parser.add_argument("--leases_path", required=True, help="기사 리스 Clean Silver 파티션 경로")
    parser.add_argument("--output_path", required=True)
    parser.add_argument("--year_month", required=True)
    parser.add_argument("--spark_memory", default="4g", help="Spark driver memory")
    args = parser.parse_args(args_list)

    spark = get_or_create_spark_session(
        "hvfhv_driver_trip_silver", driver_memory=args.spark_memory
    )
    silver = build_driver_trip(
        read_trips(spark, args.trips_path),
        spark.read.parquet(args.leases_path),
        year_month=args.year_month,
    )
    return SparkParquetLoader(args.output_path, partition_by=["year_month"]).write(silver)


if __name__ == "__main__":
    main()
