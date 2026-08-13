"""구역쌍 이동시간 Silver 생성 CLI.

HVFHV Silver 전체(또는 지정한 월)를 읽어 구역쌍별 이동시간 중앙값을 만듭니다.
기사 배정(`jobs/driver_assignment/silver_job.py`)의 `--travel_times_path` 입력입니다.

사용 예:
    cd spark && PYTHONPATH="$(pwd):$(pwd)/.." uv run --frozen python jobs/travel_times/job.py \\
      --trips_path ../data/silver/hvfhv --output_path ../data/silver/taxi_zone_travel_times
"""

import argparse
import logging

from common.io import SparkParquetLoader
from common.session import get_or_create_spark_session
from jobs.travel_times.transformer import DEFAULT_MIN_TRIPS, build_travel_times

logger = logging.getLogger(__name__)


def main(args_list: list[str] | None = None):
    parser = argparse.ArgumentParser(description="구역쌍 이동시간 Silver Spark job")
    parser.add_argument("--trips_path", required=True, help="HVFHV Silver 경로")
    parser.add_argument("--output_path", required=True)
    parser.add_argument(
        "--min_trips",
        type=int,
        default=DEFAULT_MIN_TRIPS,
        help="이보다 적게 관측된 구역쌍은 버립니다",
    )
    args = parser.parse_args(args_list)

    spark = get_or_create_spark_session("taxi_zone_travel_times")
    travel_times = build_travel_times(
        spark.read.parquet(args.trips_path), min_trips=args.min_trips
    )

    # 파티션을 두지 않습니다. 배정이 이 테이블을 통째로 dict 로 올려서 쓰고
    # (allocator 가 `collect()` 합니다), 월별로 쪼갤 이유가 없습니다.
    result = SparkParquetLoader(args.output_path).write(travel_times)
    logger.info("구역쌍 이동시간 적재 완료: %s (%d쌍)", result.location, result.row_count)
    return result


if __name__ == "__main__":
    main()
