"""
input: hvfhv_driver_trip, vehicle_master, gas_ev_price (Silver)
output: driver_aggregation, driver_car_suggestion, monthly_report (Gold)

사용 예:
    cd spark && PYTHONPATH="$(pwd):$(pwd)/.." uv run --frozen python jobs/silver_to_gold/job.py \\
      --trips_path ../data/silver/hvfhv_driver_trip/year_month=2026-01 \\
      --vehicle_master_path ../data/silver/vehicle_master/collected_date=2026-01-15/city=new-york/vehicle_master.parquet \\
      --gas_ev_price_path ../data/silver/gas_ev_price/year_month=2026-01/gas_ev_price.parquet \\
      --year 2026 --month 1 --threshold_profit_increase 30 --output_dir ../data/gold
"""

import argparse
import calendar
import logging
from pathlib import Path

import pandas as pd
from pyspark.sql import DataFrame

from common.session import get_or_create_spark_session
from jobs.silver_to_gold.transformer import (
    build_driver_monthly_aggregation,
    build_monthly_report,
    build_monthly_vehicle_recommendation,
    enrich_trips_with_fuel_cost,
)

logger = logging.getLogger(__name__)

DATASETS = ("driver_aggregation", "driver_car_suggestion", "monthly_report")


def partition_value(path: str, key: str) -> str:
    """경로에서 `key=값` 파티션의 값을 꺼냅니다.

    입력 경로가 곧 그 데이터의 시점이라, 별도 컬럼 없이 여기서 계보를 읽습니다.
    규칙과 다른 경로를 넘겼으면 조용히 빈 값을 쓰지 않고 실패시킵니다.
    """
    for part in Path(path).parts:
        if part.startswith(f"{key}="):
            return part.removeprefix(f"{key}=")
    raise ValueError(f"경로에서 {key}= 파티션을 찾지 못했습니다: {path}")


def _write_csv(dataframe: pd.DataFrame, output_dir: str, dataset: str, year_month: str) -> Path:
    path = Path(output_dir) / dataset / f"year_month={year_month}" / f"{dataset}.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    dataframe.to_csv(path, index=False)
    return path


def main(args_list: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="HVFHV 기사 배정 Silver → Gold 3종 산출")
    parser.add_argument("--trips_path", required=True, help="hvfhv_driver_trip Silver 파티션 경로")
    parser.add_argument("--vehicle_master_path", required=True, help="vehicle_master Silver 파일 경로")
    parser.add_argument("--gas_ev_price_path", required=True, help="gas_ev_price Silver 파일 경로")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    parser.add_argument(
        "--threshold_profit_increase", type=float, required=True,
        help="차량 교체 추천 기준 순수익 증가액 (USD)",
    )
    parser.add_argument("--output_dir", default="data/gold")
    args = parser.parse_args(args_list)

    year_month = f"{args.year:04d}-{args.month:02d}"
    days_in_month = calendar.monthrange(args.year, args.month)[1]

    spark = get_or_create_spark_session("hvfhv_silver_to_gold")
    # 1. Silver 데이터 로드
    trips: DataFrame = spark.read.parquet(args.trips_path)
    if trips.isEmpty():
        raise ValueError(f"기사 배정 운행 이력이 0건입니다: {args.trips_path}")
    gas_ev_price: DataFrame = spark.read.parquet(args.gas_ev_price_path)
    vehicle_master: DataFrame = spark.read.parquet(args.vehicle_master_path)

    # 2. 운행 이력에 그날의 연료 가격 / 순수익 등을 붙인다.
    enriched: DataFrame = enrich_trips_with_fuel_cost(trips, gas_ev_price, vehicle_master).persist()
    driver_aggregation: DataFrame | None = None
    recommendation: DataFrame | None = None
    try:
        # 3. 기사별 월간 집계
        driver_aggregation = build_driver_monthly_aggregation(
            enriched, vehicle_master, year_month, days_in_month
        ).persist()
        # 4. 기사별 월간 차량 추천
        recommendation = build_monthly_vehicle_recommendation(
            enriched, vehicle_master, driver_aggregation, year_month, days_in_month,
        ).persist()
        # 5. 리스 업체 월간 보고서 작성 — 계보 두 값을 함께 싣는다.
        report: DataFrame = build_monthly_report(
            recommendation,
            year_month,
            args.threshold_profit_increase,
            vehicle_master_collected_date=partition_value(
                args.vehicle_master_path, "collected_date"
            ),
            gas_ev_price_month=partition_value(args.gas_ev_price_path, "year_month"),
        )

        outputs: dict[str, DataFrame] = {
            "driver_aggregation": driver_aggregation,
            "driver_car_suggestion": recommendation,
            "monthly_report": report,
        }
        for dataset, dataframe in outputs.items():
            path = _write_csv(dataframe.toPandas(), args.output_dir, dataset, year_month)
            logger.info("gold 적재 완료: dataset=%s path=%s", dataset, path)
    finally:
        enriched.unpersist()
        if driver_aggregation is not None:
            driver_aggregation.unpersist()
        if recommendation is not None:
            recommendation.unpersist()


if __name__ == "__main__":
    main()
