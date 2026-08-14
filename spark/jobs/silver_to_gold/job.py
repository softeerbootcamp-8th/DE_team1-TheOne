"""HVFHV 기사 배정 Silver → Gold 3종 산출 CLI.

입력 경로는 이미 해당 월/스냅샷으로 해석된 구체 경로를 받는다(파티션 해석은 호출부 책임 —
``jobs/driver_assignment/silver_job.py`` 와 같은 관례). DAG 연동은 이번 범위가 아니라
아직 없다.

사용 예:
    cd spark && PYTHONPATH="$(pwd):$(pwd)/.." uv run --frozen python jobs/silver_to_gold/job.py \\
      --trips_path ../data/silver/hvfhv_driver_trip/year_month=2026-01 \\
      --vehicle_master_path ../data/silver/vehicle_master/collected_date=2026-01-15/city=new-york/vehicle_master.parquet \\
      --gas_ev_price_path ../data/silver/gas_ev_price/collected_month=2026-01/gas_ev_price.parquet \\
      --year 2026 --month 1 --threshold_profit_increase 30 --output_dir ../data/gold
"""

import argparse
import calendar
import logging
from pathlib import Path

import pandas as pd

from common.session import get_or_create_spark_session
from jobs.silver_to_gold.transformer import (
    build_driver_monthly_aggregation,
    build_monthly_report,
    build_monthly_vehicle_recommendation,
    enrich_trips_with_fuel_cost,
)

logger = logging.getLogger(__name__)

DATASETS = ("driver_aggregation", "driver_car_suggestion", "monthly_report")


def _write_csv(dataframe: pd.DataFrame, output_dir: str, dataset: str, year_month: str) -> Path:
    path = Path(output_dir) / dataset / f"year_month={year_month}" / f"{dataset}.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    dataframe.to_csv(path, index=False)
    return path


def main(args_list: list[str] | None = None):
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
    trips = spark.read.parquet(args.trips_path)
    if trips.isEmpty():
        raise ValueError(f"기사 배정 운행 이력이 0건입니다: {args.trips_path}")
    gas_ev_price = spark.read.parquet(args.gas_ev_price_path)
    vehicle_master = spark.read.parquet(args.vehicle_master_path)

    enriched = enrich_trips_with_fuel_cost(trips, gas_ev_price, vehicle_master).persist()
    driver_aggregation = recommendation = None
    try:
        driver_aggregation = build_driver_monthly_aggregation(
            enriched, vehicle_master, year_month, days_in_month
        ).persist()
        recommendation = build_monthly_vehicle_recommendation(
            enriched, vehicle_master, driver_aggregation, year_month, days_in_month,
        ).persist()
        report = build_monthly_report(recommendation, year_month, args.threshold_profit_increase)

        outputs = {
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
