"""원천 Silver 4종을 직접 읽어 Gold 3종을 만듭니다.

input: hvfhv, driver_vehicle_monthly_snapshot, lease_vehicle_inventory,
       gas_ev_price (Silver)
output: driver_aggregation, driver_car_suggestion, monthly_report (Gold)

사용 예:
    cd main/spark && PYTHONPATH=../.. uv run --frozen python -m main.spark.jobs.silver_to_gold.job \
      --hvfhv_path ../data/silver/hvfhv/year_month=2026-01 \
      --driver_snapshot_path ../data/silver/driver_vehicle_monthly_snapshot/year_month=2026-01 \
      --inventory_path ../data/silver/lease_vehicle_inventory/year_month=2026-01 \
      --fuel_price_path ../data/silver/gas_ev_price/year_month=2026-01/gas_ev_price.parquet \
      --year 2026 --month 1 --threshold_profit_increase 600 --output_dir ../data/gold
"""

import argparse
import logging
from pathlib import Path
from uuid import uuid4

import pandas as pd
from pyspark.sql import DataFrame

from main.spark.jobs.silver_to_gold.transformer import (
    build_driver_monthly_aggregation,
    build_driver_monthly_profit,
    build_monthly_report,
    build_monthly_vehicle_recommendation,
    enrich_trips_with_fuel_cost,
    validate_gold_business_invariants,
)
from shared.spark.common.session import get_or_create_spark_session


logger = logging.getLogger(__name__)


def _csv_path(output_dir: str, dataset: str, year_month: str) -> Path:
    return Path(output_dir) / dataset / f"year_month={year_month}" / f"{dataset}.csv"


def _write_all_csv(
    frames: dict[str, pd.DataFrame], output_dir: str, year_month: str
) -> dict[str, Path]:
    """3종을 임시 파일에 모두 쓴 뒤 한꺼번에 교체합니다.

    예전에는 최종 경로에 바로, 그것도 `toPandas()` 와 섞어 순차로 썼습니다. 두 번째에서
    죽으면 첫 산출물은 이번 값, 세 번째는 **직전 실행 값**이 남았고 대시보드는 그 섞인
    상태를 그대로 읽었습니다 (#589).

    파일 하나의 원자성이 아니라 **3종의 일관성**이 목적이라 교체를 끝으로 모읍니다.
    `replace` 세 번 사이의 창은 남지만, 무거운 계산과 쓰기가 모두 끝난 뒤라 실패
    가능성이 사실상 사라집니다.
    """
    temporary: dict[str, Path] = {}
    try:
        for dataset, frame in frames.items():
            path = _csv_path(output_dir, dataset, year_month)
            path.parent.mkdir(parents=True, exist_ok=True)
            staged = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
            frame.to_csv(staged, index=False)
            temporary[dataset] = staged

        written: dict[str, Path] = {}
        for dataset, staged in temporary.items():
            path = _csv_path(output_dir, dataset, year_month)
            staged.replace(path)
            written[dataset] = path
        return written
    finally:
        for staged in temporary.values():
            staged.unlink(missing_ok=True)


def main(args_list: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="원천 Silver 4종 → Gold 3종 산출")
    parser.add_argument("--hvfhv_path", required=True, help="HVFHV Silver 월 파티션")
    parser.add_argument(
        "--driver_snapshot_path",
        required=True,
        help="기사 차량 월 스냅샷 Silver 파티션",
    )
    parser.add_argument(
        "--inventory_path", required=True, help="리스 업체 보유 차량 Silver 파티션"
    )
    parser.add_argument(
        "--fuel_price_path", required=True, help="통합 연료비 Silver 파일 또는 파티션"
    )
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    parser.add_argument(
        "--threshold_profit_increase",
        type=float,
        required=True,
        help="차량 교체 추천 기준 순수익 증가액 (USD)",
    )
    parser.add_argument("--output_dir", default="data/gold")
    args = parser.parse_args(args_list)

    year_month = f"{args.year:04d}-{args.month:02d}"
    spark = get_or_create_spark_session("hvfhv_silver_to_gold")
    hvfhv: DataFrame = spark.read.parquet(args.hvfhv_path)
    driver_snapshot: DataFrame = spark.read.parquet(args.driver_snapshot_path)
    inventory: DataFrame = spark.read.parquet(args.inventory_path)
    fuel_price: DataFrame = spark.read.parquet(args.fuel_price_path)

    enriched: DataFrame | None = None
    driver_metrics: DataFrame | None = None
    recommendation: DataFrame | None = None
    try:
        enriched = enrich_trips_with_fuel_cost(
            hvfhv,
            driver_snapshot,
            inventory,
            fuel_price,
            year_month,
        )
        driver_metrics = build_driver_monthly_aggregation(
            enriched, year_month
        ).persist()
        driver_profit = build_driver_monthly_profit(driver_metrics)
        recommendation = build_monthly_vehicle_recommendation(
            driver_metrics, inventory
        ).persist()
        validate_gold_business_invariants(
            driver_profit,
            recommendation,
            driver_snapshot,
            inventory,
        )
        report = build_monthly_report(
            recommendation,
            year_month,
            args.threshold_profit_increase,
        )

        outputs: dict[str, DataFrame] = {
            "driver_aggregation": driver_profit,
            "driver_car_suggestion": recommendation,
            "monthly_report": report,
        }
        # 무거운 `toPandas()` 를 먼저 끝냅니다. 교체 직전까지 디스크를 안 건드려야
        # 계산 중 실패가 기존 산출물을 남기지 않습니다.
        frames = {name: frame.toPandas() for name, frame in outputs.items()}
        for dataset, path in _write_all_csv(frames, args.output_dir, year_month).items():
            logger.info("gold 적재 완료: dataset=%s path=%s", dataset, path)
    finally:
        if enriched is not None:
            enriched.unpersist()
        if driver_metrics is not None:
            driver_metrics.unpersist()
        if recommendation is not None:
            recommendation.unpersist()


if __name__ == "__main__":
    main()
