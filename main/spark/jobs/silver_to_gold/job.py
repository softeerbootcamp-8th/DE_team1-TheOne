"""원천 Silver 4종을 직접 읽어 Gold 3종을 만듭니다.

input: monthly_taxi_trip, driver_vehicle_monthly_snapshot, lease_vehicle_inventory,
       fuel_price (Silver)
output: driver_aggregation, driver_car_suggestion, monthly_report (Gold)

사용 예 (로컬):
    cd main/spark && PYTHONPATH=../.. uv run --frozen python -m main.spark.jobs.silver_to_gold.job \
      --year 2026 --month 1 --threshold_profit_increase 600

사용 예 (S3, --env prod):
    cd main/spark && PYTHONPATH=../.. uv run --frozen python -m main.spark.jobs.silver_to_gold.job \
      --env prod --bucket de-theone \
      --year 2026 --month 1 --threshold_profit_increase 600 --output_dir ../data/gold

`--*_path` 4개를 직접 주면 `--env` 기본 경로 대신 그 값을 그대로 씁니다.
"""

import argparse
import logging
import os
from pathlib import Path
from urllib.parse import urlsplit
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
from shared.common.s3_reader import list_keys
from shared.spark.common.session import get_or_create_spark_session


logger = logging.getLogger(__name__)

CURRENT_FILE = Path(__file__).resolve()
# spark/jobs/silver_to_gold/job.py -> project root
PROJECT_ROOT = CURRENT_FILE.parents[4]

# 월별 3종은 이 경로 뒤에 year_month=<ym> 을 붙여서 씁니다. fuel_price는 누적
# 파일이라 베이스 디렉터리 자체가 최종 경로입니다(latest_fuel_price_path가
# 최신 파티션을 찾음).
DEFAULT_LOCAL_SILVER_BASE = {
    "monthly_taxi_trip": "data/silver/monthly_taxi_trip",
    "driver_vehicle_monthly_snapshot": "data/silver/driver_vehicle_monthly_snapshot",
    "lease_vehicle_inventory": "data/silver/lease_vehicle_inventory",
    "fuel_price": "data/silver/gas_ev_price",
}


def _is_s3_path(path: str) -> bool:
    return path.startswith("s3://") or path.startswith("s3a://")


def resolve_path(path_str: str) -> str:
    if _is_s3_path(path_str):
        return path_str
    path = Path(path_str)
    if not path.is_absolute():
        return str(PROJECT_ROOT / path)
    return str(path)


def default_input_base_paths(env: str, bucket: str | None) -> dict[str, str]:
    """`--env`로 Silver 4종의 기본 베이스 경로를 고릅니다. `--*_path` 로 덮어쓸 수 있습니다."""
    if env == "local":
        return dict(DEFAULT_LOCAL_SILVER_BASE)
    if env == "prod":
        if not bucket:
            raise ValueError("--env prod는 --bucket(또는 DATA_LAKE_S3_BUCKET 환경변수)이 필요합니다")
        return {
            dataset: f"s3://{bucket}/{local_path.split('data/', 1)[1]}"
            for dataset, local_path in DEFAULT_LOCAL_SILVER_BASE.items()
        }
    raise ValueError(f"알 수 없는 --env: {env!r} (local 또는 prod)")


def latest_fuel_price_path(fuel_price_dir: str) -> str:
    """`fuel_price_dir` 아래 가장 최근 `year_month=` 파티션의 파일 경로.

    연료비 Silver는 파티션마다 그 시점까지의 과거 일별 가격을 전부 담고 있어
    (`eia_fuel_price_silver` 파이프라인 참고), 가장 최근 파티션 하나만 읽으면
    대상 월의 날짜도 포함됩니다 — 다른 3종처럼 `--year`/`--month` 로 맞춰 넘길
    필요가 없습니다.
    """
    if _is_s3_path(fuel_price_dir):
        scheme = fuel_price_dir.split("://", 1)[0]
        parsed = urlsplit(fuel_price_dir)
        bucket = parsed.netloc
        prefix = parsed.path.lstrip("/").rstrip("/") + "/"
        keys = [key for key in list_keys(bucket, prefix) if key.endswith(".parquet")]
        if not keys:
            raise FileNotFoundError(f"연료비 Silver 파티션이 없습니다: {fuel_price_dir}")
        return f"{scheme}://{bucket}/{max(keys)}"

    partitions = sorted(p for p in Path(fuel_price_dir).glob("year_month=*") if p.is_dir())
    if not partitions:
        raise FileNotFoundError(f"연료비 Silver 파티션이 없습니다: {fuel_price_dir}")
    parquet_files = sorted(partitions[-1].glob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"연료비 Silver 파티션이 비어 있습니다: {partitions[-1]}")
    return str(parquet_files[-1])


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
    parser.add_argument(
        "--env", choices=["local", "prod"], default=os.getenv("SPARK_JOB_ENV", "local"),
        help="local이면 로컬 폴더, prod면 S3에서 읽음 (기본 SPARK_JOB_ENV 환경변수, 없으면 local)",
    )
    parser.add_argument(
        "--bucket", default=os.getenv("DATA_LAKE_S3_BUCKET"),
        help="--env prod일 때 쓸 S3 버킷 (기본 DATA_LAKE_S3_BUCKET 환경변수)",
    )
    parser.add_argument(
        "--monthly_taxi_trip_path", default=None,
        help="월별 택시 운행 기록 Silver 파티션. 비우면 --env 기본 경로",
    )
    parser.add_argument(
        "--driver_vehicle_monthly_snapshot_path", default=None,
        help="기사 차량 월 스냅샷 Silver 파티션. 비우면 --env 기본 경로",
    )
    parser.add_argument(
        "--lease_vehicle_inventory_path", default=None,
        help="리스 업체 보유 차량 Silver 파티션. 비우면 --env 기본 경로",
    )
    parser.add_argument(
        "--fuel_price_path", default=None,
        help=(
            "통합 연료비 Silver 베이스 디렉터리. 비우면 --env 기본 경로 — "
            "--year/--month 와 무관하게 가장 최근 파티션을 읽음"
        ),
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

    given_paths = {
        "monthly_taxi_trip": args.monthly_taxi_trip_path,
        "driver_vehicle_monthly_snapshot": args.driver_vehicle_monthly_snapshot_path,
        "lease_vehicle_inventory": args.lease_vehicle_inventory_path,
        "fuel_price": args.fuel_price_path,
    }
    base_paths = (
        default_input_base_paths(args.env, args.bucket)
        if any(path is None for path in given_paths.values())
        else {}
    )

    def _monthly_path(dataset: str) -> str:
        base = given_paths[dataset] or f"{base_paths[dataset]}/year_month={year_month}"
        return resolve_path(base)

    monthly_taxi_trip_path = _monthly_path("monthly_taxi_trip")
    driver_vehicle_monthly_snapshot_path = _monthly_path("driver_vehicle_monthly_snapshot")
    lease_vehicle_inventory_path = _monthly_path("lease_vehicle_inventory")
    fuel_price_path = resolve_path(given_paths["fuel_price"] or base_paths["fuel_price"])

    spark = get_or_create_spark_session("hvfhv_silver_to_gold")
    monthly_taxi_trip: DataFrame = spark.read.parquet(monthly_taxi_trip_path)
    driver_snapshot: DataFrame = spark.read.parquet(driver_vehicle_monthly_snapshot_path)
    inventory: DataFrame = spark.read.parquet(lease_vehicle_inventory_path)
    fuel_price: DataFrame = spark.read.parquet(latest_fuel_price_path(fuel_price_path))

    enriched: DataFrame | None = None
    driver_metrics: DataFrame | None = None
    recommendation: DataFrame | None = None
    try:
        enriched = enrich_trips_with_fuel_cost(
            monthly_taxi_trip,
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
