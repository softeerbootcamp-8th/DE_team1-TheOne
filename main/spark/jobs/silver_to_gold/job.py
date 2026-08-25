"""원천 Silver 4종을 직접 읽어 Gold 3종을 만듭니다.

input: monthly_taxi_trip, driver_vehicle_monthly_snapshot, lease_vehicle_inventory,
       fuel_price (Silver)
output: driver_aggregation, driver_car_suggestion, silver_lineage (Gold)

사용 예 (로컬):
    cd main/spark && PYTHONPATH=../.. uv run --frozen python -m main.spark.jobs.silver_to_gold.job \
      --year 2026 --month 1 --service_area NYC

사용 예 (S3, --env prod):
    cd main/spark && PYTHONPATH=../.. uv run --frozen python -m main.spark.jobs.silver_to_gold.job \
      --env prod --bucket de-theone \
      --year 2026 --month 1 --service_area NYC --output_dir ../data/gold

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

from main.spark.jobs.silver_to_gold.postgres_loader import write_gold_to_postgres
from main.spark.jobs.service_area_path import (
    gold_csv_path,
    service_area_prefix,
    service_area_root,
)
from main.spark.jobs.silver_to_gold.transformer import (
    build_driver_monthly_aggregation,
    build_driver_monthly_profit,
    build_monthly_vehicle_recommendation,
    enrich_trips_with_fuel_cost,
    validate_gold_business_invariants,
)
from shared.common.s3_reader import list_keys
from main.common.eia_fuel_version import FUEL_FILE_NAME, fuel_source_tokens
from shared.common.success_marker import data_key_is_complete, marker_path
from main.spark.jobs.silver_to_gold.monthly_silver import (
    latest_local_silver_version,
    latest_s3_silver_version,
)
from shared.spark.common.session import get_or_create_spark_session


logger = logging.getLogger(__name__)

CURRENT_FILE = Path(__file__).resolve()
# spark/jobs/silver_to_gold/job.py -> project root
PROJECT_ROOT = CURRENT_FILE.parents[4]

# 이 경로 뒤에 service_area=<area>/year_month=<ym> 을 붙여서 씁니다.
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


def latest_partition_file(
    base_path: str, year_month: str, service_area: str
) -> str:
    """`year_month=` 파티션 안의 최신 버전 파일 하나.

    monthly_taxi_trip·driver_vehicle_monthly_snapshot·lease_vehicle_inventory는
    fuel_price와 달리 매번 그달 스냅샷 전체가 새 버전으로 통째로 쌓입니다. 파티션
    디렉터리째 `spark.read.parquet()`에 넘기면 과거 버전이 다 합쳐져 조인 키
    (driver_id/taxi_id 등)가 중복됩니다 — 로컬 모드는 DAG(tasks.py)가 미리 최신
    파일 경로를 골라 넘겨서 안 걸렸지만, `--env prod`는 이 파티션 디렉터리를
    그대로 읽어 실제로 걸렸습니다 (#759).
    """
    resolved = resolve_path(base_path)
    if _is_s3_path(resolved):
        scheme = resolved.split("://", 1)[0]
        parsed = urlsplit(resolved)
        bucket = parsed.netloc
        base_key = parsed.path.lstrip("/").rstrip("/")
        area_prefix = service_area_prefix(base_key, service_area=service_area)
        prefix = f"{area_prefix}/year_month={year_month}/"
        keys = list_keys(bucket, prefix)
        versioned = latest_s3_silver_version(keys, prefix)
        if versioned is not None:
            return f"{scheme}://{bucket}/{versioned}"
        raise FileNotFoundError(
            f"Silver 파티션이 없습니다: {scheme}://{bucket}/{prefix}"
        )

    root = service_area_root(resolved, service_area)
    partition_dir = root / f"year_month={year_month}"
    if partition_dir.is_dir():
        versioned = latest_local_silver_version(partition_dir)
        if versioned is not None:
            return str(versioned)
    raise FileNotFoundError(f"Silver 파티션이 없습니다: {partition_dir}")


def monthly_fuel_price_path(
    fuel_price_dir: str, year_month: str, service_area: str
) -> str:
    """대상 지역·월에서 완료된 최신 연료비 `ny_fuel.parquet` 경로."""
    if _is_s3_path(fuel_price_dir):
        scheme = fuel_price_dir.split("://", 1)[0]
        parsed = urlsplit(fuel_price_dir)
        bucket = parsed.netloc
        base_key = parsed.path.lstrip("/").rstrip("/")
        area_prefix = service_area_prefix(base_key, service_area=service_area)
        partition_prefix = f"{area_prefix}/year_month={year_month}"
        keys = set(list_keys(bucket, f"{partition_prefix}/"))
        candidates = []
        for key in keys:
            relative = key.removeprefix(f"{partition_prefix}/")
            parts = relative.split("/")
            if len(parts) != 2 or parts[1] != FUEL_FILE_NAME:
                continue
            source_tokens = fuel_source_tokens(parts[0])
            if source_tokens and data_key_is_complete(key, keys):
                candidates.append((*source_tokens, key))
        if candidates:
            return f"{scheme}://{bucket}/{max(candidates)[-1]}"
        raise FileNotFoundError(
            f"연료비 Silver 완료본이 없습니다: "
            f"{scheme}://{bucket}/{partition_prefix}/"
        )

    root = service_area_root(fuel_price_dir, service_area)
    partition = root / f"year_month={year_month}"
    candidates = []
    for version in partition.glob("input_version=*"):
        source_tokens = fuel_source_tokens(version.name)
        path = version / FUEL_FILE_NAME
        if source_tokens and path.is_file() and marker_path(version).is_file():
            candidates.append((*source_tokens, path))
    if candidates:
        return str(max(candidates)[-1])
    raise FileNotFoundError(f"연료비 Silver 완료본이 없습니다: {partition}")


def _csv_path(
    output_dir: str, dataset: str, year_month: str, service_area: str
) -> Path:
    """Airflow 검증과 같은 공용 규칙으로 지역별 Gold 경로를 만듭니다."""
    return gold_csv_path(output_dir, dataset, year_month, service_area)


def _write_all_csv(
    frames: dict[str, pd.DataFrame],
    output_dir: str,
    year_month: str,
    service_area: str,
) -> dict[str, Path]:
    """2종을 임시 파일에 모두 쓴 뒤 한꺼번에 교체합니다.

    예전에는 최종 경로에 바로, 그것도 `toPandas()` 와 섞어 순차로 썼습니다. 두 번째에서
    죽으면 첫 산출물은 이번 값, 세 번째는 **직전 실행 값**이 남았고 대시보드는 그 섞인
    상태를 그대로 읽었습니다 (#589).

    파일 하나의 원자성이 아니라 **2종의 일관성**이 목적이라 교체를 끝으로 모읍니다.
    `replace` 세 번 사이의 창은 남지만, 무거운 계산과 쓰기가 모두 끝난 뒤라 실패
    가능성이 사실상 사라집니다.
    """
    temporary: dict[str, Path] = {}
    try:
        for dataset, frame in frames.items():
            path = _csv_path(output_dir, dataset, year_month, service_area)
            path.parent.mkdir(parents=True, exist_ok=True)
            staged = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
            frame.to_csv(staged, index=False)
            temporary[dataset] = staged

        written: dict[str, Path] = {}
        for dataset, staged in temporary.items():
            path = _csv_path(output_dir, dataset, year_month, service_area)
            staged.replace(path)
            written[dataset] = path
        return written
    finally:
        for staged in temporary.values():
            staged.unlink(missing_ok=True)


def main(args_list: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="원천 Silver 4종 → Gold 2종 산출")
    parser.add_argument(
        "--env", choices=["local", "prod"], default=os.getenv("SPARK_JOB_ENV", "local"),
        help="local이면 로컬 폴더, prod면 S3에서 읽음 (기본 SPARK_JOB_ENV 환경변수, 없으면 local)",
    )
    parser.add_argument(
        "--bucket", default=os.getenv("DATA_LAKE_S3_BUCKET"),
        help="--env prod일 때 쓸 S3 버킷 (기본 DATA_LAKE_S3_BUCKET 환경변수)",
    )
    parser.add_argument(
        "--enable_s3",
        default=False,
        type=lambda value: str(value).lower() == "true",
        help=(
            "로컬 pyspark에 hadoop-aws를 얹어 --env prod의 s3:// 를 직접 읽음. "
            "EMR 제출 시에는 이미 세션이 있어 무시됨(#712)"
        ),
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
            "통합 연료비 Silver Parquet 파일. 비우면 --env 기본 경로에서 "
            "--year/--month 파티션을 읽음"
        ),
    )
    parser.add_argument("--year", type=int, required=True)
    # 지역은 Airflow asset 파티션 키("{service_area}:{year_month}")에서 옵니다(#674).
    # driver_id 가 지역 간 유니크하지 않으므로(#805) Gold 자연 키의 일부입니다.
    parser.add_argument("--service_area", required=True)
    parser.add_argument("--month", type=int, required=True)
    parser.add_argument("--output_dir", default="data/gold")
    parser.add_argument(
        "--gold_dsn", default=os.getenv("GOLD_DATABASE_URL"),
        help="--env prod일 때 Gold 2종을 적재할 PostgreSQL DSN (기본 GOLD_DATABASE_URL 환경변수)",
    )
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
        given = given_paths[dataset]
        if given is not None:
            return resolve_path(given)
        return latest_partition_file(
            base_paths[dataset], year_month, args.service_area
        )

    monthly_taxi_trip_path = _monthly_path("monthly_taxi_trip")
    driver_vehicle_monthly_snapshot_path = _monthly_path("driver_vehicle_monthly_snapshot")
    lease_vehicle_inventory_path = _monthly_path("lease_vehicle_inventory")
    fuel_price_path = (
        resolve_path(given_paths["fuel_price"])
        if given_paths["fuel_price"] is not None
        else monthly_fuel_price_path(
            base_paths["fuel_price"], year_month, args.service_area
        )
    )

    spark = get_or_create_spark_session(
        "monthly_taxi_trip_silver_to_gold", enable_s3=args.enable_s3
    )
    monthly_taxi_trip: DataFrame = spark.read.parquet(monthly_taxi_trip_path)
    driver_snapshot: DataFrame = spark.read.parquet(driver_vehicle_monthly_snapshot_path)
    inventory: DataFrame = spark.read.parquet(lease_vehicle_inventory_path)
    fuel_price: DataFrame = spark.read.parquet(fuel_price_path)

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
            enriched, year_month, args.service_area
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

        outputs: dict[str, DataFrame] = {
            "driver_aggregation": driver_profit,
            "driver_car_suggestion": recommendation,
        }
        # 무거운 `toPandas()` 를 먼저 끝냅니다. 교체 직전까지 디스크를 안 건드려야
        # 계산 중 실패가 기존 산출물을 남기지 않습니다.
        frames = {name: frame.toPandas() for name, frame in outputs.items()}
        # driver_aggregation·driver_car_suggestion 은 같은 실행에서 같은 Silver 4종을
        # 함께 읽으므로, 행마다 경로를 반복하는 대신 실행당 한 행으로 따로 적재한다.
        frames["silver_lineage"] = pd.DataFrame([{
            "service_area": args.service_area,
            "year_month": year_month,
            "silver_monthly_taxi_trip_s3_link": monthly_taxi_trip_path,
            "silver_driver_vehicle_monthly_snapshot_s3_link": driver_vehicle_monthly_snapshot_path,
            "silver_lease_vehicle_inventory_s3_link": lease_vehicle_inventory_path,
            "silver_gas_ev_price_s3_link": fuel_price_path,
        }])
        if args.env == "prod":
            if not args.gold_dsn:
                raise ValueError(
                    "--env prod는 --gold_dsn(또는 GOLD_DATABASE_URL 환경변수)이 필요합니다"
                )
            written = write_gold_to_postgres(
                frames, args.gold_dsn, args.service_area, year_month
            )
            for dataset, rows in written.items():
                logger.info("gold 적재 완료: dataset=%s rows=%d", dataset, rows)
        else:
            for dataset, path in _write_all_csv(
                frames, args.output_dir, year_month, args.service_area
            ).items():
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
