"""기사 차량 월별 스냅샷 Bronze 를 정제해 Silver 에 적재합니다."""

import json
import os

from pipeline_core.pipeline import Pipeline

from shared.aws_lambda.common.logging_setup import configure_lambda_logging
from main.aws_lambda.common.monthly_dataset import (
    TIMESTAMP_FILE_PATTERN,
    YEAR_MONTH_PATTERN,
)
from .extractor import build_bronze_extractor
from .loader import DATASET, build_silver_loader
from .transformer import DriverVehicleMonthlySnapshotSilverTransformer


configure_lambda_logging()


def lambda_handler(event: dict | None = None, context=None) -> dict:
    event = event or {}
    year_month = str(event.get("year_month") or "")
    if not YEAR_MONTH_PATTERN.fullmatch(year_month):
        raise ValueError("year_month가 YYYY-MM 형식이 아닙니다")
    storage = event.get("storage") or os.getenv("BRONZE_STORAGE", "local")
    bucket = event.get("bucket") or os.getenv("DATA_LAKE_S3_BUCKET")
    bronze_dir = event.get("bronze_dir") or os.getenv("BRONZE_DIR", "data/bronze")
    silver_file_name = str(event.get("silver_file_name") or "")
    if not TIMESTAMP_FILE_PATTERN.fullmatch(silver_file_name):
        raise ValueError("silver_file_name이 수집 시각 Parquet 형식이 아닙니다")
    silver_dir = event.get("silver_dir") or os.getenv(
        "DRIVER_VEHICLE_MONTHLY_SNAPSHOT_SILVER_DIR", f"data/silver/{DATASET}"
    )

    result = Pipeline(
        build_bronze_extractor(storage, bronze_dir, bucket, year_month),
        build_silver_loader(
            storage, silver_dir, bucket, year_month, silver_file_name
        ),
        transformer=DriverVehicleMonthlySnapshotSilverTransformer(),
    ).run()
    return {
        "row_count": result.write_result.row_count,
        "locations": [result.write_result.location],
        "year_month": year_month,
        "silver_file_name": silver_file_name,
    }


if __name__ == "__main__":
    print(json.dumps(lambda_handler(), ensure_ascii=False, indent=2))
