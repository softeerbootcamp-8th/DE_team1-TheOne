"""기사 차량 월별 스냅샷 Bronze 를 정제해 Silver 에 적재합니다."""

import json
import os

from pipeline_core.pipeline import Pipeline

from shared.aws_lambda.common.logging_setup import configure_lambda_logging
from main.aws_lambda.common.monthly_dataset import YEAR_MONTH_PATTERN
from .extractor import DriverVehicleMonthlySnapshotBronzeExtractor
from .loader import DATASET, build_silver_loader
from .transformer import DriverVehicleMonthlySnapshotSilverTransformer


configure_lambda_logging()


def lambda_handler(event: dict | None = None, context=None) -> dict:
    event = event or {}
    bronze_path = event.get("bronze_path")
    if not bronze_path:
        raise ValueError("bronze_path가 누락되었습니다")
    year_month = str(event.get("year_month") or "")
    if not YEAR_MONTH_PATTERN.fullmatch(year_month):
        raise ValueError("year_month가 YYYY-MM 형식이 아닙니다")
    silver_file_name = str(event.get("silver_file_name") or "")
    silver_dir = event.get("silver_dir") or os.getenv(
        "DRIVER_VEHICLE_MONTHLY_SNAPSHOT_SILVER_DIR", f"data/silver/{DATASET}"
    )
    storage = event.get("storage") or os.getenv("BRONZE_STORAGE", "local")
    bucket = event.get("bucket") or os.getenv("DATA_LAKE_S3_BUCKET")
    dry_run = event.get("dry_run", False)
    if not isinstance(dry_run, bool):
        raise ValueError("dry_run은 boolean이어야 합니다")

    loader = build_silver_loader(
        storage,
        silver_dir,
        bucket,
        year_month,
        silver_file_name,
        dry_run=dry_run,
    )
    result = Pipeline(
        DriverVehicleMonthlySnapshotBronzeExtractor(bronze_path),
        loader,
        transformer=DriverVehicleMonthlySnapshotSilverTransformer(),
    ).run()
    response = {
        "row_count": result.write_result.row_count,
        "locations": [result.write_result.location],
        "year_month": year_month,
        "silver_file_name": silver_file_name,
    }
    if dry_run:
        response["dry_run"] = True
    return response


if __name__ == "__main__":
    print(json.dumps(lambda_handler(), ensure_ascii=False, indent=2))
