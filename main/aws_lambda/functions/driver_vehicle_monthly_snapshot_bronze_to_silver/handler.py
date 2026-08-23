"""기사 차량 월별 스냅샷 Bronze 를 정제해 Silver 에 적재합니다."""

import json
import os

from pipeline_core.pipeline import Pipeline

from shared.aws_lambda.common.logging_setup import configure_lambda_logging
from main.aws_lambda.common.monthly_dataset import YEAR_MONTH_PATTERN
from .extractor import build_bronze_extractor
from .loader import build_silver_loader
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
    silver_output_path = str(event.get("silver_output_path") or "")
    if not silver_output_path:
        raise ValueError("silver_output_path가 필요합니다")

    result = Pipeline(
        build_bronze_extractor(
            storage,
            bronze_dir,
            bucket,
            year_month,
            service_area=event.get("service_area"),
        ),
        build_silver_loader(
            storage,
            silver_output_path,
            bucket,
        ),
        transformer=DriverVehicleMonthlySnapshotSilverTransformer(),
    ).run()
    return {
        "row_count": result.write_result.row_count,
        "locations": [result.write_result.location],
        "year_month": year_month,
        "silver_output_path": silver_output_path,
    }


if __name__ == "__main__":
    print(json.dumps(lambda_handler(), ensure_ascii=False, indent=2))
