"""기사 계약 Bronze 를 정제해 Silver 에 적재합니다."""

import json
import os

from pipeline_core.pipeline import Pipeline

from shared.aws_lambda.common.logging_setup import configure_lambda_logging
from main.aws_lambda.common.monthly_dataset import YEAR_MONTH_PATTERN
from .extractor import DriverVehicleLeaseBronzeExtractor
from .loader import DATASET, DriverVehicleLeaseSilverLoader
from .transformer import DriverVehicleLeaseSilverTransformer


configure_lambda_logging()


def lambda_handler(event: dict | None = None, context=None) -> dict:
    event = event or {}
    bronze_path = event.get("bronze_path")
    if not bronze_path:
        raise ValueError("bronze_path가 누락되었습니다")
    year_month = str(event.get("year_month") or "")
    if not YEAR_MONTH_PATTERN.fullmatch(year_month):
        raise ValueError("year_month가 YYYY-MM 형식이 아닙니다")
    silver_dir = event.get("silver_dir") or os.getenv(
        "DRIVER_MASTER_SILVER_DIR", f"data/silver/{DATASET}"
    )

    loader = DriverVehicleLeaseSilverLoader(silver_dir, year_month)
    result = Pipeline(
        DriverVehicleLeaseBronzeExtractor(bronze_path),
        loader,
        transformer=DriverVehicleLeaseSilverTransformer(),
    ).run()
    return {
        "row_count": result.write_result.row_count,
        "locations": [result.write_result.location],
        "year_month": year_month,
    }


if __name__ == "__main__":
    print(json.dumps(lambda_handler(), ensure_ascii=False, indent=2))
