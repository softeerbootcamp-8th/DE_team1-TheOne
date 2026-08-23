"""월별 택시 운행 데이터를 Bronze에 적재합니다."""

import json
import os

from pipeline_core.pipeline import Pipeline

from shared.aws_lambda.common.logging_setup import configure_lambda_logging
from main.aws_lambda.common.monthly_dataset import requested_year_month
from .extractor import MonthlyTaxiTripExtractor
from .loader import build_loader


configure_lambda_logging()


def lambda_handler(event: dict | None = None, context=None) -> dict:
    event = event or {}
    api_base_url = event.get("api_base_url") or os.getenv("SOURCE_API_URL")
    if not api_base_url:
        raise ValueError("api_base_url이 누락되었습니다")
    base_dir = event.get("base_dir") or os.getenv("BRONZE_DIR", "data/bronze")
    storage = event.get("storage") or os.getenv("BRONZE_STORAGE", "local")
    bucket = event.get("bucket") or os.getenv("DATA_LAKE_S3_BUCKET")
    loader = build_loader(
        storage, base_dir, bucket=bucket, service_area=event.get("service_area")
    )
    result = Pipeline(
        MonthlyTaxiTripExtractor(api_base_url, requested_year_month(event)), loader
    ).run()
    payload = loader.payload
    return {
        "year_month": payload["year_month"],
        "collected_at": payload["collected_at"],
        "year": payload["year_month"][:4],
        "month": payload["year_month"][5:],
        "row_count": result.write_result.row_count,
        "source_changed": loader.source_changed,
        "locations": [result.write_result.location],
        "file_size_bytes": len(payload["content"]),
    }


if __name__ == "__main__":
    print(json.dumps(lambda_handler(), ensure_ascii=False, indent=2))
