"""Uber Eligible Vehicles 수집/적재 Lambda 핸들러.

Extractor(수집) 와 Loader(적재) 를 Pipeline 으로 이어붙이기만 합니다.
"""

import json
import os

from pipeline_core.pipeline import Pipeline

from shared.aws_lambda.common.collected_at import resolve_collected_at
from shared.aws_lambda.common.logging_setup import configure_lambda_logging
from .extractor import UberEligibleVehiclesExtractor
from .loader import build_raw_loader

configure_lambda_logging()


def lambda_handler(event: dict | None = None, context=None) -> dict:
    event = event or {}
    city_slug = event.get("city_slug") or os.getenv("CITY_SLUG", "new-york")
    base_dir = event.get("base_dir") or os.getenv("RAW_DIR", "data/source/raw")
    storage = event.get("storage") or os.getenv("RAW_STORAGE", "local")
    bucket = event.get("bucket") or os.getenv("DATA_LAKE_S3_BUCKET")
    collected_at = resolve_collected_at(event)

    result = Pipeline(
        UberEligibleVehiclesExtractor(city_slug, collected_at),
        build_raw_loader(storage, base_dir, city_slug, collected_at, bucket=bucket),
    ).run()

    return {
        "row_count": result.write_result.row_count,
        "locations": [result.write_result.location],
        "city_slug": city_slug,
        # Curated 배치가 이 하루치 파티션을 읽습니다 (Raw 파티션 키와 동일).
        "collected_date": f"{collected_at:%Y-%m-%d}",
    }


if __name__ == "__main__":
    print(json.dumps(lambda_handler(), ensure_ascii=False, indent=2))
