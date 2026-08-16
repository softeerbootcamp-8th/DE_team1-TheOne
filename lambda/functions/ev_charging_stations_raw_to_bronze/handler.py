"""NLR 전기차 충전소 수집과 Bronze 적재를 실행합니다."""

import os
from datetime import datetime, timezone

from pipeline_core.pipeline import Pipeline

from ..common.env import load_local_env
from ..common.logging_setup import configure_lambda_logging
from .extractor import FUEL_TYPE_CODE, STATE, EvChargingStationExtractor
from .loader import build_bronze_loader

configure_lambda_logging()


def lambda_handler(event: dict | None = None, context=None) -> dict:
    load_local_env()
    event = event or {}
    api_key = os.getenv("NLR_API_KEY", "").strip()
    if not api_key:
        raise ValueError("NLR_API_KEY 환경변수가 필요합니다.")

    base_dir = event.get("base_dir") or os.getenv("BRONZE_DIR", "data/bronze")
    storage = event.get("storage") or os.getenv("BRONZE_STORAGE", "local")
    bucket = event.get("bucket") or os.getenv("DATA_LAKE_S3_BUCKET")
    collected_at = datetime.now(timezone.utc)

    result = Pipeline(
        EvChargingStationExtractor(api_key),
        build_bronze_loader(storage, base_dir, collected_at, bucket=bucket),
    ).run()

    return {
        "row_count": result.write_result.row_count,
        "locations": [result.write_result.location],
        "state": STATE,
        "fuel_type_code": FUEL_TYPE_CODE,
        "collected_date": f"{collected_at:%Y-%m-%d}",
    }
