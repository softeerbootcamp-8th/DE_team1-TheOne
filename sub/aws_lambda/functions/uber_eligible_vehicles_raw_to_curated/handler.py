"""Uber 배차 가능 차량 목록 Raw 를 Curated 로 변환합니다."""

import json
import os

from pipeline_core.pipeline import Pipeline

from shared.aws_lambda.common.logging_setup import configure_lambda_logging
from .extractor import build_raw_extractor
from .loader import build_curated_loader
from .transformer import UberEligibleVehiclesCuratedTransformer

configure_lambda_logging()


def lambda_handler(event: dict | None = None, context=None) -> dict:
    event = event or {}
    collected_date = event.get("collected_date") or os.getenv("COLLECTED_DATE")
    if not collected_date:
        raise ValueError("collected_date 또는 COLLECTED_DATE가 필요합니다.")

    storage = event.get("storage") or os.getenv("RAW_STORAGE", "local")
    raw_dir = event.get("raw_dir") or os.getenv("RAW_DIR", "data/source/raw")
    curated_dir = event.get("curated_dir") or os.getenv("CURATED_DIR", "data/source/curated")
    bucket = event.get("bucket") or os.getenv("DATA_LAKE_S3_BUCKET")

    loader = build_curated_loader(storage, curated_dir, collected_date, bucket=bucket)
    result = Pipeline(
        build_raw_extractor(storage, raw_dir, collected_date, bucket=bucket),
        loader,
        transformer=UberEligibleVehiclesCuratedTransformer(),
    ).run()

    return {
        "row_count": result.write_result.row_count,
        # 도시별로 파일 하나씩 씁니다 — 도시 수는 len(locations) 입니다.
        "locations": loader.paths,
        "collected_date": collected_date,
    }


if __name__ == "__main__":
    print(json.dumps(lambda_handler(), ensure_ascii=False, indent=2))
