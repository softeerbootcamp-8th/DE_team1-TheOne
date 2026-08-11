"""Lyft 배차 가능 차량 목록을 Bronze에서 Silver로 변환합니다."""

import json
import os

from pipeline_core.pipeline import Pipeline

from ..common.logging_setup import configure_lambda_logging
from .extractor import LyftEligibleVehiclesBronzeExtractor
from .loader import LyftEligibleVehiclesSilverLoader
from .transformer import LyftEligibleVehiclesSilverTransformer

configure_lambda_logging()


def lambda_handler(event: dict | None = None, context=None) -> dict:
    event = event or {}
    collected_date = event.get("collected_date") or os.getenv("COLLECTED_DATE")
    if not collected_date:
        raise ValueError("collected_date 또는 COLLECTED_DATE가 필요합니다.")

    bronze_dir = event.get("bronze_dir") or os.getenv("BRONZE_DIR", "data/bronze")
    silver_dir = event.get("silver_dir") or os.getenv("SILVER_DIR", "data/silver")

    loader = LyftEligibleVehiclesSilverLoader(
        silver_dir, expect_collected_date=collected_date
    )
    result = Pipeline(
        LyftEligibleVehiclesBronzeExtractor(bronze_dir, collected_date),
        loader,
        transformer=LyftEligibleVehiclesSilverTransformer(),
    ).run()

    return {
        "row_count": result.write_result.row_count,
        "locations": loader.paths,
        "collected_date": collected_date,
    }


if __name__ == "__main__":
    print(json.dumps(lambda_handler(), ensure_ascii=False, indent=2))
