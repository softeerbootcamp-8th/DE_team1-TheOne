"""Uber 배차 가능 차량 목록 Bronze 를 Silver 로 변환합니다."""

import json
import os

from pipeline_core.pipeline import Pipeline

from ..common.logging_setup import configure_lambda_logging
from .extractor import UberEligibleVehiclesBronzeExtractor
from .loader import UberEligibleVehiclesSilverLoader
from .transformer import UberEligibleVehiclesSilverTransformer

configure_lambda_logging()


def lambda_handler(event: dict | None = None, context=None) -> dict:
    event = event or {}
    collected_date = event.get("collected_date") or os.getenv("COLLECTED_DATE")
    if not collected_date:
        raise ValueError("collected_date 또는 COLLECTED_DATE가 필요합니다.")

    bronze_dir = event.get("bronze_dir") or os.getenv("BRONZE_DIR", "data/bronze")
    silver_dir = event.get("silver_dir") or os.getenv("SILVER_DIR", "data/silver")

    loader = UberEligibleVehiclesSilverLoader(
        silver_dir, expect_collected_date=collected_date
    )
    result = Pipeline(
        UberEligibleVehiclesBronzeExtractor(bronze_dir, collected_date),
        loader,
        transformer=UberEligibleVehiclesSilverTransformer(),
    ).run()

    return {
        "row_count": result.write_result.row_count,
        # 도시별로 파일 하나씩 씁니다 — 도시 수는 len(locations) 입니다.
        "locations": loader.paths,
        "collected_date": collected_date,
    }


if __name__ == "__main__":
    print(json.dumps(lambda_handler(), ensure_ascii=False, indent=2))
