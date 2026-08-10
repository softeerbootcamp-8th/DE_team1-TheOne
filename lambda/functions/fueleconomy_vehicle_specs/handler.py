"""fueleconomy.gov 차종별 제원 수집/적재 Lambda 핸들러.

Extractor(수집) 와 Loader(적재) 를 Pipeline 으로 이어붙이기만 합니다.
1년에 한 번 도는 것을 전제로, 매번 전량 스냅샷을 새 파티션에 씁니다.
"""

import json
import os
from datetime import datetime, timezone

from pipeline_core.pipeline import Pipeline

from ..common.logging_setup import configure_lambda_logging
from .extractor import VehicleSpecsExtractor
from .loader import VehicleSpecsBronzeLoader

configure_lambda_logging()


def lambda_handler(event: dict | None = None, context=None) -> dict:
    event = event or {}
    base_dir = event.get("base_dir") or os.getenv("BRONZE_DIR", "data/bronze")
    collected_at = datetime.now(timezone.utc)

    result = Pipeline(
        VehicleSpecsExtractor(collected_at),
        VehicleSpecsBronzeLoader(base_dir, collected_at),
    ).run()

    return {
        "collected_date": f"{collected_at:%Y-%m-%d}",
        "row_count": result.write_result.row_count,
        "path": result.write_result.location,
    }


if __name__ == "__main__":
    print(json.dumps(lambda_handler(), ensure_ascii=False, indent=2))
