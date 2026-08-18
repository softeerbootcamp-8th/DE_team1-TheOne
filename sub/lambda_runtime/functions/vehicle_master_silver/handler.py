"""네 개 Silver 를 합쳐 차량 마스터 Silver 를 만듭니다.

다른 함수와 달리 Bronze 를 읽지 않습니다. 입력도 출력도 Silver 입니다.

`collected_date` 는 **만드는 날**입니다. 비우면 실행일(UTC)을 씁니다. 원천은
각자의 최신 파티션에서 읽으므로 이 값과 원천 수집일은 다를 수 있습니다.
"""

import json
import os
from datetime import datetime, timezone

from pipeline_core.pipeline import Pipeline

from shared.lambda_runtime.common.logging_setup import configure_lambda_logging
from .extractor import build_extractor
from .loader import build_loader
from .transformer import VehicleMasterSilverTransformer

configure_lambda_logging()


def lambda_handler(event: dict | None = None, context=None) -> dict:
    event = event or {}
    collected_date = (
        event.get("collected_date")
        or os.getenv("COLLECTED_DATE")
        or datetime.now(timezone.utc).date().isoformat()
    )
    storage = event.get("storage") or os.getenv("BRONZE_STORAGE", "local")
    silver_dir = event.get("silver_dir") or os.getenv("SILVER_DIR", "data/silver")
    bucket = event.get("bucket") or os.getenv("DATA_LAKE_S3_BUCKET")

    extractor = build_extractor(storage, silver_dir, collected_date, bucket=bucket)
    loader = build_loader(storage, silver_dir, collected_date, bucket=bucket)
    result = Pipeline(
        extractor,
        loader,
        transformer=VehicleMasterSilverTransformer(),
    ).run()

    return {
        "row_count": result.write_result.row_count,
        # 도시별로 파일 하나씩 씁니다 — 도시 수는 len(locations) 입니다.
        "locations": loader.paths,
        "collected_date": collected_date,
        # 어느 원천 스냅샷으로 만들었는지. 원천마다 수집 주기가 달라 날짜가
        # 제각각이라 결과만 보고는 알 수 없습니다.
        "source_collected_dates": extractor.source_collected_dates,
    }


if __name__ == "__main__":
    print(json.dumps(lambda_handler(), ensure_ascii=False, indent=2))
