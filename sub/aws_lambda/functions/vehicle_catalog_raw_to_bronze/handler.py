"""리스 업체 보유 차량 대장 수집/적재 Lambda 핸들러.

Extractor(수집) 와 Loader(적재) 를 Pipeline 으로 이어붙이기만 합니다.
"""

import json
import os
from datetime import datetime, timezone

from pipeline_core.pipeline import Pipeline

from shared.aws_lambda.common.logging_setup import configure_lambda_logging
from .extractor import (
    VehicleCatalogCardsExtractor,
    VehicleCatalogHtmlExtractor,
    VehicleCatalogImageExtractor,
    row_from_snapshot,
)
from .loader import build_bronze_loader
from .snapshot import (
    VehicleCatalogHtmlSnapshotLoader,
    VehicleCatalogImageSnapshotLoader,
)

configure_lambda_logging()


def lambda_handler(event: dict | None = None, context=None) -> dict:
    event = event or {}
    base_dir = event.get("base_dir") or os.getenv("BRONZE_DIR", "data/bronze")
    storage = event.get("storage") or os.getenv("BRONZE_STORAGE", "local")
    bucket = event.get("bucket") or os.getenv("DATA_LAKE_S3_BUCKET")
    collected_at = datetime.now(timezone.utc)

    html_result = Pipeline(
        VehicleCatalogHtmlExtractor(),
        VehicleCatalogHtmlSnapshotLoader(base_dir, collected_at),
    ).run()
    html_snapshot_path = html_result.write_result.location

    cards = VehicleCatalogCardsExtractor(html_snapshot_path).extract()
    rows: list[dict] = []
    for card in cards:
        image_result = Pipeline(
            VehicleCatalogImageExtractor(card["image_url"]),
            VehicleCatalogImageSnapshotLoader(
                base_dir, collected_at, card["image_url"]
            ),
        ).run()
        rows.append(
            row_from_snapshot(
                card,
                html_snapshot_path,
                image_result.write_result.location,
                collected_at,
            )
        )

    write_result = build_bronze_loader(storage, base_dir, collected_at, bucket=bucket).write(
        rows
    )

    return {
        "row_count": write_result.row_count,
        "locations": [write_result.location],
        # Silver 배치가 이 하루치 파티션을 읽습니다 (Bronze 파티션 키와 동일).
        "collected_date": f"{collected_at:%Y-%m-%d}",
    }


if __name__ == "__main__":
    print(json.dumps(lambda_handler(), ensure_ascii=False, indent=2))
