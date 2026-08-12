"""회사 고객·택시·리스 원천 스냅샷을 Bronze로 적재하는 Lambda 핸들러."""

import json
import os
from datetime import datetime, timezone

from pipeline_core.pipeline import Pipeline

from ..common.logging_setup import configure_lambda_logging
from .loader import CompanySnapshotBronzeLoader
from .source_snapshot import CompanySnapshotExtractor

configure_lambda_logging()


def lambda_handler(event: dict | None = None, context=None) -> dict:
    event = event or {}
    snapshot_date = event.get("snapshot_date") or os.getenv("SNAPSHOT_DATE")
    if not snapshot_date:
        raise ValueError("snapshot_date 또는 SNAPSHOT_DATE가 필요합니다.")

    source_dir = event.get("source_dir") or os.getenv("COMPANY_SOURCE_DIR", "data/source/company")
    bronze_dir = event.get("bronze_dir") or os.getenv("BRONZE_DIR", "data/bronze")
    collected_at = datetime.now(timezone.utc)
    loader = CompanySnapshotBronzeLoader(bronze_dir, snapshot_date, collected_at)
    result = Pipeline(
        CompanySnapshotExtractor(source_dir, snapshot_date),
        loader,
    ).run()

    return {
        "row_count": result.write_result.row_count,
        "row_counts": loader.row_counts,
        "locations": loader.paths,
        "snapshot_date": snapshot_date,
        "collected_date": collected_at.date().isoformat(),
    }


if __name__ == "__main__":
    print(json.dumps(lambda_handler(), ensure_ascii=False, indent=2))
