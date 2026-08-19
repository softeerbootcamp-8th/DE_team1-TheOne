"""기사 데이터를 Bronze에 적재합니다."""

import json
import os
from pathlib import Path

from pipeline_core.pipeline import Pipeline

from shared.aws_lambda.common.logging_setup import configure_lambda_logging
from main.aws_lambda.common.monthly_dataset import requested_year_month
from .loader import CompanySnapshotBronzeLoader
from .source_snapshot import CompanySnapshotExtractor


configure_lambda_logging()


def lambda_handler(event: dict | None = None, context=None) -> dict:
    event = event or {}
    api_base_url = event.get("api_base_url") or os.getenv("SYNTHETIC_SOURCE_API_URL")
    if not api_base_url:
        raise ValueError("api_base_url이 누락되었습니다")
    base_dir = event.get("base_dir") or os.getenv("BRONZE_DIR", "data/bronze")
    loader = CompanySnapshotBronzeLoader(base_dir)
    result = Pipeline(
        CompanySnapshotExtractor(api_base_url, requested_year_month(event)),
        loader,
    ).run()
    path = Path(result.write_result.location)
    payload = loader.payload
    return {
        "year_month": payload["year_month"],
        "year": payload["year_month"][:4],
        "month": payload["year_month"][5:],
        "row_count": result.write_result.row_count,
        "locations": [str(path)],
        "file_size_bytes": path.stat().st_size,
    }


if __name__ == "__main__":
    print(json.dumps(lambda_handler(), ensure_ascii=False, indent=2))
