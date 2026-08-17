"""HVFHV+taxi_id 데이터를 Bronze에 적재합니다."""

import json
import os
from pathlib import Path

from pipeline_core.pipeline import Pipeline

from ..common.logging_setup import configure_lambda_logging
from ..common.synthetic_release import requested_year_month
from .extractor import HvfhvExtractor
from .loader import HvfhvBronzeLoader


configure_lambda_logging()


def lambda_handler(event: dict | None = None, context=None) -> dict:
    event = event or {}
    api_base_url = event.get("api_base_url") or os.getenv("SYNTHETIC_SOURCE_API_URL")
    if not api_base_url:
        raise ValueError("api_base_url이 누락되었습니다")
    base_dir = event.get("base_dir") or os.getenv("BRONZE_DIR", "data/bronze")
    loader = HvfhvBronzeLoader(base_dir)
    result = Pipeline(
        HvfhvExtractor(api_base_url, requested_year_month(event)), loader
    ).run()
    path = Path(result.write_result.location)
    release = loader.release
    return {
        "release_id": release["release_id"],
        "year_month": release["year_month"],
        "year": release["year_month"][:4],
        "month": release["year_month"][5:],
        "row_count": result.write_result.row_count,
        "locations": [str(path)],
        "marker_location": str(loader.marker_path),
        "sha256": release["metadata"]["sha256"],
        "file_size_bytes": path.stat().st_size,
        "already_collected": loader.already_collected,
    }


if __name__ == "__main__":
    print(json.dumps(lambda_handler(), ensure_ascii=False, indent=2))
