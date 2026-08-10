"""NYC FHVHV Trip Record 수집/적재 Lambda 핸들러.

Extractor(수집) 와 Loader(적재) 를 Pipeline 으로 이어붙이기만 합니다.
"""

import json
import os
from datetime import datetime, timezone
from pathlib import Path

from pipeline_core.pipeline import Pipeline

from ..common.logging_setup import configure_lambda_logging
from .extractor import HvfhvExtractor
from .loader import HvfhvBronzeLoader

configure_lambda_logging()


def lambda_handler(event: dict | None = None, context=None) -> dict:
    event = event or {}

    year_str = event.get("year") or os.getenv("YEAR")
    month_str = event.get("month") or os.getenv("MONTH")
    if not year_str or not month_str:
        raise ValueError("year와 month 파라미터가 누락되었습니다.")

    base_dir = event.get("base_dir") or os.getenv("BRONZE_DIR", "data/bronze")
    collected_at = datetime.now(timezone.utc)
    year_month = f"{year_str}-{str(month_str).zfill(2)}"

    result = Pipeline(
        HvfhvExtractor(year_str, month_str),
        HvfhvBronzeLoader(base_dir, year_month, collected_at),
    ).run()

    path = result.write_result.location
    return {
        "year": year_str,
        "month": month_str,
        "year_month": year_month,
        "collected_date": f"{collected_at:%Y-%m-%d}",
        "file_size_bytes": Path(path).stat().st_size,
        "path": path,
    }


if __name__ == "__main__":
    print(json.dumps(lambda_handler(), ensure_ascii=False, indent=2))
