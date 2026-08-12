"""기사 마스터 테이블 월별 신규·탈퇴 배치 Lambda 핸들러.

Extractor(전월 스냅샷 + 신규·탈퇴 반영) 와 Loader(적재) 를 Pipeline 으로 이어붙이기만 합니다.
"""

import json
import os
from datetime import datetime, timezone

from pipeline_core.pipeline import Pipeline

from ..common.logging_setup import configure_lambda_logging
from .extractor import DEFAULT_SEED_PATH, DriverMasterExtractor
from .loader import DriverMasterBronzeLoader

configure_lambda_logging()


def lambda_handler(event: dict | None = None, context=None) -> dict:
    event = event or {}

    year_str = event.get("year") or os.getenv("YEAR")
    month_str = event.get("month") or os.getenv("MONTH")
    if not year_str or not month_str:
        raise ValueError("year와 month 파라미터가 누락되었습니다.")

    base_dir = event.get("base_dir") or os.getenv("BRONZE_DIR", "data/bronze/driver_master")
    seed_path = event.get("seed_path") or os.getenv("DRIVER_MASTER_SEED_PATH", DEFAULT_SEED_PATH)
    collected_at = datetime.now(timezone.utc)
    year_month = f"{year_str}-{str(month_str).zfill(2)}"

    result = Pipeline(
        DriverMasterExtractor(year_str, month_str, base_dir, seed_path),
        DriverMasterBronzeLoader(base_dir, year_month, collected_at),
    ).run()

    return {
        "row_count": result.write_result.row_count,
        "locations": [result.write_result.location],
        "year": year_str,
        "month": month_str,
        "year_month": year_month,
        "collected_date": f"{collected_at:%Y-%m-%d}",
    }


if __name__ == "__main__":
    print(json.dumps(lambda_handler(), ensure_ascii=False, indent=2))
