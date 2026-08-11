"""Gas Price Bronze 수집분을 일별 Silver JSON으로 변환합니다.

정기 실행은 `collected_date` 하루치만 처리합니다. 과거 보정은 `collected_month`를
지정한 수동 백필로 처리합니다. 둘 중 정확히 하나만 지정해야 합니다.
"""

import os

from pipeline_core.pipeline import Pipeline

from ..common.logging_setup import configure_lambda_logging
from .extractor import GasPriceBronzeExtractor
from .loader import GasPriceSilverLoader
from .transformer import GasPriceSilverTransformer

configure_lambda_logging()


def lambda_handler(event: dict | None = None, context=None) -> dict:
    event = event or {}
    collected_date = event.get("collected_date") or os.getenv("COLLECTED_DATE")
    collected_month = event.get("collected_month") or os.getenv("COLLECTED_MONTH")
    if bool(collected_date) == bool(collected_month):
        raise ValueError(
            "collected_date와 collected_month 중 정확히 하나만 지정해야 합니다."
        )

    bronze_dir = event.get("bronze_dir") or os.getenv("BRONZE_DIR", "data/bronze")
    silver_dir = event.get("silver_dir") or os.getenv("SILVER_DIR", "data/silver")
    expect_price_date = event.get("expect_price_date")

    loader = GasPriceSilverLoader(silver_dir)
    result = Pipeline(
        GasPriceBronzeExtractor(
            bronze_dir,
            collected_date=collected_date,
            collected_month=collected_month,
        ),
        loader,
        transformer=GasPriceSilverTransformer(),
    ).run()

    # 파일 존재 여부가 아니라 "이번 실행이 이 날짜를 처리했는가"를 확인합니다.
    # 이전 실행이 남긴 파일이 있으면 존재 검사만으로는 누락을 잡지 못합니다.
    if expect_price_date and expect_price_date not in loader.handled:
        raise RuntimeError(
            f"이번 실행이 대상 날짜를 처리하지 않았습니다: price_date={expect_price_date} "
            f"(처리한 날짜: {sorted(loader.handled) or '없음'})"
        )

    return {
        "row_count": result.write_result.row_count,
        "locations": [result.write_result.location],
        "target": collected_date or collected_month,
        # 이번 실행이 처리한 price_date 개수 (row_count 와 다릅니다).
        "processed_count": len(loader.handled),
    }
