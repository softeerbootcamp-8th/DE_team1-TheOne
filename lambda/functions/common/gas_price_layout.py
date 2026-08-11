"""휘발유 가격 데이터셋의 저장 경로 규칙.

Bronze 를 쓰는 쪽(`gas_price_raw_to_bronze`)과 읽는 쪽(`gas_price_bronze_to_silver`)이
같은 규칙을 봐야 하므로 한 곳에 모읍니다. 규칙을 바꿀 때 여기만 고치면 양쪽이 함께 따라갑니다.

    <base>/gas_price/collected_date=YYYY-MM-DD/gas_price.json     (Bronze)
    <base>/gas_price/price_date=YYYY-MM-DD/gas_price.json          (Silver)
"""

from datetime import date
from pathlib import Path

DATASET = "gas_price"
BRONZE_PARTITION_KEY = "collected_date"
SILVER_PARTITION_KEY = "price_date"
BRONZE_FILE_NAME = "gas_price.json"
SILVER_FILE_NAME = "gas_price.json"


def dataset_path(base_dir: str) -> Path:
    return Path(base_dir) / DATASET


def bronze_partition(base_dir: str, collected_date: str) -> Path:
    """수집일 파티션 경로. `collected_date` 에 glob 패턴(`2026-08-*`)도 넣을 수 있습니다."""
    return dataset_path(base_dir) / f"{BRONZE_PARTITION_KEY}={collected_date}"


def bronze_file(base_dir: str, collected_date: str) -> Path:
    """수집일 파티션에 원문 JSON 하나를 저장합니다."""
    return bronze_partition(base_dir, collected_date) / BRONZE_FILE_NAME


def silver_file(base_dir: str, price_date: date) -> Path:
    partition = f"{SILVER_PARTITION_KEY}={price_date.isoformat()}"
    return dataset_path(base_dir) / partition / SILVER_FILE_NAME
