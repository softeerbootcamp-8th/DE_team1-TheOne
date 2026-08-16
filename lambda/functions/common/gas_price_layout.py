"""휘발유 가격 데이터셋의 저장 경로 규칙.

Bronze 를 쓰는 쪽(`gas_price_raw_to_bronze`)과 읽는 쪽(`gas_price_bronze_to_silver`)이
같은 규칙을 봐야 하므로 한 곳에 모읍니다. 규칙을 바꿀 때 여기만 고치면 양쪽이 함께 따라갑니다.

    <base>/gas_price/raw/collected_at=YYYYMMDDTHHMMSSffffffZ/source.html
    <base>/gas_price/collected_date=YYYY-MM-DD/gas_price.json       (Bronze)
    <base>/gas_price/collected_month=YYYY-MM/gas_price.parquet      (Silver)
"""

from datetime import datetime, timezone
from pathlib import Path

DATASET = "gas_price"
BRONZE_PARTITION_KEY = "collected_date"
SILVER_PARTITION_KEY = "collected_month"
RAW_DIR_NAME = "raw"
SNAPSHOT_PARTITION_KEY = "collected_at"
SNAPSHOT_FILE_NAME = "source.html"
BRONZE_FILE_NAME = "gas_price.json"
SILVER_FILE_NAME = "gas_price.parquet"


def dataset_path(base_dir: str) -> Path:
    return Path(base_dir) / DATASET


def snapshot_file(base_dir: str, collected_at: datetime) -> Path:
    """수집시각별 HTML 원문 경로. 같은 시각의 파일은 Loader가 덮어쓰지 않습니다."""
    if collected_at.tzinfo is None:
        raise ValueError("collected_at에 시간대가 필요합니다.")
    timestamp = collected_at.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    partition = f"{SNAPSHOT_PARTITION_KEY}={timestamp}"
    return dataset_path(base_dir) / RAW_DIR_NAME / partition / SNAPSHOT_FILE_NAME


def bronze_partition(base_dir: str, collected_date: str) -> Path:
    """수집일 파티션 경로. `collected_date` 에 glob 패턴(`2026-08-*`)도 넣을 수 있습니다."""
    return dataset_path(base_dir) / f"{BRONZE_PARTITION_KEY}={collected_date}"


def bronze_file(base_dir: str, collected_date: str) -> Path:
    """수집일 파티션에 원문 JSON 하나를 저장합니다."""
    return bronze_partition(base_dir, collected_date) / BRONZE_FILE_NAME


def bronze_key(collected_date: str) -> str:
    """S3 bronze key. bronze_file()과 같은 파티션 규칙, base_dir 대신 bronze/ prefix."""
    return f"bronze/{DATASET}/{BRONZE_PARTITION_KEY}={collected_date}/{BRONZE_FILE_NAME}"


def silver_file(base_dir: str, collected_month: str) -> Path:
    partition = f"{SILVER_PARTITION_KEY}={collected_month}"
    return dataset_path(base_dir) / partition / SILVER_FILE_NAME
