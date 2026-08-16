"""전기차 충전소 데이터셋의 저장 경로 규칙.

Bronze 를 쓰는 쪽(`ev_charging_stations_raw_to_bronze`)과 읽는 쪽
(`ev_charging_stations_bronze_to_silver`)이 같은 규칙을 봐야 하므로 한 곳에 모읍니다.

    <base>/ev_charging_stations/collected_date=YYYY-MM-DD/<수집시각>.json        (Bronze)
    <base>/ev_charging_price/collected_month=YYYY-MM/ev_charging_price.parquet  (Silver)

Bronze 는 NLR API 응답 원문 JSON, Silver 는 뉴욕시 일별 평균 요금 한 행이라
데이터셋 이름이 다릅니다. 같은 데이터의 이름이 갈린 것이 아니라 알갱이가 다릅니다.
"""

from datetime import datetime
from pathlib import Path

BRONZE_DATASET = "ev_charging_stations"
SILVER_DATASET = "ev_charging_price"
BRONZE_PARTITION_KEY = "collected_date"
SILVER_PARTITION_KEY = "collected_month"
SILVER_FILE_NAME = "ev_charging_price.parquet"


def bronze_dataset_path(base_dir: str) -> Path:
    return Path(base_dir) / BRONZE_DATASET


def bronze_partition(base_dir: str, collected_date: str) -> Path:
    """수집일 파티션 경로."""
    return bronze_dataset_path(base_dir) / f"{BRONZE_PARTITION_KEY}={collected_date}"


def bronze_file(base_dir: str, collected_at: datetime) -> Path:
    """Bronze 파일 이름은 수집 시각입니다 (하루에 여러 번 수집해도 안 덮어씁니다)."""
    partition = bronze_partition(base_dir, f"{collected_at:%Y-%m-%d}")
    return partition / f"{collected_at:%Y%m%dT%H%M%SZ}.json"


def bronze_key(collected_at: datetime) -> str:
    """S3 bronze key. bronze_file()과 같은 파티션 규칙, base_dir 대신 bronze/ prefix."""
    return (
        f"bronze/{BRONZE_DATASET}/{BRONZE_PARTITION_KEY}={collected_at:%Y-%m-%d}/"
        f"{collected_at:%Y%m%dT%H%M%SZ}.json"
    )


def silver_dataset_path(base_dir: str) -> Path:
    return Path(base_dir) / SILVER_DATASET


def silver_file(base_dir: str, collected_month: str) -> Path:
    partition = f"{SILVER_PARTITION_KEY}={collected_month}"
    return silver_dataset_path(base_dir) / partition / SILVER_FILE_NAME
