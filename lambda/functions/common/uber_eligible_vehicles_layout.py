"""Uber 배차 가능 차량 데이터셋의 저장 경로 규칙.

Bronze 를 쓰는 쪽(`uber_eligible_vehicles_raw_to_bronze`)과 읽는 쪽
(`uber_eligible_vehicles_bronze_to_silver`)이 같은 규칙을 봐야 하므로 한 곳에 모읍니다.

    <base>/uber_eligible_vehicles/collected_date=YYYY-MM-DD/city=<도시>/<수집시각>.parquet
    <base>/uber_eligible_vehicles/collected_date=YYYY-MM-DD/city=<도시>/uber_eligible_vehicles.parquet

도시(city)는 데이터셋 이름이 아니라 파티션 키입니다. 뉴욕 외 도시가 늘어도
데이터셋 이름은 그대로입니다.
"""

from datetime import date, datetime
from pathlib import Path

DATASET = "uber_eligible_vehicles"
DATE_PARTITION_KEY = "collected_date"
CITY_PARTITION_KEY = "city"
SILVER_FILE_NAME = f"{DATASET}.parquet"


def dataset_path(base_dir: str) -> Path:
    return Path(base_dir) / DATASET


def date_partition(base_dir: str, collected_date: str) -> Path:
    """수집일 파티션 경로. 아래에 도시 파티션이 한 단계 더 있습니다."""
    return dataset_path(base_dir) / f"{DATE_PARTITION_KEY}={collected_date}"


def city_partition(base_dir: str, collected_date: str, city: str) -> Path:
    return date_partition(base_dir, collected_date) / f"{CITY_PARTITION_KEY}={city}"


def city_from_partition(partition: Path) -> str:
    """도시 파티션 디렉터리명에서 도시를 읽습니다 (파일 안에는 없는 값입니다)."""
    return partition.name.removeprefix(f"{CITY_PARTITION_KEY}=")


def bronze_file(base_dir: str, city: str, collected_at: datetime) -> Path:
    """Bronze 파일 이름은 수집 시각입니다 (하루에 여러 번 수집해도 안 덮어씁니다)."""
    partition = city_partition(base_dir, f"{collected_at:%Y-%m-%d}", city)
    return partition / f"{collected_at:%Y%m%dT%H%M%SZ}.parquet"


def bronze_key(city: str, collected_at: datetime) -> str:
    """S3 bronze key. bronze_file()과 같은 파티션 규칙, base_dir 대신 bronze/ prefix."""
    return (
        f"bronze/{DATASET}/{DATE_PARTITION_KEY}={collected_at:%Y-%m-%d}/"
        f"{CITY_PARTITION_KEY}={city}/{collected_at:%Y%m%dT%H%M%SZ}.parquet"
    )


def silver_file(base_dir: str, collected_date: date, city: str) -> Path:
    """Silver 는 재실행하면 덮어씁니다. 그래서 파일명이 고정입니다."""
    partition = city_partition(base_dir, collected_date.isoformat(), city)
    return partition / SILVER_FILE_NAME
