"""Lyft 배차 가능 차량 데이터셋의 공통 저장 경로 규칙."""

from pathlib import Path

DATASET = "lyft_eligible_vehicles"
DATE_PARTITION_KEY = "collected_date"
CITY_PARTITION_KEY = "city"


def dataset_path(base_dir: str) -> Path:
    return Path(base_dir) / DATASET


def date_partition(base_dir: str, collected_date: str) -> Path:
    return dataset_path(base_dir) / f"{DATE_PARTITION_KEY}={collected_date}"


def city_from_partition(partition: Path) -> str:
    return partition.name.removeprefix(f"{CITY_PARTITION_KEY}=")
