"""리스 업체 보유 차량 대장 데이터셋의 저장 경로 규칙.

Bronze 를 쓰는 쪽(`vehicle_catalog_raw_to_bronze`)과 읽는 쪽
(`vehicle_catalog_bronze_to_silver`)이 같은 규칙을 봐야 하므로 한 곳에 모읍니다.

    <base>/vehicle_catalog/collected_date=YYYY-MM-DD/vendor=<업체>/<수집시각>.parquet
    <base>/vehicle_catalog/collected_date=YYYY-MM-DD/vendor=<업체>/vehicle_catalog.parquet

업체(vendor)는 데이터셋 이름이 아니라 파티션 키입니다. 업체가 늘어도
데이터셋 이름은 그대로입니다.
"""

from datetime import date, datetime
from pathlib import Path

DATASET = "vehicle_catalog"
DATE_PARTITION_KEY = "collected_date"
VENDOR_PARTITION_KEY = "vendor"
SILVER_FILE_NAME = f"{DATASET}.parquet"


def dataset_path(base_dir: str) -> Path:
    return Path(base_dir) / DATASET


def date_partition(base_dir: str, collected_date: str) -> Path:
    """수집일 파티션 경로. 아래에 업체 파티션이 한 단계 더 있습니다."""
    return dataset_path(base_dir) / f"{DATE_PARTITION_KEY}={collected_date}"


def vendor_partition(base_dir: str, collected_date: str, vendor: str) -> Path:
    return date_partition(base_dir, collected_date) / f"{VENDOR_PARTITION_KEY}={vendor}"


def vendor_from_partition(partition: Path) -> str:
    """업체 파티션 디렉터리명에서 업체를 읽습니다 (파일 안에는 없는 값입니다)."""
    return partition.name.removeprefix(f"{VENDOR_PARTITION_KEY}=")


def bronze_file(base_dir: str, vendor: str, collected_at: datetime) -> Path:
    """Bronze 파일 이름은 수집 시각입니다 (하루에 여러 번 수집해도 안 덮어씁니다)."""
    partition = vendor_partition(base_dir, f"{collected_at:%Y-%m-%d}", vendor)
    return partition / f"{collected_at:%Y%m%dT%H%M%SZ}.parquet"


def silver_file(base_dir: str, collected_date: date, vendor: str) -> Path:
    """Silver 는 재실행하면 덮어씁니다. 그래서 파일명이 고정입니다."""
    partition = vendor_partition(base_dir, collected_date.isoformat(), vendor)
    return partition / SILVER_FILE_NAME
