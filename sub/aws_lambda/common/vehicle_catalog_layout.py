"""리스 업체 보유 차량 대장 데이터셋의 저장 경로 규칙.

Raw 를 쓰는 쪽(`vehicle_catalog_source_to_raw`)과 읽는 쪽
(`vehicle_catalog_raw_to_curated`)이 같은 규칙을 봐야 하므로 한 곳에 모읍니다.

    <base>/vehicle_catalog/raw/collected_at=<수집시각>/source.html
    <base>/vehicle_catalog/raw/collected_at=<수집시각>/images/<URL-SHA256>.bin
    <base>/vehicle_catalog/collected_date=YYYY-MM-DD/vendor=<업체>/<수집시각>.parquet
    <base>/vehicle_catalog/collected_date=YYYY-MM-DD/vendor=<업체>/vehicle_catalog.parquet

업체(vendor)는 데이터셋 이름이 아니라 파티션 키입니다. 업체가 늘어도
데이터셋 이름은 그대로입니다.
"""

import hashlib
from datetime import date, datetime, timezone
from pathlib import Path

DATASET = "vehicle_catalog"
DATE_PARTITION_KEY = "collected_date"
VENDOR_PARTITION_KEY = "vendor"
RAW_DIR_NAME = "raw"
SNAPSHOT_PARTITION_KEY = "collected_at"
HTML_SNAPSHOT_FILE_NAME = "source.html"
IMAGE_SNAPSHOT_DIR_NAME = "images"
CURATED_FILE_NAME = f"{DATASET}.parquet"


def dataset_path(base_dir: str) -> Path:
    return Path(base_dir) / DATASET


def snapshot_partition(base_dir: str, collected_at: datetime) -> Path:
    if collected_at.tzinfo is None:
        raise ValueError("collected_at에 시간대가 필요합니다.")
    timestamp = collected_at.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    return dataset_path(base_dir) / RAW_DIR_NAME / f"{SNAPSHOT_PARTITION_KEY}={timestamp}"


def html_snapshot_file(base_dir: str, collected_at: datetime) -> Path:
    return snapshot_partition(base_dir, collected_at) / HTML_SNAPSHOT_FILE_NAME


def image_snapshot_file(base_dir: str, collected_at: datetime, source_url: str) -> Path:
    digest = hashlib.sha256(source_url.encode("utf-8")).hexdigest()
    return snapshot_partition(base_dir, collected_at) / IMAGE_SNAPSHOT_DIR_NAME / f"{digest}.bin"


def date_partition(base_dir: str, collected_date: str) -> Path:
    """수집일 파티션 경로. 아래에 업체 파티션이 한 단계 더 있습니다."""
    return dataset_path(base_dir) / f"{DATE_PARTITION_KEY}={collected_date}"


def vendor_partition(base_dir: str, collected_date: str, vendor: str) -> Path:
    return date_partition(base_dir, collected_date) / f"{VENDOR_PARTITION_KEY}={vendor}"


def vendor_from_partition(partition: Path) -> str:
    """업체 파티션 디렉터리명에서 업체를 읽습니다 (파일 안에는 없는 값입니다)."""
    return partition.name.removeprefix(f"{VENDOR_PARTITION_KEY}=")


def raw_file(base_dir: str, vendor: str, collected_at: datetime) -> Path:
    """Raw 파일 이름은 수집 시각입니다 (하루에 여러 번 수집해도 안 덮어씁니다)."""
    partition = vendor_partition(base_dir, f"{collected_at:%Y-%m-%d}", vendor)
    return partition / f"{collected_at:%Y%m%dT%H%M%SZ}.parquet"


def raw_date_prefix(collected_date: str) -> str:
    """S3 raw의 날짜 파티션 prefix. 그 아래 업체별 파티션이 있습니다.

    raw_key()와 이 함수가 서로 다른 문자열을 만들면 업체 목록을 나열하는 쪽
    (S3RawExtractor)이 쓰는 쪽이 실제로 쓴 위치를 못 찾게 되므로 한 곳에 모읍니다.
    """
    return f"source/raw/{DATASET}/{DATE_PARTITION_KEY}={collected_date}/"


def raw_key(vendor: str, collected_at: datetime) -> str:
    """S3 raw key. raw_file()과 같은 파티션 규칙, base_dir 대신 raw/ prefix."""
    return (
        f"{raw_date_prefix(f'{collected_at:%Y-%m-%d}')}"
        f"{VENDOR_PARTITION_KEY}={vendor}/{collected_at:%Y%m%dT%H%M%SZ}.parquet"
    )


def curated_file(base_dir: str, collected_date: date, vendor: str) -> Path:
    """Curated 는 재실행하면 덮어씁니다. 그래서 파일명이 고정입니다."""
    partition = vendor_partition(base_dir, collected_date.isoformat(), vendor)
    return partition / CURATED_FILE_NAME


def curated_key(collected_date: date, vendor: str) -> str:
    """S3 curated key. curated_file()과 같은 파티션 규칙, base_dir 대신 curated/ prefix."""
    return (
        f"source/curated/{DATASET}/{DATE_PARTITION_KEY}={collected_date.isoformat()}/"
        f"{VENDOR_PARTITION_KEY}={vendor}/{CURATED_FILE_NAME}"
    )


def vendor_from_key(key: str) -> str:
    """S3 raw key에서 vendor=<업체> 파티션 세그먼트를 읽습니다 (파일 안에는 없는 값입니다)."""
    for segment in key.split("/"):
        if segment.startswith(f"{VENDOR_PARTITION_KEY}="):
            return segment.removeprefix(f"{VENDOR_PARTITION_KEY}=")
    raise ValueError(f"key에 vendor 파티션이 없습니다: {key}")
