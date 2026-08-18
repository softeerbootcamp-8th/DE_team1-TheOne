"""Lyft 배차 가능 차량 데이터셋의 저장 경로 규칙.

Bronze 를 쓰는 쪽(`lyft_eligible_vehicles_raw_to_bronze`)과 읽는 쪽
(`lyft_eligible_vehicles_bronze_to_silver`), 그리고 적재 결과를 검증하는 DAG 가
같은 규칙을 봐야 하므로 한 곳에 모읍니다.

    <base>/lyft_eligible_vehicles/collected_date=YYYY-MM-DD/city=<도시>/<수집시각>.parquet
    <base>/lyft_eligible_vehicles/collected_date=YYYY-MM-DD/city=<도시>/lyft_eligible_vehicles.parquet

도시(city)는 데이터셋 이름이 아니라 파티션 키입니다. 뉴욕 외 도시가 늘어도
데이터셋 이름은 그대로입니다.

시그니처는 `uber_eligible_vehicles_layout` 과 같습니다. 두 데이터셋을 같은 방식으로
다루기 위해서이고, 한쪽만 바꾸면 조인·검증이 조용히 어긋납니다.
"""

from datetime import date, datetime
from pathlib import Path

DATASET = "lyft_eligible_vehicles"
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


def bronze_date_prefix(collected_date: str) -> str:
    """S3 bronze의 날짜 파티션 prefix. 그 아래 도시별 파티션이 있습니다.

    bronze_key()와 이 함수가 서로 다른 문자열을 만들면 도시 목록을 나열하는 쪽
    (S3BronzeExtractor)이 쓰는 쪽이 실제로 쓴 위치를 못 찾게 되므로 한 곳에 모읍니다.
    """
    return f"bronze/{DATASET}/{DATE_PARTITION_KEY}={collected_date}/"


def bronze_key(city: str, collected_at: datetime) -> str:
    """S3 bronze key. bronze_file()과 같은 파티션 규칙, base_dir 대신 bronze/ prefix."""
    return (
        f"{bronze_date_prefix(f'{collected_at:%Y-%m-%d}')}"
        f"{CITY_PARTITION_KEY}={city}/{collected_at:%Y%m%dT%H%M%SZ}.parquet"
    )


def silver_file(base_dir: str, collected_date: date, city: str) -> Path:
    """Silver 는 재실행하면 덮어씁니다. 그래서 파일명이 고정입니다."""
    partition = city_partition(base_dir, collected_date.isoformat(), city)
    return partition / SILVER_FILE_NAME


def silver_key(collected_date: date, city: str) -> str:
    """S3 silver key. silver_file()과 같은 파티션 규칙, base_dir 대신 silver/ prefix."""
    return (
        f"silver/{DATASET}/{DATE_PARTITION_KEY}={collected_date.isoformat()}/"
        f"{CITY_PARTITION_KEY}={city}/{SILVER_FILE_NAME}"
    )


def city_from_key(key: str) -> str:
    """S3 bronze key에서 city=<city> 파티션 세그먼트를 읽습니다 (파일 안에는 없는 값입니다)."""
    for segment in key.split("/"):
        if segment.startswith(f"{CITY_PARTITION_KEY}="):
            return segment.removeprefix(f"{CITY_PARTITION_KEY}=")
    raise ValueError(f"key에 city 파티션이 없습니다: {key}")
