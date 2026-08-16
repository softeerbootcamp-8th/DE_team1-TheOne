"""차종별 제원 데이터셋의 저장 경로 규칙.

Bronze 를 쓰는 쪽(`fueleconomy_vehicle_specs_raw_to_bronze`)과 읽는 쪽
(`fueleconomy_vehicle_specs_bronze_to_silver`)이 같은 규칙을 봐야 하므로 한 곳에 모읍니다.

    <base>/fueleconomy_vehicle_specs/collected_date=YYYY-MM-DD/source=<출처>/<수집시각>.parquet
    <base>/fueleconomy_vehicle_specs/collected_date=YYYY-MM-DD/source=<출처>/fueleconomy_vehicle_specs.parquet

출처(source)는 파티션 키입니다. 지금은 fueleconomy.gov 하나뿐이지만 다른
제원 출처가 붙어도 데이터셋 이름은 그대로입니다.

한 달에 한 번 수집이라 실제로는 월 1개 파티션이 쌓입니다. 다른 데이터셋과
모양을 맞추기 위해 날짜 단위 키를 그대로 씁니다.
"""

from datetime import date, datetime
from pathlib import Path

DATASET = "fueleconomy_vehicle_specs"
DATE_PARTITION_KEY = "collected_date"
SOURCE_PARTITION_KEY = "source"
SILVER_FILE_NAME = f"{DATASET}.parquet"


def dataset_path(base_dir: str) -> Path:
    return Path(base_dir) / DATASET


def date_partition(base_dir: str, collected_date: str) -> Path:
    """수집일 파티션 경로. 아래에 출처 파티션이 한 단계 더 있습니다."""
    return dataset_path(base_dir) / f"{DATE_PARTITION_KEY}={collected_date}"


def source_partition(base_dir: str, collected_date: str, source: str) -> Path:
    return date_partition(base_dir, collected_date) / f"{SOURCE_PARTITION_KEY}={source}"


def source_from_partition(partition: Path) -> str:
    """출처 파티션 디렉터리명에서 출처를 읽습니다 (파일 안에는 없는 값입니다)."""
    return partition.name.removeprefix(f"{SOURCE_PARTITION_KEY}=")


def bronze_file(base_dir: str, source: str, collected_at: datetime) -> Path:
    """Bronze 파일 이름은 수집 시각입니다.

    매 실행이 전량 스냅샷이라 같은 해에 다시 돌려도 이전 것을 덮어쓰지 않습니다.
    """
    partition = source_partition(base_dir, f"{collected_at:%Y-%m-%d}", source)
    return partition / f"{collected_at:%Y%m%dT%H%M%SZ}.parquet"


def bronze_key(source: str, collected_at: datetime) -> str:
    """S3 bronze key. bronze_file()과 같은 파티션 규칙, base_dir 대신 bronze/ prefix."""
    return (
        f"bronze/{DATASET}/{DATE_PARTITION_KEY}={collected_at:%Y-%m-%d}/"
        f"{SOURCE_PARTITION_KEY}={source}/{collected_at:%Y%m%dT%H%M%SZ}.parquet"
    )


def silver_file(base_dir: str, collected_date: date, source: str) -> Path:
    """Silver 는 재실행하면 덮어씁니다. 그래서 파일명이 고정입니다."""
    partition = source_partition(base_dir, collected_date.isoformat(), source)
    return partition / SILVER_FILE_NAME
