"""차량 마스터 통합 Silver 데이터셋의 저장 경로 규칙.

    <base>/vehicle_master/collected_date=YYYY-MM-DD/city=<도시>/vehicle_master.parquet

다른 데이터셋과 달리 Bronze 가 없습니다. 네 개의 Silver 를 조인해서 만드는
파생 Silver 라 원천은 전부 `<base>/<데이터셋>/collected_date=.../` 아래에 있습니다.

도시(city)가 파티션 키인 이유는 배차 자격이 도시마다 다르기 때문입니다. 차량
대장과 제원은 도시와 무관하지만, 자격이 도시별이라 결과 행도 도시별로 갈립니다.

`collected_date` 는 **이 테이블을 만든 날**이지 원천을 수집한 날이 아닙니다.
원천마다 수집 주기가 달라(제원 연 1회, 자격·대장 주 1회) 한 날짜로 묶을 수
없습니다. 어느 스냅샷에서 나왔는지는 행의 `*_bronze_path` 로 되짚습니다.
"""

from datetime import date
from pathlib import Path

DATASET = "vehicle_master"
DATE_PARTITION_KEY = "collected_date"
CITY_PARTITION_KEY = "city"
SILVER_FILE_NAME = f"{DATASET}.parquet"


def dataset_path(base_dir: str) -> Path:
    return Path(base_dir) / DATASET


def date_partition(base_dir: str, collected_date: str) -> Path:
    """생성일 파티션 경로. 아래에 도시 파티션이 한 단계 더 있습니다."""
    return dataset_path(base_dir) / f"{DATE_PARTITION_KEY}={collected_date}"


def city_partition(base_dir: str, collected_date: str, city: str) -> Path:
    return date_partition(base_dir, collected_date) / f"{CITY_PARTITION_KEY}={city}"


def city_from_partition(partition: Path) -> str:
    """도시 파티션 디렉터리명에서 도시를 읽습니다 (파일 안에는 없는 값입니다)."""
    return partition.name.removeprefix(f"{CITY_PARTITION_KEY}=")


def silver_file(base_dir: str, collected_date: date, city: str) -> Path:
    """Silver 는 재실행하면 덮어씁니다. 그래서 파일명이 고정입니다."""
    return city_partition(base_dir, collected_date.isoformat(), city) / SILVER_FILE_NAME


def latest_date_partition(dataset_dir: Path, as_of: date) -> tuple[date, Path]:
    """`as_of` 이하 중 가장 최신인 `collected_date=` 파티션을 고릅니다.

    원천 네 개는 수집 주기가 제각각입니다 — 제원은 연 1회, 자격과 대장은 주 1회.
    실행일 파티션을 그대로 찾으면 제원은 1년에 하루만 붙습니다. 그래서
    데이터셋마다 따로 최신 파티션을 찾습니다.

    `as_of` 보다 뒤의 파티션은 건너뜁니다. 과거 날짜로 다시 돌렸을 때 그때
    없었던 데이터가 섞여 들어오면 재현이 안 됩니다.
    """
    if not dataset_dir.is_dir():
        raise FileNotFoundError(f"원천 Silver 데이터셋이 없습니다: {dataset_dir}")

    candidates: list[tuple[date, Path]] = []
    for partition in dataset_dir.glob(f"{DATE_PARTITION_KEY}=*"):
        if not partition.is_dir():
            continue
        try:
            partition_date = date.fromisoformat(
                partition.name.removeprefix(f"{DATE_PARTITION_KEY}=")
            )
        except ValueError:
            # 규칙과 다른 디렉터리는 파티션이 아닙니다. 조용히 건너뛰면 원천이
            # 통째로 비어도 모르니 여기서는 넘기고 아래에서 한 번에 실패시킵니다.
            continue
        if partition_date <= as_of:
            candidates.append((partition_date, partition))

    if not candidates:
        raise FileNotFoundError(
            f"{as_of.isoformat()} 이전의 Silver 파티션이 없습니다: {dataset_dir}"
        )
    return max(candidates, key=lambda item: item[0])
