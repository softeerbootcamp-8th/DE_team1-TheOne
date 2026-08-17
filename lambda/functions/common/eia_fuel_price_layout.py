"""EIA 연료비 데이터셋의 저장 경로 규칙.

    <base>/eia_gas_price/collected_date=YYYY-MM-DD/gasoline_weekly_ny.xls
    <base>/eia_electricity_price/collected_date=YYYY-MM-DD/sales_revenue.xlsx

Silver 는 만들지 않습니다 — 두 원본을 합쳐 `gas_ev_price` Silver 에 씁니다
(`schema/silver/gas_ev_price.py`). 소스가 늘어도 Gold 가 읽는 자리는 하나입니다.

수집일 파티션인 이유
-------------------
`collected_date` 는 **EIA 파일을 받은 날**입니다. 파일 안에는 2000년(휘발유)·2010년
(전력)부터의 이력이 통째로 들어 있어서, 한 번 받으면 여러 달의 Silver 를 만들 수
있습니다. 다른 수집처럼 "하루 받으면 하루치"가 아닙니다.

그래서 이 데이터셋에는 "그 데이터가 나타내는 날짜" 라는 게 없습니다 — 한 파일이 26년을
담고 있으니 어떤 날짜를 붙여도 거짓말이 됩니다. 받은 날을 쓰는 것이 유일하게 정직합니다.
데이터 기간으로 나뉘는 것은 Silver 부터입니다(`year_month=`).

수집일로 나눠 **보관**하는 이유는 EIA 가 과거 값을 개정하기 때문입니다. 전력 통계는
최근 약 17개월을 `Preliminary` 로 표시하고 나중에 `Final` 로 확정합니다. 언제 받은
파일로 만든 Silver 인지 남겨두지 않으면 "지난달 결과와 왜 다르지"에 답할 수 없습니다.
"""

from datetime import date
from pathlib import Path

GAS_DATASET = "eia_gas_price"
ELECTRICITY_DATASET = "eia_electricity_price"
BRONZE_PARTITION_KEY = "collected_date"
GAS_FILE_NAME = "gasoline_weekly_ny.xls"
ELECTRICITY_FILE_NAME = "sales_revenue.xlsx"

# 원본이 형식만 바뀌어도 파싱은 예외 없이 이상한 값을 냅니다. 크기로 1차 확인하는데,
# 수집(lambda)과 검증(airflow)이 같은 하한을 봐야 하므로 여기 한 곳에 둡니다.
# 실측(2026-08-17 수집분): 휘발유 xls 99,840 bytes / 전력 xlsx 2,247,049 bytes.
GAS_MIN_BYTES = 10_000
ELECTRICITY_MIN_BYTES = 100_000


def _bronze_file(base_dir: str, dataset: str, file_name: str, collected_date: date) -> Path:
    return (
        Path(base_dir)
        / dataset
        / f"{BRONZE_PARTITION_KEY}={collected_date.isoformat()}"
        / file_name
    )


def gas_bronze_file(base_dir: str, collected_date: date) -> Path:
    return _bronze_file(base_dir, GAS_DATASET, GAS_FILE_NAME, collected_date)


def electricity_bronze_file(base_dir: str, collected_date: date) -> Path:
    return _bronze_file(base_dir, ELECTRICITY_DATASET, ELECTRICITY_FILE_NAME, collected_date)


def _bronze_key(dataset: str, file_name: str, collected_date: date) -> str:
    return f"bronze/{dataset}/{BRONZE_PARTITION_KEY}={collected_date.isoformat()}/{file_name}"


def gas_bronze_key(collected_date: date) -> str:
    return _bronze_key(GAS_DATASET, GAS_FILE_NAME, collected_date)


def electricity_bronze_key(collected_date: date) -> str:
    return _bronze_key(ELECTRICITY_DATASET, ELECTRICITY_FILE_NAME, collected_date)


def bronze_partitions(base_dir: str, dataset: str) -> list[tuple[date, Path]]:
    """수집일 오름차순 `collected_date=` 파티션 목록."""
    dataset_dir = Path(base_dir) / dataset
    if not dataset_dir.is_dir():
        raise FileNotFoundError(f"EIA Bronze 데이터셋이 없습니다: {dataset_dir}")

    partitions: list[tuple[date, Path]] = []
    for partition in dataset_dir.glob(f"{BRONZE_PARTITION_KEY}=*"):
        if not partition.is_dir():
            continue
        try:
            partition_date = date.fromisoformat(
                partition.name.removeprefix(f"{BRONZE_PARTITION_KEY}=")
            )
        except ValueError:
            continue
        partitions.append((partition_date, partition))
    if not partitions:
        raise FileNotFoundError(f"EIA Bronze 파티션이 없습니다: {dataset_dir}")
    return sorted(partitions)


def newest_bronze_partition(base_dir: str, dataset: str) -> tuple[date, Path]:
    """가장 최근에 받은 `collected_date=` 파티션.

    "대상 월 이하 최신" 이 아니라 **무조건 최신**인 이유
    ------------------------------------------------
    EIA 전력 통계는 약 3개월 늦게 공개됩니다. 그래서 M월 값은 M+3월쯤 받은 파일에만
    들어 있습니다. 예전에는 재현성을 위해 "대상 월 이하" 로 골랐는데, 그 규칙은 이
    지연과 정면으로 충돌해 **구조적으로 대상 월이 없는 파일**을 집었습니다. 자동
    스케줄로는 어느 달도 만들 수 없었습니다.

    최신을 쓰면 값도 더 정확합니다. EIA 는 최근 약 17개월을 `Preliminary` 로 표시하고
    나중에 `Final` 로 확정하는데, 최신 파일일수록 확정분이 많습니다. 대신 나중에 다시
    만들면 숫자가 달라질 수 있으므로, 어느 수집분과 어떤 확정 상태로 만들었는지를
    Silver 에 남겨 그 변화를 설명할 수 있게 합니다.

    특정 수집분으로 고정해야 하면 호출하는 쪽에서 경로를 직접 지정하면 됩니다.
    """
    return bronze_partitions(base_dir, dataset)[-1]


def is_duplicate_of_newest(base_dir: str, dataset: str, file_name: str, body: bytes) -> Path | None:
    """받은 내용이 최신 수집분과 같으면 그 경로를, 다르면 None 을 반환합니다.

    전력은 3개월에 한 번만 실제로 갱신되므로 월 1회 수집분 12개 중 8~9개가 바이트까지
    같습니다. 같은 것을 새 파티션으로 쌓지 않으면 파티션 개수 자체가 "언제 실제로
    바뀌었는지" 를 말해주는 기록이 됩니다.
    """
    try:
        _, partition = newest_bronze_partition(base_dir, dataset)
    except FileNotFoundError:
        return None
    path = partition / file_name
    if path.is_file() and path.read_bytes() == body:
        return path
    return None
