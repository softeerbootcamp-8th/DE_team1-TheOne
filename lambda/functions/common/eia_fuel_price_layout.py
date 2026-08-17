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

그래도 수집일로 나누는 이유는 **EIA 가 과거 값을 개정하기 때문**입니다. 특히 월간
전력 통계는 발표 후 몇 달간 바뀝니다. 언제 받은 파일로 만든 Silver 인지 남겨두지
않으면 "지난달 결과와 왜 다르지"에 답할 수 없습니다.
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


def latest_bronze_partition(base_dir: str, dataset: str, as_of: date) -> Path:
    """`as_of` 이하 최신 `collected_date=` 파티션.

    대상 월보다 **뒤에** 받은 파일도 그 달 값을 담고 있습니다(이력 파일이라). 그래도
    `as_of` 를 넘기지 않는 쪽을 우선하는 것은, 과거 달을 다시 만들 때 그 사이 개정된
    값이 섞여 결과가 달라지는 것을 막기 위해서입니다.
    """
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

    within = [item for item in partitions if item[0] <= as_of]
    # 대상 월이 첫 수집보다 과거면 `as_of` 이하가 없습니다. 이력 파일이라 나중에 받은
    # 것에도 그 달 값이 있으므로, 가장 오래된 수집분으로 물러섭니다.
    chosen = max(within, key=lambda item: item[0]) if within else min(partitions, key=lambda item: item[0])
    return chosen[1]
