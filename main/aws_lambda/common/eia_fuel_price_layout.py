"""EIA 연료비 데이터셋의 저장 경로 규칙.

    <base>/eia_gas_price/year_month=YYYY-MM/collected_at=<UTC>/gasoline_weekly_ny.xls
    <base>/eia_electricity_price/year_month=YYYY-MM/collected_at=<UTC>/sales_revenue.xlsx

Silver 는 만들지 않습니다 — 두 원본을 합쳐 `gas_ev_price` Silver 에 씁니다
(`schema/silver/gas_ev_price.py`). 소스가 늘어도 Gold 가 읽는 자리는 하나입니다.

수집 시각 버전인 이유
---------------------
`collected_at` 은 **EIA 파일을 받은 시각**입니다. 파일 안에는 2000년(휘발유)·2010년
(전력)부터의 이력이 통째로 들어 있어서, 한 번 받으면 여러 달의 Silver 를 만들 수
있습니다. 다른 수집처럼 "하루 받으면 하루치"가 아닙니다.

그래서 이 데이터셋에는 "그 데이터가 나타내는 날짜" 라는 게 없습니다 — 한 파일이 26년을
담고 있으니 어떤 날짜를 붙여도 거짓말이 됩니다. 받은 날을 쓰는 것이 유일하게 정직합니다.
데이터 기간으로 나뉘는 것은 Silver 부터입니다(`year_month=`).

수집일로 나눠 **보관**하는 이유는 EIA 가 과거 값을 개정하기 때문입니다. 전력 통계는
최근 약 17개월을 `Preliminary` 로 표시하고 나중에 `Final` 로 확정합니다. 언제 받은
파일로 만든 Silver 인지 남겨두지 않으면 "지난달 결과와 왜 다르지"에 답할 수 없습니다.
"""

from pathlib import Path, PurePosixPath

from main.aws_lambda.common.monthly_dataset import (
    collected_at_from_token,
    collected_at_token,
    join_segments,
    service_area_segment,
)
from shared.common.eia_fuel_version import (
    require_collected_at_token,
    source_collected_at_token,
)
from shared.common.success_marker import data_key_is_complete, marker_path

GAS_DATASET = "eia_gas_price"
ELECTRICITY_DATASET = "eia_electricity_price"
BRONZE_PARTITION_KEY = "year_month"
GAS_FILE_NAME = "gasoline_weekly_ny.xls"
ELECTRICITY_FILE_NAME = "sales_revenue.xlsx"

# 원본이 형식만 바뀌어도 파싱은 예외 없이 이상한 값을 냅니다. 크기로 1차 확인하는데,
# 수집(lambda)과 검증(airflow)이 같은 하한을 봐야 하므로 여기 한 곳에 둡니다.
# 실측(2026-08-17 수집분): 휘발유 xls 99,840 bytes / 전력 xlsx 2,247,049 bytes.
GAS_MIN_BYTES = 10_000
ELECTRICITY_MIN_BYTES = 100_000


def _bronze_file(
    base_dir: str,
    dataset: str,
    file_name: str,
    collected_at: str,
    service_area: str,
) -> Path:
    dataset_root = Path(base_dir) / dataset
    area = service_area_segment(service_area)
    token = collected_at_token(collected_at)
    year_month = collected_at_from_token(token)[:7]
    return (
        (dataset_root / area)
        / f"{BRONZE_PARTITION_KEY}={year_month}"
        / f"collected_at={token}"
        / file_name
    )


def gas_bronze_file(
    base_dir: str, collected_at: str, service_area: str
) -> Path:
    return _bronze_file(
        base_dir, GAS_DATASET, GAS_FILE_NAME, collected_at, service_area
    )


def electricity_bronze_file(
    base_dir: str, collected_at: str, service_area: str
) -> Path:
    return _bronze_file(
        base_dir,
        ELECTRICITY_DATASET,
        ELECTRICITY_FILE_NAME,
        collected_at,
        service_area,
    )


def _bronze_key(
    dataset: str,
    file_name: str,
    collected_at: str,
    service_area: str,
) -> str:
    token = collected_at_token(collected_at)
    year_month = collected_at_from_token(token)[:7]
    return join_segments(
        "bronze",
        dataset,
        service_area_segment(service_area),
        f"{BRONZE_PARTITION_KEY}={year_month}",
        f"collected_at={token}",
        file_name,
    )


def gas_bronze_key(collected_at: str, service_area: str) -> str:
    return _bronze_key(GAS_DATASET, GAS_FILE_NAME, collected_at, service_area)


def electricity_bronze_key(
    collected_at: str, service_area: str
) -> str:
    return _bronze_key(
        ELECTRICITY_DATASET, ELECTRICITY_FILE_NAME, collected_at, service_area
    )


def bronze_collected_at(location: str | Path) -> str:
    """Bronze 파일 경로에서 검증된 UTC 수집 시각을 복원합니다."""
    path = PurePosixPath(str(location).split("://", 1)[-1])
    token = require_collected_at_token(
        path.parent.name.removeprefix("collected_at=")
    )
    collected_at = collected_at_from_token(token)
    if (
        path.parent.name != f"collected_at={token}"
        or path.parent.parent.name != f"year_month={collected_at[:7]}"
    ):
        raise ValueError(f"EIA Bronze 경로의 수집 버전이 올바르지 않습니다: {location}")
    return collected_at


def silver_source_collected_at(location: str | Path) -> str:
    """CLEAN Silver 파일 경로에서 Bronze 수집 시각을 복원합니다."""
    path = PurePosixPath(str(location).split("://", 1)[-1])
    token = source_collected_at_token(path.parent.name)
    if token is None:
        raise ValueError(f"EIA Silver 경로의 원천 버전이 올바르지 않습니다: {location}")
    return collected_at_from_token(token)


def bronze_s3_prefix(dataset: str, service_area: str) -> str:
    """지역 아래 모든 월별 Bronze 버전을 나열하는 S3 접두사."""
    return join_segments(
        "bronze", dataset, service_area_segment(service_area)
    ) + "/"


def newest_bronze_s3_key(
    keys: list[str],
    dataset: str,
    file_name: str,
    service_area: str,
) -> tuple[str, str]:
    """완료된 S3 Bronze 중 가장 최근 `collected_at` 버전을 고릅니다.

    `service_area` 는 `keys` 를 만들 때 쓴 접두사와 **같아야** 합니다 — 어긋나면
    `key[len(prefix):]` 오프셋이 밀려 날짜 파싱이 전부 실패하고, `except ValueError:
    continue` 에 걸려 **조용히 빈 목록**이 됩니다.
    """
    prefix = bronze_s3_prefix(dataset, service_area)
    key_set = set(keys)
    versions: list[tuple[str, str]] = []
    for key in keys:
        if not key.startswith(prefix) or not data_key_is_complete(key, key_set):
            continue
        parts = key.removeprefix(prefix).split("/")
        if len(parts) != 3 or parts[2] != file_name:
            continue
        year_month, version = parts[:2]
        if not year_month.startswith(f"{BRONZE_PARTITION_KEY}="):
            continue
        token = version.removeprefix("collected_at=")
        if version != f"collected_at={token}":
            continue
        try:
            require_collected_at_token(token)
            collected_at = collected_at_from_token(token)
        except ValueError:
            continue
        if year_month != f"{BRONZE_PARTITION_KEY}={collected_at[:7]}":
            continue
        versions.append((token, key))
    if not versions:
        raise FileNotFoundError(f"EIA Bronze S3 파티션이 없습니다: {prefix}")
    token, key = max(versions)
    return collected_at_from_token(token), key


def bronze_partitions(
    base_dir: str, dataset: str, service_area: str
) -> list[tuple[str, Path]]:
    """수집 시각 오름차순의 완료된 Bronze 버전 목록."""
    dataset_root = Path(base_dir) / dataset
    area = service_area_segment(service_area)
    dataset_dir = dataset_root / area
    if not dataset_dir.is_dir():
        raise FileNotFoundError(f"EIA Bronze 데이터셋이 없습니다: {dataset_dir}")

    file_name = GAS_FILE_NAME if dataset == GAS_DATASET else ELECTRICITY_FILE_NAME
    versions: list[tuple[str, Path]] = []
    for version in dataset_dir.glob(f"{BRONZE_PARTITION_KEY}=*/collected_at=*"):
        if (
            not version.is_dir()
            or not (version / file_name).is_file()
            or not marker_path(version).is_file()
        ):
            continue
        try:
            token = require_collected_at_token(
                version.name.removeprefix("collected_at=")
            )
            collected_at = collected_at_from_token(token)
        except ValueError:
            continue
        if version.parent.name != f"{BRONZE_PARTITION_KEY}={collected_at[:7]}":
            continue
        versions.append((token, version))
    if not versions:
        raise FileNotFoundError(f"EIA Bronze 파티션이 없습니다: {dataset_dir}")
    return [
        (collected_at_from_token(token), version)
        for token, version in sorted(versions)
    ]


def newest_bronze_partition(
    base_dir: str, dataset: str, service_area: str
) -> tuple[str, Path]:
    """가장 최근에 받은 `collected_at=` 버전.

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
    return bronze_partitions(base_dir, dataset, service_area)[-1]


def is_duplicate_of_newest(
    base_dir: str,
    dataset: str,
    file_name: str,
    body: bytes,
    service_area: str,
) -> Path | None:
    """받은 내용이 최신 수집분과 같으면 그 경로를, 다르면 None 을 반환합니다.

    전력은 3개월에 한 번만 실제로 갱신되므로 월 1회 수집분 12개 중 8~9개가 바이트까지
    같습니다. 같은 것을 새 파티션으로 쌓지 않으면 파티션 개수 자체가 "언제 실제로
    바뀌었는지" 를 말해주는 기록이 됩니다.
    """
    # 지역을 안 넘기면 지역 계층 아래 파티션을 못 찾아 FileNotFoundError → None →
    # **dedup 이 조용히 꺼집니다**(매번 새 파티션을 쓰지만 에러가 없음). 호출부가
    # 쓰기와 같은 지역을 넘기는지 반드시 확인하세요(#843, #844).
    try:
        _, partition = newest_bronze_partition(base_dir, dataset, service_area)
    except FileNotFoundError:
        return None
    path = partition / file_name
    if path.is_file() and path.read_bytes() == body:
        return path
    return None
