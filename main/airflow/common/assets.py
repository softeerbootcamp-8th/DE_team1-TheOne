"""Gold 입력으로 쓰는 월별 Silver Asset 정의와 파티션 키 규약.

URI 는 저장 위치가 아니라 데이터 제품의 논리 이름입니다. 그래서 로컬 파일과 S3
어느 쪽에서 실행해도 생산자와 Gold DAG 가 같은 Asset 을 봅니다. 실제 대상은 각
이벤트의 파티션 키로 구분합니다.

파티션 키는 ``"{service_area}:{year_month}"`` 복합 문자열입니다(예: ``"NYC:2026-08"``).
Airflow 의 asset 파티션은 키를 불투명한 문자열로 다루므로, 이 형식만 바꾸면
``PartitionedAssetTimetable``/``IdentityMapper`` 코드를 손대지 않고도 지역별로 완전히
독립된 파티션이 됩니다 — Gold 는 ``"NYC:2026-08"`` 의 소스가 다 준비되면 그때
트리거되고 ``"TX:2026-08"`` 은 그와 무관하게 따로 트리거됩니다. **지역마다 새 DAG 를
만들 필요가 없습니다**(#674).

**왜 두 축을 나누지 않고 한 문자열로 합치나**: Airflow 에 다차원 파티션 키라는 개념이
없습니다. ``add_partitions(keys: str | list[str])``
(``airflow/sdk/types.py``)는 문자열만 받고, ``DagRun.partition_key`` 도 문자열 컬럼
하나입니다(``airflow/models/dagrun.py``, str 이 아니면 생성 시점에 거부). 키 길이 상한은
250자입니다. 축을 정말 나누려면 지역마다 Asset·DAG 를 복제해야 하는데, 그러면 위에
적은 "지역마다 새 DAG 를 만들지 않는다" 는 이점을 잃습니다. 그래서 **합치는 것은 API
제약이지 설계 선호가 아닙니다.**

반대로 **저장 경로는 축을 나눕니다** — ``service_area=<sa>/year_month=<ym>/`` Hive 스타일
두 계층(로컬·S3 동일). 경로는 다축을 지원하므로 지역 하위에서 기존 ``year_month=*``
glob 이 그대로 동작하고 파티션 프루닝도 축별로 걸립니다(#810).

`service_area` 는 AWS 리전(`region_name`, `AWS_REGION`)과 헷갈리지 않도록 고른
이름입니다. 같은 오퍼레이터 호출 안에 두 개념이 나란히 놓이기 때문입니다.
"""

import re
from pathlib import Path

from airflow.sdk import Asset


FUEL_PRICE_SILVER = Asset("silver://gas_ev_price")
API_SILVER_REFRESH_READY = Asset("silver://api_refresh_ready")
GOLD_INPUTS_READY = Asset("silver://gold_inputs_ready")

# API 3종은 감시 DAG가 변경된 Silver 실행을 모두 기다린 뒤 READY를 한 번만 냅니다.
# 같은 파티션의 두 입력이 처음 모이면 Gold를 실행합니다. Gold 입력 검증이 성공하면
# GOLD_INPUTS_READY를 같은 키로 발행해 준비 상태를 남기므로, 그다음부터는 두 입력 중
# 하나만 갱신돼도 같은 파티션을 다시 실행합니다.
GOLD_INPUTS = (
    (API_SILVER_REFRESH_READY & FUEL_PRICE_SILVER)
    | (GOLD_INPUTS_READY & (API_SILVER_REFRESH_READY | FUEL_PRICE_SILVER))
)

# 지금 서비스하는 지역은 뉴욕 하나입니다. 지역이 늘면 지역별 설정(EIA 시리즈 URL,
# 택시존 스키마 등)을 담을 레지스트리가 필요한데, 항목이 하나뿐인 레지스트리는
# 과잉이라 그 시점에 만듭니다(#810).
DEFAULT_SERVICE_AREA = "NYC"
# 서로 다른 지역 파티션은 저장 경로와 Gold 자연 키가 격리되어 있으므로 동시에
# 실행할 수 있습니다. 단일 EC2 LocalExecutor의 초기 운영 상한은 3개이며,
# 지역 수가 이 값을 넘으면 나머지 DagRun은 큐에서 기다립니다.
MAX_ACTIVE_SERVICE_AREA_RUNS = 3
PARTITION_KEY_SEPARATOR = ":"
# 구분자를 값에 넣을 수 없게 막습니다. 허용하면 "nyc:2026-08" 같은 값이
# service_area 로 들어와 키가 "nyc:2026-08:2026-08" 이 되고, 파싱이 조용히 엉뚱한
# 값을 돌려줍니다.
SERVICE_AREA_PATTERN = re.compile(r"^[A-Z][A-Z0-9_]*$")
YEAR_MONTH_PATTERN = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")


def service_area_segment(service_area: str) -> str:
    if not SERVICE_AREA_PATTERN.fullmatch(service_area):
        raise ValueError(
            f"service_area 는 대문자 코드여야 합니다(예: NYC): {service_area!r}"
        )
    return f"service_area={service_area}"


def join_segments(*segments: str | None) -> str:
    return "/".join(segment for segment in segments if segment)


def service_area_root(root: str | Path, service_area: str) -> Path:
    return Path(root) / service_area_segment(service_area)


def service_area_prefix(*head: str, service_area: str) -> str:
    return join_segments(*head, service_area_segment(service_area))


def gold_csv_path(
    output_dir: str,
    dataset: str,
    year_month: str,
    service_area: str,
) -> Path:
    dataset_root = Path(output_dir) / dataset
    area = service_area_segment(service_area)
    return (
        (dataset_root / area)
        / f"year_month={year_month}"
        / f"{dataset}.csv"
    )


def build_partition_key(service_area: str, year_month: str) -> str:
    """생산자가 발행할 복합 파티션 키를 만듭니다."""
    if not SERVICE_AREA_PATTERN.fullmatch(service_area or ""):
        raise ValueError(
            f"service_area 는 대문자 코드여야 합니다(예: NYC): {service_area!r}"
        )
    if not YEAR_MONTH_PATTERN.fullmatch(year_month or ""):
        raise ValueError(f"year_month 가 YYYY-MM 형식이 아닙니다: {year_month!r}")
    return f"{service_area}{PARTITION_KEY_SEPARATOR}{year_month}"


def parse_partition_key(partition_key: str) -> tuple[str, str]:
    """소비자가 복합 파티션 키를 (service_area, year_month) 로 나눠 읽습니다.

    지역 성분이 없는 옛 형식(`"2026-08"`)은 **일부러 받지 않습니다.** 받아서
    기본 지역으로 넘기면 "생산자를 아직 안 고쳤다" 는 사실이 조용히 묻힙니다 —
    이 변경에서 가장 위험한 실패가 생산자와 소비자 중 한쪽만 바뀌어 Gold 가
    아무 에러 없이 안 도는 것이라, 형식이 어긋나면 요란하게 실패하는 쪽을 택합니다.
    """
    service_area, separator, year_month = str(partition_key or "").partition(
        PARTITION_KEY_SEPARATOR
    )
    if not separator:
        raise ValueError(
            "파티션 키에 지역 성분이 없습니다. "
            f'"{{service_area}}{PARTITION_KEY_SEPARATOR}{{year_month}}" 형식이어야 합니다: '
            f"{partition_key!r}"
        )
    # 다시 검증을 태워 생산자·소비자가 같은 규약을 쓰는지 확인합니다.
    build_partition_key(service_area, year_month)
    return service_area, year_month


def resolve_service_area(params: dict) -> str:
    """DAG 파라미터에서 대상 지역을 고릅니다. 비어 있으면 기본 지역입니다."""
    service_area = str((params or {}).get("service_area") or "").strip()
    if not service_area:
        return DEFAULT_SERVICE_AREA
    if not SERVICE_AREA_PATTERN.fullmatch(service_area):
        raise ValueError(
            f"service_area 는 대문자 코드여야 합니다(예: NYC): {service_area!r}"
        )
    return service_area


def publish_month_partition(
    outlet_events,
    asset: Asset,
    year_month: str,
    service_area: str = DEFAULT_SERVICE_AREA,
) -> None:
    """태스크 검증을 통과한 (지역, 월) 을 partition-aware Asset 이벤트로 기록합니다."""
    if outlet_events is not None:
        outlet_events[asset].add_partitions(
            build_partition_key(service_area, year_month)
        )
