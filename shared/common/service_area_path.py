"""저장 경로의 지역(`service_area=`) 계층을 만들고 찾는 단일 규칙. (#674, #839)

**이 모듈이 삽입 규칙의 유일한 정의입니다.** Bronze/Silver/Gold, 로컬/S3, Airflow/Spark/
Lambda 세 런타임이 모두 여기를 통과해야 규칙이 갈라지지 않습니다. `shared/common` 은
세 런타임이 전부 import 하는 유일한 위치라서 여기에 둡니다 — 그래서 **표준 라이브러리만
씁니다**(런타임마다 lockfile 이 분리돼 있어 의존성을 추가하면 그 분리가 무너집니다).

## 삽입 규칙

`service_area=<sa>` 는 **파티션 키 디렉터리 바로 위, 데이터셋 디렉터리 바로 아래**에
들어갑니다. 파티션 키 이름은 데이터셋에 따라 다릅니다.

```
# API 3종 (Bronze 는 year_month=, Silver 도 year_month=)
<base>/<dataset>/service_area=NYC/year_month=2026-08/collected_at=<token>/data.parquet
<base>/service_area=NYC/year_month=2026-08/source_collected_at=<token>/part-*.parquet

# EIA 2종 — Bronze 축이 collected_date= 다름
<base>/<dataset>/service_area=NYC/collected_date=2026-08-01/<file>
<base>/<dataset>/service_area=NYC/year_month=2026-08/<dataset>.parquet
```

## 전환 전략 (#839 → #840~848 → #849)

`service_area=None` 이면 **지역 계층 없이 지금과 완전히 같은 경로**를 만듭니다. 그래서
데이터셋별로 하나씩 옮길 수 있고(#840~#848), 읽는 쪽은 `candidate_prefixes` 로 지역
경로를 먼저 보고 없으면 지역 없는 경로를 봅니다. 모든 writer 가 옮겨지면 #849 에서
`None` 허용과 폴백을 제거합니다.

`latest_local_silver_version` 계열의 구/신 레이아웃 이중 읽기와는 **다른 층위**입니다 —
그건 *같은 파티션 디렉터리 안* 이고, 여기는 *파티션 디렉터리의 위치* 입니다.
"""

import re
from pathlib import Path


SERVICE_AREA_KEY = "service_area"
# main.airflow.common.assets.SERVICE_AREA_PATTERN 과 같은 규칙입니다. 그 모듈은
# airflow.sdk 를 import 하므로 Lambda·Spark 에서 못 씁니다. 규칙이 갈라지면 파티션
# 키와 경로가 서로 다른 지역을 가리키므로, 양쪽을 바꿀 때 반드시 함께 바꾸세요.
SERVICE_AREA_PATTERN = re.compile(r"^[A-Z][A-Z0-9_]*$")


def validate_service_area(service_area: str) -> str:
    """지역 코드 형식을 확인합니다. 경로에 쓰기 전 항상 통과시킵니다."""
    if not SERVICE_AREA_PATTERN.fullmatch(service_area or ""):
        raise ValueError(
            f"service_area 는 대문자 코드여야 합니다(예: NYC): {service_area!r}"
        )
    return service_area


def service_area_segment(service_area: str | None) -> str:
    """경로에 끼울 `service_area=<sa>` 세그먼트. `None` 이면 빈 문자열입니다.

    빈 문자열을 돌려주는 것이 전환 전략의 핵심입니다 — 호출부가 분기 없이
    `*filter(None, (...))` 로 조립할 수 있습니다.
    """
    if service_area is None:
        return ""
    return f"{SERVICE_AREA_KEY}={validate_service_area(service_area)}"


def join_segments(*segments: str | None) -> str:
    """빈 세그먼트를 버리고 `/` 로 잇습니다. S3 키 조립용입니다.

    `service_area_segment(None)` 이 빈 문자열이라, 지역이 없을 때 `//` 가 생기거나
    앞뒤에 `/` 가 남는 것을 여기서 막습니다.
    """
    return "/".join(segment for segment in segments if segment)


def candidate_segments(service_area: str | None) -> tuple[str | None, ...]:
    """읽는 쪽이 시도할 지역 세그먼트를 **우선순위 순서**로 돌려줍니다.

    지역이 주어지면 `(지역 세그먼트, None)` — 지역 경로를 먼저 보고, 없으면 아직
    옮겨지지 않은 지역 없는 경로를 봅니다. 이 폴백이 있어야 데이터셋별로 하나씩
    writer 를 옮길 수 있습니다(#840~#848).

    지역이 없으면 `(None,)` 뿐입니다 — 지금 동작 그대로입니다.

    #849 에서 이 함수가 폴백을 빼고 `(지역 세그먼트,)` 만 돌려주도록 바뀝니다.
    """
    if service_area is None:
        return (None,)
    return (service_area_segment(service_area), None)


def gold_csv_path(
    output_dir: str,
    dataset: str,
    year_month: str,
    service_area: str | None = None,
) -> Path:
    """Gold 산출물 CSV 경로. Spark(쓰기)와 Airflow(검증)가 **같은 함수**를 씁니다.

    전에는 `main/spark/.../job.py:_csv_path` 와
    `main/airflow/.../tasks.py:validate_gold_outputs` 가 같은 경로를 각각 조립해서,
    한쪽만 고치면 **검증이 엉뚱한 곳을 보고도 통과**할 수 있었습니다. 두 런타임이
    모두 import 하는 `shared/common` 으로 모아 그 어긋남을 구조적으로 막습니다.

    반환형이 `Path` 라 pathlib 만 쓰며, 로컬 산출물 전용입니다(운영 Gold 는 Postgres).
    """
    dataset_root = Path(output_dir) / dataset
    area = service_area_segment(service_area)
    return (
        (dataset_root / area if area else dataset_root)
        / f"year_month={year_month}"
        / f"{dataset}.csv"
    )


def candidate_roots(root: str | Path, service_area: str | None = None) -> tuple[Path, ...]:
    """읽는 쪽이 시도할 **로컬** 데이터셋 루트를 우선순위 순서로 돌려줍니다.

    ```
    candidate_roots("/silver/ds", "NYC")  # (/silver/ds/service_area=NYC, /silver/ds)
    candidate_roots("/silver/ds", None)   # (/silver/ds,)
    ```

    지역 경로를 **먼저** 봅니다. 순서가 뒤집히면 이미 옮긴 데이터셋이 옛 경로의 낡은
    데이터를 조용히 집어갑니다. 직접 조립하지 말고 이 함수를 쓰세요.
    """
    base = Path(root)
    return tuple(
        base / segment if segment else base
        for segment in candidate_segments(service_area)
    )


def candidate_prefixes(*head: str, service_area: str | None = None) -> tuple[str, ...]:
    """읽는 쪽이 시도할 **S3 키 접두사**를 우선순위 순서로 돌려줍니다.

    `head` 는 데이터셋 루트까지의 세그먼트입니다. 반환값에는 뒤에 `/` 가 없으므로
    호출부가 `f"{prefix}/year_month={ym}/"` 처럼 이어 붙입니다.

    ```
    candidate_prefixes("bronze", "ds", service_area="NYC")
    # ("bronze/ds/service_area=NYC", "bronze/ds")
    ```
    """
    return tuple(
        join_segments(*head, segment) if segment else join_segments(*head)
        for segment in candidate_segments(service_area)
    )
