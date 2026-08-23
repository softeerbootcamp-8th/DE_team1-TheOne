"""NYC 합성 원천의 S3 published 저장 계약.

다른 지역은 각 원천 시스템이 자기 지역 코드를 사용합니다. 이 원천 시스템은 NYC
데이터만 만들기 때문에 지역 코드를 설정값으로 열지 않고 고정합니다.
"""

PUBLISHED_SERVICE_AREA = "NYC"
PUBLISHED_DATASETS = frozenset(
    {
        "monthly_taxi_trip",
        "driver_vehicle_monthly_snapshot",
        "lease_vehicle_inventory",
    }
)
S3_PUBLISHED_PREFIX = f"source/published/{PUBLISHED_SERVICE_AREA}"
# EMR executor가 릴리스 생성 중 읽는 작은 중간 뷰입니다. 전월 상태의 정본은
# 아래 경로가 아니라 published 데이터셋/manifest이며, 이 경로는 재시도 때
# 덮어쓸 수 있는 런타임 캐시로만 사용합니다.
S3_PUBLISHED_RUNTIME_PREFIX = f"{S3_PUBLISHED_PREFIX}/_runtime/synthetic_driver_trip"


def dataset_key(dataset: str, year_month: str) -> str:
    return f"{S3_PUBLISHED_PREFIX}/{dataset}/year_month={year_month}/data.parquet"


def manifest_key(year_month: str) -> str:
    return f"{S3_PUBLISHED_PREFIX}/_manifests/year_month={year_month}.json"


def quality_report_key(year_month: str) -> str:
    return f"{S3_PUBLISHED_PREFIX}/_quality_reports/year_month={year_month}.json"
