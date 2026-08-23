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


def dataset_key(dataset: str, year_month: str) -> str:
    return f"{S3_PUBLISHED_PREFIX}/{dataset}/year_month={year_month}/data.parquet"


def manifest_key(year_month: str) -> str:
    return f"{S3_PUBLISHED_PREFIX}/_manifests/year_month={year_month}.json"


def quality_report_key(year_month: str) -> str:
    return f"{S3_PUBLISHED_PREFIX}/_quality_reports/year_month={year_month}.json"
