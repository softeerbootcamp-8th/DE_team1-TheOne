"""NYC 합성 원천의 published S3 저장 계약.

1. 데이터셋 3종·manifest·품질 리포트가 모두 source/published/NYC 아래에 있다.
2. 운행 데이터셋은 과거 hvfhv_taxi_trips가 아닌 monthly_taxi_trip을 쓴다.
"""

from shared.common.source_published_layout import (
    PUBLISHED_DATASETS,
    dataset_key,
    manifest_key,
    quality_report_key,
)


def test_published의_다섯_폴더는_NYC_아래에_모인다():
    year_month = "2026-08"
    keys = {
        *(dataset_key(dataset, year_month) for dataset in PUBLISHED_DATASETS),
        manifest_key(year_month),
        quality_report_key(year_month),
    }

    folders = {key.removeprefix("source/published/NYC/").split("/", 1)[0] for key in keys}
    assert folders == {
        "monthly_taxi_trip",
        "driver_vehicle_monthly_snapshot",
        "lease_vehicle_inventory",
        "_manifests",
        "_quality_reports",
    }
    assert all(key.startswith("source/published/NYC/") for key in keys)
    assert all("hvfhv_taxi_trips" not in key for key in keys)
