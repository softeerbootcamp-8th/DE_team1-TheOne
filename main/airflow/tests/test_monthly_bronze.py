"""월별 Silver 최종 버전 경로와 `_SUCCESS` 공개 버전 선택 계약."""

from main.airflow.common import monthly_bronze
from shared.airflow.common.validation import S3Location


TOKEN = "20260811T085354000000Z"


def _result(year_month: str = "2026-07") -> dict:
    return {
        "locations": [
            "/bronze/monthly_taxi_trip/year_month=2026-07/"
            f"collected_at={TOKEN}/data.parquet"
        ],
        "row_count": 1,
        "year_month": year_month,
        "collected_at": "2026-08-11T08:53:54.000000Z",
    }


def test_로컬_Silver는_source_collected_at_최종경로를_계산한다(tmp_path):
    version = monthly_bronze.silver_version_path(tmp_path, _result(), "NYC")

    assert version == (
        tmp_path
        / "service_area=NYC"
        / "year_month=2026-07"
        / f"source_collected_at={TOKEN}"
    )


def test_S3_Silver도_source_collected_at_최종경로를_계산한다():
    result = _result()
    result["locations"] = [
        "s3://de-theone/bronze/monthly_taxi_trip/year_month=2026-07/"
        f"collected_at={TOKEN}/data.parquet"
    ]

    version = monthly_bronze.silver_version_path(
        "s3://de-theone/silver", result, "NYC"
    )

    assert version == S3Location(
        "de-theone",
        f"silver/service_area=NYC/year_month=2026-07/source_collected_at={TOKEN}",
    )


def test_로컬_최신_Silver는_SUCCESS가_있는_버전만_고른다(tmp_path):
    partition = tmp_path / "year_month=2026-07"
    completed = partition / "source_collected_at=20260811T085354000000Z"
    incomplete = partition / "source_collected_at=20260812T085354000000Z"
    for version in (completed, incomplete):
        version.mkdir(parents=True)
        (version / "data.parquet").write_bytes(b"data")
    (completed / "_SUCCESS").touch()

    assert monthly_bronze.latest_local_silver_version(partition) == completed


def test_SUCCESS가_없으면_Silver를_공개본으로_세지_않는다(tmp_path):
    partition = tmp_path / "year_month=2026-07"
    version = partition / "source_collected_at=20260811T085354000000Z"
    version.mkdir(parents=True)
    (version / "part-00000.parquet").write_bytes(b"data")

    assert monthly_bronze.latest_local_silver_version(partition) is None
