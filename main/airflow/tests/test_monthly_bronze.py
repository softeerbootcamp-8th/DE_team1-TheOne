"""Silver 버전 경로 계산과 staging→최종 커밋(#742) 단위 테스트.

1. staging 경로는 확장자를 `.parquet`로 유지하되 TIMESTAMP_FILE_PATTERN과는
   겹치지 않는다(로컬·S3 모두) — parquet_file()의 확장자 검사는 통과하면서
   "최신 버전" 탐색에서는 제외되게 하려는 것.
2. 로컬 커밋은 staging 파일을 최종 경로로 원자적 rename한다.
3. S3 커밋은 copy 후 staging 객체를 delete한다.
4. staged와 final의 위치 종류(로컬/S3)가 다르면 커밋을 거부한다.
"""

from pathlib import Path

import pytest

from main.airflow.common import monthly_bronze
from shared.airflow.common.validation import S3Location


def _result(year_month: str = "2026-07") -> dict:
    return {
        "locations": ["/bronze/monthly_taxi_trip/year_month=2026-07/20260811T085354000000Z.parquet"],
        "row_count": 1,
        "year_month": year_month,
    }


def test_로컬_staging_경로는_확장자를_유지한채_최종경로와_구분된다(tmp_path):
    final = monthly_bronze.silver_version_path(tmp_path, _result())
    staged = monthly_bronze.staged_silver_version_path(tmp_path, _result())

    assert staged == final.with_name("20260811T085354000000Z.staged.parquet")
    assert staged.suffix == ".parquet"
    assert not monthly_bronze.TIMESTAMP_FILE_PATTERN.fullmatch(staged.name)


def test_S3_staging_경로도_확장자를_유지한채_최종키와_구분된다():
    result = _result()
    result["locations"] = [
        "s3://de-theone/bronze/monthly_taxi_trip/year_month=2026-07/"
        "20260811T085354000000Z.parquet"
    ]
    final = monthly_bronze.silver_version_path("s3://de-theone/silver", result)
    staged = monthly_bronze.staged_silver_version_path("s3://de-theone/silver", result)

    assert isinstance(staged, S3Location)
    parent = final.key.rsplit("/", 1)[0]
    assert staged == S3Location(final.bucket, f"{parent}/20260811T085354000000Z.staged.parquet")


def test_로컬_커밋은_staging_파일을_최종경로로_옮긴다(tmp_path):
    staged = tmp_path / "20260811T085354000000Z.staged.parquet"
    staged.write_bytes(b"data")
    final = tmp_path / "year_month=2026-07" / "20260811T085354000000Z.parquet"

    monthly_bronze.commit_staged_silver(staged, final)

    assert final.read_bytes() == b"data"
    assert not staged.exists()


def test_S3_커밋은_copy후_staging객체를_지운다(monkeypatch):
    calls = []

    class FakeClient:
        def copy(self, source, bucket, key):
            calls.append(("copy", source, bucket, key))

        def delete_object(self, Bucket, Key):
            calls.append(("delete", Bucket, Key))

    monkeypatch.setattr(monthly_bronze.boto3, "client", lambda name: FakeClient())
    staged = S3Location("de-theone", "silver/x/year_month=2026-07/f.staged.parquet")
    final = S3Location("de-theone", "silver/x/year_month=2026-07/f.parquet")

    monthly_bronze.commit_staged_silver(staged, final)

    assert calls == [
        ("copy", {"Bucket": staged.bucket, "Key": staged.key}, final.bucket, final.key),
        ("delete", staged.bucket, staged.key),
    ]


def test_staged와_final의_위치종류가_다르면_커밋을_거부한다(tmp_path):
    staged = tmp_path / "f.staged.parquet"
    final = S3Location("bucket", "key")

    with pytest.raises(TypeError):
        monthly_bronze.commit_staged_silver(staged, final)

    with pytest.raises(TypeError):
        monthly_bronze.commit_staged_silver(final, Path(tmp_path / "f.parquet"))
