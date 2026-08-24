"""월별 Silver 버전 디렉터리 계산과 staging→공개 승격 계약.

1. Bronze collected_at은 동일한 source_collected_at 최종·staging 디렉터리를 만듦
2. 로컬은 검증된 part와 _SUCCESS를 함께 최종 디렉터리로 승격
3. S3는 기존 공개 marker를 먼저 지우고 part 복사 후 _SUCCESS를 마지막에 생성
4. staged와 final의 위치 종류가 다르면 승격 거부
5. Spark는 part만, Lambda는 data.parquet 하나만 공개 허용
"""

import pytest

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


def test_로컬_Silver는_source_collected_at_최종과_staging경로를_계산한다(tmp_path):
    final = monthly_bronze.silver_version_path(tmp_path, _result(), "NYC")
    staged = monthly_bronze.staged_silver_version_path(tmp_path, _result(), "NYC")

    assert final == (
        tmp_path / "service_area=NYC" / "year_month=2026-07"
        / f"source_collected_at={TOKEN}"
    )
    assert staged == final.parent / ".staging" / final.name


def test_S3_Silver도_source_collected_at_최종과_staging경로를_계산한다():
    result = _result()
    result["locations"] = [
        "s3://de-theone/bronze/monthly_taxi_trip/year_month=2026-07/"
        f"collected_at={TOKEN}/data.parquet"
    ]

    final = monthly_bronze.silver_version_path(
        "s3://de-theone/silver", result, "NYC"
    )
    staged = monthly_bronze.staged_silver_version_path(
        "s3://de-theone/silver", result, "NYC"
    )

    assert final == S3Location(
        "de-theone",
        f"silver/service_area=NYC/year_month=2026-07/source_collected_at={TOKEN}",
    )
    assert staged == S3Location(
        "de-theone",
        f"silver/service_area=NYC/year_month=2026-07/.staging/source_collected_at={TOKEN}",
    )


def test_로컬_승격은_part와_SUCCESS를_최종디렉터리에_공개한다(tmp_path):
    staged = tmp_path / "year_month=2026-07/.staging/source_collected_at=x"
    final = tmp_path / "year_month=2026-07/source_collected_at=x"
    staged.mkdir(parents=True)
    (staged / "part-00000.parquet").write_bytes(b"data")

    monthly_bronze.commit_staged_silver(staged, final, layout="spark_parts")

    assert (final / "part-00000.parquet").read_bytes() == b"data"
    assert (final / "_SUCCESS").is_file()
    assert not staged.exists()


def test_Lambda_승격은_data_parquet_하나만_허용한다(tmp_path):
    staged = tmp_path / "year_month=2026-07/.staging/source_collected_at=x"
    final = tmp_path / "year_month=2026-07/source_collected_at=x"
    staged.mkdir(parents=True)
    (staged / "data.parquet").write_bytes(b"data")

    monthly_bronze.commit_staged_silver(staged, final, layout="single_data")

    assert (final / "data.parquet").read_bytes() == b"data"
    assert (final / "_SUCCESS").is_file()


@pytest.mark.parametrize(
    ("file_name", "layout"),
    [
        ("data.parquet", "spark_parts"),
        ("part-00000.parquet", "single_data"),
    ],
)
def test_생산자와_다른_Silver파일형식은_공개하지_않는다(
    tmp_path, file_name, layout
):
    staged = tmp_path / "year_month=2026-07/.staging/source_collected_at=x"
    final = tmp_path / "year_month=2026-07/source_collected_at=x"
    staged.mkdir(parents=True)
    (staged / file_name).touch()

    with pytest.raises(ValueError, match="계약과 다릅니다"):
        monthly_bronze.commit_staged_silver(staged, final, layout=layout)

    assert not final.exists()


def test_S3_승격은_part복사후_SUCCESS를_마지막에_생성한다(monkeypatch):
    calls = []

    class FakeClient:
        def copy(self, source, bucket, key):
            calls.append(("copy", source, bucket, key))

        def delete_object(self, Bucket, Key):
            calls.append(("delete", Bucket, Key))

        def put_object(self, Bucket, Key, Body):
            calls.append(("put", Bucket, Key, Body))

    staged = S3Location(
        "lake", "silver/x/year_month=2026-07/.staging/source_collected_at=x"
    )
    final = S3Location(
        "lake", "silver/x/year_month=2026-07/source_collected_at=x"
    )
    monkeypatch.setattr(monthly_bronze.boto3, "client", lambda name: FakeClient())
    monkeypatch.setattr(
        monthly_bronze,
        "list_keys",
        lambda bucket, prefix: (
            [f"{staged.key}/part-00000.parquet"]
            if "/.staging/" in prefix
            else [f"{final.key}/old.parquet", f"{final.key}/_SUCCESS"]
        ),
    )

    monthly_bronze.commit_staged_silver(staged, final, layout="spark_parts")

    copy_index = next(i for i, call in enumerate(calls) if call[0] == "copy")
    success_index = calls.index(("put", "lake", f"{final.key}/_SUCCESS", b""))
    assert calls.index(("delete", "lake", f"{final.key}/_SUCCESS")) < copy_index
    assert copy_index < success_index
    assert calls[-1] == (
        "delete",
        "lake",
        f"{staged.key}/part-00000.parquet",
    )


def test_staged와_final의_위치종류가_다르면_승격을_거부한다(tmp_path):
    local = tmp_path / "source_collected_at=x"
    s3 = S3Location("bucket", "source_collected_at=x")

    with pytest.raises(TypeError):
        monthly_bronze.commit_staged_silver(local, s3, layout="spark_parts")

    with pytest.raises(TypeError):
        monthly_bronze.commit_staged_silver(s3, local, layout="spark_parts")
