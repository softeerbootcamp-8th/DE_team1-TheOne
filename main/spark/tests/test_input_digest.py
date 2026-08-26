"""Silver 입력 내용 digest 시나리오 (#1088).

1. 같은 디렉터리를 다시 해시하면 같은 값 — 재실행 멱등성 유지
2. 파일 내용이 바뀌면 digest 도 바뀜 — 같은 경로 재발행 감지
3. 파일 추가·삭제도 digest 를 바꿈
4. S3 경로도 같은 규칙으로 해시 (moto)
5. 비어 있거나 없는 입력은 FileNotFoundError
"""

import pytest
from moto import mock_aws

from main.spark.jobs.silver_to_gold.input_digest import silver_input_digest


import boto3


def _write(path, content: bytes):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)


def test_같은_디렉터리는_다시_해시해도_같은_digest다(tmp_path):
    _write(tmp_path / "data.parquet", b"same-bytes")

    first = silver_input_digest(str(tmp_path))
    second = silver_input_digest(str(tmp_path))

    assert first == second
    assert len(first) == 64


def test_파일_내용이_바뀌면_digest도_바뀐다(tmp_path):
    target = tmp_path / "source_collected_at=v1"
    _write(target / "data.parquet", b"v1-content")
    original = silver_input_digest(str(target))

    _write(target / "data.parquet", b"v2-content")

    assert silver_input_digest(str(target)) != original


def test_같은_경로라도_버전디렉터리가_다른_내용이면_다른_digest다(tmp_path):
    """fingerprint 가 경로만 보면 놓치는 바로 그 경우(#1088)."""
    v1 = tmp_path / "silver" / "source_collected_at=20260801T000000Z"
    v1_overwritten = tmp_path / "other" / "source_collected_at=20260801T000000Z"
    _write(v1 / "data.parquet", b"first")
    _write(v1_overwritten / "data.parquet", b"republished")

    assert silver_input_digest(str(v1)) != silver_input_digest(str(v1_overwritten))


def test_파일_추가와_삭제도_digest를_바꾼다(tmp_path):
    target = tmp_path / "version"
    _write(target / "a.parquet", b"a")
    base = silver_input_digest(str(target))

    _write(target / "b.parquet", b"b")
    with_added = silver_input_digest(str(target))
    (target / "b.parquet").unlink()
    (target / "c.parquet").write_bytes(b"c")

    assert with_added != base
    assert silver_input_digest(str(target)) != with_added


def test_단일_파일_경로도_해시한다(tmp_path):
    target = tmp_path / "fuel.parquet"
    target.write_bytes(b"single")

    assert silver_input_digest(str(target)) == silver_input_digest(str(target))


@pytest.mark.parametrize("missing", ["no-such-dir", "empty-dir"])
def test_없거나_빈_입력은_실패한다(tmp_path, missing):
    if missing == "empty-dir":
        (tmp_path / missing).mkdir()

    with pytest.raises(FileNotFoundError):
        silver_input_digest(str(tmp_path / missing))


@mock_aws
def test_S3_경로도_같은_규칙으로_해시한다(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "ap-northeast-2")
    client = boto3.client("s3", region_name="ap-northeast-2")
    client.create_bucket(
        Bucket="lake",
        CreateBucketConfiguration={"LocationConstraint": "ap-northeast-2"},
    )
    client.put_object(Bucket="lake", Key="silver/x/version/data.parquet", Body=b"s3-1")
    first = silver_input_digest("s3://lake/silver/x/version/")
    assert first == silver_input_digest("s3://lake/silver/x/version/")

    # 같은 키에 다른 내용 재업로드 — digest 가 바꿔야 감지할 수 있습니다.
    client.put_object(Bucket="lake", Key="silver/x/version/data.parquet", Body=b"s3-2")

    assert silver_input_digest("s3://lake/silver/x/version/") != first
    assert first != silver_input_digest("s3://lake/silver/x/version/")


@mock_aws
def test_S3_접두사_경계가_보존된다(monkeypatch):
    """version 이 version_old 를 삼키면 안 됩니다."""
    monkeypatch.setenv("AWS_DEFAULT_REGION", "ap-northeast-2")
    client = boto3.client("s3", region_name="ap-northeast-2")
    client.create_bucket(
        Bucket="lake",
        CreateBucketConfiguration={"LocationConstraint": "ap-northeast-2"},
    )
    client.put_object(Bucket="lake", Key="silver/x/version/data.parquet", Body=b"mine")
    client.put_object(
        Bucket="lake", Key="silver/x/version_old/data.parquet", Body=b"other"
    )

    digest = silver_input_digest("s3://lake/silver/x/version/")

    other = silver_input_digest("s3://lake/silver/x/version_old/")
    assert digest != other
