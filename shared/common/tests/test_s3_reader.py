"""S3 Bronze/Silver 공통 읽기 헬퍼(s3_reader) 계약 검증."""

import boto3
import pytest
from moto import mock_aws

from shared.common.s3_reader import (
    get_object_bytes,
    is_s3_uri,
    list_keys,
    parent_uri,
    parse_s3_uri,
)

BUCKET = "test-bucket"
REGION = "ap-northeast-2"


def _make_bucket(client):
    client.create_bucket(
        Bucket=BUCKET,
        CreateBucketConfiguration={"LocationConstraint": REGION},
    )


@mock_aws
def test_list_keys가_prefix_아래_모든_키를_페이지네이션하며_나열한다():
    client = boto3.client("s3", region_name=REGION)
    _make_bucket(client)
    for i in range(3):
        client.put_object(Bucket=BUCKET, Key=f"bronze/uber/part-{i}.parquet", Body=b"x")
    client.put_object(Bucket=BUCKET, Key="bronze/lyft/part-0.parquet", Body=b"x")

    keys = list_keys(BUCKET, "bronze/uber/")

    assert sorted(keys) == [
        "bronze/uber/part-0.parquet",
        "bronze/uber/part-1.parquet",
        "bronze/uber/part-2.parquet",
    ]


@mock_aws
def test_list_keys는_일치하는_객체가_없으면_빈_리스트를_반환한다():
    client = boto3.client("s3", region_name=REGION)
    _make_bucket(client)

    assert list_keys(BUCKET, "bronze/does-not-exist/") == []


@mock_aws
def test_get_object_bytes가_객체_본문을_그대로_반환한다():
    client = boto3.client("s3", region_name=REGION)
    _make_bucket(client)
    body = b"hello bronze"
    client.put_object(Bucket=BUCKET, Key="bronze/uber/part-0.parquet", Body=body)

    assert get_object_bytes(BUCKET, "bronze/uber/part-0.parquet") == body


# --- URI 헬퍼 (#767) ----------------------------------------------------------
#
# pandas 를 쓰는 read_parquet_uri 는 이 런타임에 pandas 가 없어 sub/spark 테스트에
# 있습니다. 여기서는 스킴 파싱만 검증합니다.

@pytest.mark.parametrize(
    "uri, expected",
    [
        ("s3://bucket/key.parquet", True),
        ("s3a://bucket/key.parquet", True),
        ("/opt/airflow/data/key.parquet", False),
        ("data/key.parquet", False),
    ],
)
def test_is_s3_uri는_s3와_s3a만_S3로_본다(uri, expected):
    assert is_s3_uri(uri) is expected


def test_parse_s3_uri는_버킷과_키를_나눈다():
    assert parse_s3_uri("s3://de-theone/source/raw/a.parquet") == (
        "de-theone",
        "source/raw/a.parquet",
    )
    assert parse_s3_uri("s3a://de-theone/source/raw/a.parquet") == (
        "de-theone",
        "source/raw/a.parquet",
    )


@pytest.mark.parametrize("uri", ["/local/path.parquet", "s3://", "s3://bucket", "s3://bucket/"])
def test_parse_s3_uri는_스킴이나_키가_없으면_실패한다(uri):
    with pytest.raises(ValueError):
        parse_s3_uri(uri)


def test_parent_uri는_S3_스킴의_이중_슬래시를_보존한다():
    """`pathlib.Path` 로 올라가면 `s3://b/x` 가 `s3:/b/x` 로 뭉개집니다.

    그 상태로 EMR 에 넘기면 스킴을 못 알아봐 job 이 죽습니다.
    """
    uri = "s3://de-theone/bronze/hvfhv/year_month=2026-08/part-0.parquet"

    assert parent_uri(uri) == "s3://de-theone/bronze/hvfhv/year_month=2026-08"
    assert parent_uri(uri, 2) == "s3://de-theone/bronze/hvfhv"


def test_parent_uri는_로컬_경로도_그대로_올라간다():
    assert parent_uri("/data/bronze/hvfhv/year_month=2026-08/part-0.parquet", 2) == (
        "/data/bronze/hvfhv"
    )


def test_parent_uri는_버킷보다_위로는_못_올라간다():
    with pytest.raises(ValueError):
        parent_uri("s3://de-theone/bronze", 2)
