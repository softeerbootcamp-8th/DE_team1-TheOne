"""S3 Bronze/Silver 공통 읽기 헬퍼(s3_reader) 계약 검증."""

import boto3
from moto import mock_aws

from shared.aws_lambda.common.s3_reader import get_object_bytes, list_keys

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
