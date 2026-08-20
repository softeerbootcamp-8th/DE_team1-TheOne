"""SyntheticDatasetLoader/S3SyntheticDatasetLoader의 storage 분기 계약."""

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from moto import mock_aws

from main.aws_lambda.common.monthly_dataset import (
    S3SyntheticDatasetLoader,
    SyntheticDatasetLoader,
    build_bronze_loader,
)

DATASET = "monthly_taxi_trip"
YEAR_MONTH = "2026-08"
S3_BUCKET = "test-de-theone"
S3_REGION = "ap-northeast-2"


def _parquet_bytes() -> bytes:
    sink = pa.BufferOutputStream()
    pq.write_table(pa.Table.from_pylist([{"x": 1}]), sink)
    return sink.getvalue().to_pybytes()


def _payload(content: bytes) -> dict:
    return {"year_month": YEAR_MONTH, "dataset": DATASET, "content": content}


def test_build_bronze_loader는_local이면_SyntheticDatasetLoader를_돌려준다(tmp_path):
    loader = build_bronze_loader("local", str(tmp_path), DATASET, DATASET)

    assert isinstance(loader, SyntheticDatasetLoader)


def test_build_bronze_loader는_알수없는_storage면_실패한다(tmp_path):
    with pytest.raises(ValueError, match="알 수 없는 storage"):
        build_bronze_loader("nope", str(tmp_path), DATASET, DATASET)


@pytest.fixture
def s3_client():
    with mock_aws():
        client = boto3.client("s3", region_name=S3_REGION)
        client.create_bucket(
            Bucket=S3_BUCKET,
            CreateBucketConfiguration={"LocationConstraint": S3_REGION},
        )
        yield client


def test_S3SyntheticDatasetLoader는_고정_키에_적재한다(s3_client):
    content = _parquet_bytes()
    loader = S3SyntheticDatasetLoader(DATASET, DATASET, bucket=S3_BUCKET)

    result = loader.write(_payload(content))

    assert result.location == f"s3://{S3_BUCKET}/bronze/{DATASET}/year_month={YEAR_MONTH}/data.parquet"
    assert result.row_count == 1
    body = s3_client.get_object(
        Bucket=S3_BUCKET, Key=f"bronze/{DATASET}/year_month={YEAR_MONTH}/data.parquet"
    )["Body"].read()
    assert body == content


def test_S3SyntheticDatasetLoader는_dataset이_다르면_실패한다(s3_client):
    loader = S3SyntheticDatasetLoader(DATASET, DATASET, bucket=S3_BUCKET)

    with pytest.raises(ValueError, match="수집 dataset이 다릅니다"):
        loader.write(_payload(_parquet_bytes()) | {"dataset": "other"})


def test_S3SyntheticDatasetLoader는_빈_파일이면_실패한다(s3_client):
    loader = S3SyntheticDatasetLoader(DATASET, DATASET, bucket=S3_BUCKET)

    with pytest.raises(ValueError, match="비어 있습니다"):
        loader.write(_payload(b""))


def test_S3SyntheticDatasetLoader는_깨진_parquet이면_실패한다(s3_client):
    loader = S3SyntheticDatasetLoader(DATASET, DATASET, bucket=S3_BUCKET)

    with pytest.raises(ValueError, match="읽을 수 있는 Parquet이 아닙니다"):
        loader.write(_payload(b"not parquet"))
