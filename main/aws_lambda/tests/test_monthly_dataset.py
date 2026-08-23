"""월별 Parquet Bronze Loader의 로컬·S3 수집 이력 계약."""

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from moto import mock_aws

from main.aws_lambda.common.monthly_dataset import (
    MonthlyParquetBronzeLoader,
    S3MonthlyParquetBronzeLoader,
    build_bronze_loader,
)

DATASET = "monthly_taxi_trip"
YEAR_MONTH = "2026-08"
FIRST_COLLECTED_AT = "2026-08-20T10:15:30.123456Z"
SECOND_COLLECTED_AT = "2026-08-20T11:22:05.654321Z"
FIRST_KEY = "collected_at=20260820T101530123456Z/data.parquet"
SECOND_KEY = "collected_at=20260820T112205654321Z/data.parquet"
S3_BUCKET = "test-de-theone"
S3_REGION = "ap-northeast-2"


def _parquet_bytes(value: int = 1) -> bytes:
    sink = pa.BufferOutputStream()
    pq.write_table(pa.Table.from_pylist([{"x": value}]), sink)
    return sink.getvalue().to_pybytes()


def _payload(content: bytes, collected_at: str = FIRST_COLLECTED_AT) -> dict:
    return {
        "year_month": YEAR_MONTH,
        "dataset": DATASET,
        "collected_at": collected_at,
        "content": content,
    }


def test_build_bronze_loader는_local_loader를_돌려준다(tmp_path):
    loader = build_bronze_loader("local", str(tmp_path), DATASET, DATASET)

    assert isinstance(loader, MonthlyParquetBronzeLoader)


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


def test_S3_loader는_변경된_원본을_수집시각_키로_append한다(s3_client):
    loader = S3MonthlyParquetBronzeLoader(DATASET, DATASET, bucket=S3_BUCKET)
    first = loader.write(_payload(_parquet_bytes()))
    second_content = _parquet_bytes(2)
    second = loader.write(_payload(second_content, SECOND_COLLECTED_AT))

    prefix = f"bronze/{DATASET}/year_month={YEAR_MONTH}/"
    keys = [
        obj["Key"]
        for obj in s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)[
            "Contents"
        ]
    ]
    assert first.location == f"s3://{S3_BUCKET}/{prefix}{FIRST_KEY}"
    assert second.location == f"s3://{S3_BUCKET}/{prefix}{SECOND_KEY}"
    assert loader.source_changed is True
    assert keys == [f"{prefix}{FIRST_KEY}", f"{prefix}{SECOND_KEY}"]
    assert (
        s3_client.get_object(Bucket=S3_BUCKET, Key=keys[-1])["Body"].read()
        == second_content
    )


def test_S3_loader는_동일한_최신원본을_재사용한다(s3_client):
    content = _parquet_bytes()
    loader = S3MonthlyParquetBronzeLoader(DATASET, DATASET, bucket=S3_BUCKET)

    first = loader.write(_payload(content))
    second = loader.write(_payload(content, SECOND_COLLECTED_AT))

    assert second.location == first.location
    assert loader.source_changed is False
    assert loader.payload["collected_at"] == FIRST_COLLECTED_AT
    response = s3_client.list_objects_v2(
        Bucket=S3_BUCKET,
        Prefix=f"bronze/{DATASET}/year_month={YEAR_MONTH}/",
    )
    assert response["KeyCount"] == 1


def test_S3_loader는_기존_flat파일도_동일원본이면_재사용한다(s3_client):
    content = _parquet_bytes()
    prefix = f"bronze/{DATASET}/year_month={YEAR_MONTH}/"
    legacy_key = f"{prefix}20260820T101530123456Z.parquet"
    s3_client.put_object(Bucket=S3_BUCKET, Key=legacy_key, Body=content)
    loader = S3MonthlyParquetBronzeLoader(DATASET, DATASET, bucket=S3_BUCKET)

    result = loader.write(_payload(content, SECOND_COLLECTED_AT))

    assert result.location == f"s3://{S3_BUCKET}/{legacy_key}"
    assert loader.payload["collected_at"] == FIRST_COLLECTED_AT
    assert loader.source_changed is False
    assert s3_client.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)["KeyCount"] == 1


def test_local_loader는_collected_at_디렉터리에_data파일을_쓴다(tmp_path):
    loader = MonthlyParquetBronzeLoader(tmp_path, DATASET, DATASET)

    result = loader.write(_payload(_parquet_bytes()))

    path = tmp_path / DATASET / f"year_month={YEAR_MONTH}" / FIRST_KEY
    assert result.location == str(path)
    assert path.is_file()


def test_local_loader는_기존_flat파일도_동일원본이면_재사용한다(tmp_path):
    content = _parquet_bytes()
    partition = tmp_path / DATASET / f"year_month={YEAR_MONTH}"
    partition.mkdir(parents=True)
    legacy = partition / "20260820T101530123456Z.parquet"
    legacy.write_bytes(content)
    loader = MonthlyParquetBronzeLoader(tmp_path, DATASET, DATASET)

    result = loader.write(_payload(content, SECOND_COLLECTED_AT))

    assert result.location == str(legacy)
    assert loader.payload["collected_at"] == FIRST_COLLECTED_AT
    assert loader.source_changed is False
    assert len(list(partition.rglob("*.parquet"))) == 1


def test_S3_loader는_dataset이_다르면_실패한다(s3_client):
    loader = S3MonthlyParquetBronzeLoader(DATASET, DATASET, bucket=S3_BUCKET)

    with pytest.raises(ValueError, match="수집 dataset이 다릅니다"):
        loader.write(_payload(_parquet_bytes()) | {"dataset": "other"})


@pytest.mark.parametrize("content", [b"", b"not parquet"])
def test_S3_loader는_읽을수없는_원본이면_실패한다(s3_client, content):
    loader = S3MonthlyParquetBronzeLoader(DATASET, DATASET, bucket=S3_BUCKET)

    with pytest.raises(ValueError, match="비어 있습니다|Parquet이 아닙니다"):
        loader.write(_payload(content))
