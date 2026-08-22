"""Lambda dry-run 무적재 계약.

1. 공통 S3 loader는 클라이언트 생성과 PutObject를 모두 생략
2. 기존 S3 Bronze와 같은 원천은 재적재 없이 기존 객체를 반환
3. 변경 원천은 과거 Bronze로 거짓 성공하지 않고 실패
4. EIA 통합 Silver는 적재 없이 월 품질·계보 계약을 검증
"""

from datetime import date
import io

import boto3
from moto import mock_aws
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from main.aws_lambda.common.monthly_dataset import S3MonthlyParquetBronzeLoader
from main.aws_lambda.functions.eia_fuel_price_silver.loader import (
    EiaFuelPriceSilverLoader,
)
from shared.aws_lambda.common.s3_loader import S3Loader, S3Object


BUCKET = "test-dry-run"
REGION = "ap-northeast-2"


def _parquet_bytes(value: str) -> bytes:
    buffer = io.BytesIO()
    pq.write_table(pa.table({"value": [value]}), buffer)
    return buffer.getvalue()


def _fuel_rows() -> list[dict]:
    return [
        {
            "date": date(2025, 5, day),
            "gas_price": 3.4,
            "ev_price": 0.4,
            "price_source": "eia",
            "bronze_collected_date": date(2026, 8, 10),
            "ev_price_status": "Final",
        }
        for day in range(1, 32)
    ]


def test_공통_S3_loader는_dry_run에서_client와_put을_만들지_않는다(monkeypatch):
    monkeypatch.setattr(
        "shared.aws_lambda.common.s3_loader.boto3.client",
        lambda *args, **kwargs: pytest.fail("dry-run에서 S3 client 생성"),
    )

    result = S3Loader("silver/data.parquet", BUCKET, dry_run=True).write(
        S3Object(body=b"parquet", row_count=7)
    )

    assert result.location == f"s3://{BUCKET}/silver/data.parquet"
    assert result.row_count == 7


@mock_aws
def test_같은_S3_Bronze는_dry_run에서_새_object를_만들지_않는다():
    client = boto3.client("s3", region_name=REGION)
    client.create_bucket(
        Bucket=BUCKET,
        CreateBucketConfiguration={"LocationConstraint": REGION},
    )
    content = _parquet_bytes("same")
    existing_key = (
        "bronze/monthly_taxi_trip/year_month=2026-08/"
        "20260821T010203123456Z.parquet"
    )
    client.put_object(Bucket=BUCKET, Key=existing_key, Body=content)
    loader = S3MonthlyParquetBronzeLoader(
        "monthly_taxi_trip",
        "monthly_taxi_trip",
        BUCKET,
        dry_run=True,
    )

    result = loader.write(
        {
            "dataset": "monthly_taxi_trip",
            "year_month": "2026-08",
            "collected_at": "2026-08-22T01:02:03.123456Z",
            "content": content,
        }
    )
    keys = [item["Key"] for item in client.list_objects_v2(Bucket=BUCKET)["Contents"]]

    assert result.location == f"s3://{BUCKET}/{existing_key}"
    assert loader.source_changed is False
    assert keys == [existing_key]


@mock_aws
def test_변경된_S3_원천은_dry_run에서_과거_Bronze로_통과하지_않는다():
    client = boto3.client("s3", region_name=REGION)
    client.create_bucket(
        Bucket=BUCKET,
        CreateBucketConfiguration={"LocationConstraint": REGION},
    )
    existing_key = (
        "bronze/monthly_taxi_trip/year_month=2026-08/"
        "20260821T010203123456Z.parquet"
    )
    client.put_object(Bucket=BUCKET, Key=existing_key, Body=_parquet_bytes("old"))
    loader = S3MonthlyParquetBronzeLoader(
        "monthly_taxi_trip",
        "monthly_taxi_trip",
        BUCKET,
        dry_run=True,
    )

    with pytest.raises(ValueError, match="기존 Bronze와 다릅니다"):
        loader.write(
            {
                "dataset": "monthly_taxi_trip",
                "year_month": "2026-08",
                "collected_at": "2026-08-22T01:02:03.123456Z",
                "content": _parquet_bytes("new"),
            }
        )

    keys = [item["Key"] for item in client.list_objects_v2(Bucket=BUCKET)["Contents"]]
    assert keys == [existing_key]


def test_EIA_Silver_dry_run은_파일없이_월_품질을_검증한다(tmp_path):
    rows = _fuel_rows()
    loader = EiaFuelPriceSilverLoader(str(tmp_path), "2025-05", dry_run=True)

    result = loader.write(rows)

    assert result.row_count == 31
    assert list(tmp_path.iterdir()) == []

    rows[-1]["ev_price_status"] = "Preliminary"
    with pytest.raises(ValueError, match="ev_price_status"):
        loader.write(rows)
    assert list(tmp_path.iterdir()) == []
