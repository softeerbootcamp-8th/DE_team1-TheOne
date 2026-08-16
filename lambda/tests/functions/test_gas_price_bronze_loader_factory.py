"""gas_price_raw_to_bronze의 build_bronze_loader 팩토리 시나리오 (moto로 S3 mock).

1. storage="local" → GasPriceBronzeLoader로 로컬 파티션에 정상 적재
2. storage="s3" → GasPriceS3BronzeLoader로 S3 bucket/key에 정상 적재
3. storage에 local/s3 외 값 → ValueError로 명확히 실패
"""

import json
from datetime import datetime, timezone
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

from functions.common import gas_price_layout as layout
from functions.gas_price_raw_to_bronze.extractor import PAGE_URL
from functions.gas_price_raw_to_bronze.loader import (
    GasPriceBronzeLoader,
    GasPriceS3BronzeLoader,
    build_bronze_loader,
)

ROW = {
    "state": "NY",
    "fuel_type": "regular",
    "price_raw": "$3.210",
    "price_date_raw": "8/8/26",
    "source_url": PAGE_URL,
}
COLLECTED_AT = datetime(2026, 8, 9, 12, 0, tzinfo=timezone.utc)
BUCKET = "test-bucket"
REGION = "us-east-1"


@pytest.fixture(autouse=True)
def _no_dotenv_load(monkeypatch):
    # S3Loader.__init__ 이 저장소 루트의 실제 .env 를 읽으려 하므로, 개발자마다
    # 다른 .env 내용에 테스트 결과가 좌우되지 않게 no-op으로 막는다.
    monkeypatch.setattr("functions.common.s3_loader.load_local_env", lambda: None)


@pytest.fixture
def s3_bucket(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", REGION)
    with mock_aws():
        boto3.client("s3", region_name=REGION).create_bucket(Bucket=BUCKET)
        yield BUCKET


def test_storage가_local이면_GasPriceBronzeLoader로_로컬에_적재한다(tmp_path):
    loader = build_bronze_loader("local", str(tmp_path), COLLECTED_AT)

    assert isinstance(loader, GasPriceBronzeLoader)

    result = loader.write(ROW)
    path = Path(result.location)

    assert path == layout.bronze_file(str(tmp_path), "2026-08-09")
    assert json.loads(path.read_text(encoding="utf-8")) == {
        **ROW,
        "collected_at": COLLECTED_AT.isoformat(),
    }
    assert result.row_count == 1


def test_storage가_s3이면_GasPriceS3BronzeLoader로_S3에_적재한다(s3_bucket):
    loader = build_bronze_loader("s3", "unused", COLLECTED_AT, bucket=s3_bucket)

    assert isinstance(loader, GasPriceS3BronzeLoader)

    result = loader.write(ROW)
    expected_key = layout.bronze_key("2026-08-09")

    assert result.location == f"s3://{s3_bucket}/{expected_key}"
    assert result.row_count == 1

    stored = boto3.client("s3", region_name=REGION).get_object(
        Bucket=s3_bucket, Key=expected_key
    )
    assert json.loads(stored["Body"].read()) == {
        **ROW,
        "collected_at": COLLECTED_AT.isoformat(),
    }


def test_알_수_없는_storage는_ValueError로_실패한다(tmp_path):
    with pytest.raises(ValueError, match="알 수 없는 storage"):
        build_bronze_loader("gcs", str(tmp_path), COLLECTED_AT)
