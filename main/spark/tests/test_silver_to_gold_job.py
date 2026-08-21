"""silver_to_gold `job.py`의 연료비 최신 파티션 선택. 이슈 #696.

1. 로컬 여러 파티션 중 가장 최근(year_month 최댓값) 파일을 고른다
2. 파티션이 하나도 없으면 FileNotFoundError
3. 파티션 디렉터리는 있는데 parquet 파일이 없으면 FileNotFoundError
4. S3 경로에서도 같은 로직으로 최신 파티션을 고른다
"""

import boto3
import pytest
from moto import mock_aws

from main.spark.jobs.silver_to_gold import job

S3_BUCKET = "test-de-theone"
S3_REGION = "ap-northeast-2"


@pytest.fixture
def s3_client():
    with mock_aws():
        client = boto3.client("s3", region_name=S3_REGION)
        client.create_bucket(
            Bucket=S3_BUCKET,
            CreateBucketConfiguration={"LocationConstraint": S3_REGION},
        )
        yield client


def test_로컬에서_가장_최근_파티션_파일을_고른다(tmp_path):
    for year_month in ("2026-01", "2026-04", "2026-05"):
        partition = tmp_path / f"year_month={year_month}"
        partition.mkdir()
        (partition / "gas_ev_price.parquet").touch()

    result = job.latest_fuel_price_path(str(tmp_path))

    assert result == str(tmp_path / "year_month=2026-05" / "gas_ev_price.parquet")


def test_로컬에_파티션이_하나도_없으면_FileNotFoundError(tmp_path):
    with pytest.raises(FileNotFoundError):
        job.latest_fuel_price_path(str(tmp_path))


def test_로컬_파티션_디렉터리는_있는데_파일이_없으면_FileNotFoundError(tmp_path):
    (tmp_path / "year_month=2026-05").mkdir()

    with pytest.raises(FileNotFoundError):
        job.latest_fuel_price_path(str(tmp_path))


def test_S3에서도_가장_최근_파티션_파일을_고른다(s3_client):
    prefix = "silver/gas_ev_price"
    for year_month in ("2026-01", "2026-04", "2026-05"):
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=f"{prefix}/year_month={year_month}/gas_ev_price.parquet",
            Body=b"x",
        )

    result = job.latest_fuel_price_path(f"s3://{S3_BUCKET}/{prefix}")

    assert result == f"s3://{S3_BUCKET}/{prefix}/year_month=2026-05/gas_ev_price.parquet"
