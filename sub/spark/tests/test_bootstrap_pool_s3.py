"""부트스트랩 풀 입력을 S3 에서 읽는 경로 검증 (#767).

EMR Serverless 워커는 Airflow 컨테이너의 로컬 디스크를 볼 수 없습니다. 그래서
`load_bootstrap_pools` 의 bronze_dir 과 `vehicle_master_path` 가 `s3://` 를 받아야
합니다. `pd.read_parquet` 에 `s3://` 를 그대로 넘기지 않는 이유는 s3fs 가 필요하고
그것이 aiobotocore 를 끌고 와 런타임의 boto3 핀과 충돌하기 때문입니다.
"""

import io

import boto3
import pandas as pd
import pytest
from moto import mock_aws

from shared.common.s3_reader import read_parquet_uri
from sub.spark.jobs.driver_master.traits import (
    _latest_partition_file,
    load_bootstrap_pools,
)

BUCKET = "test-bucket"
REGION = "ap-northeast-2"


def _make_bucket(client):
    client.create_bucket(
        Bucket=BUCKET,
        CreateBucketConfiguration={"LocationConstraint": REGION},
    )


def _trip_frame(rows: int) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "trip_miles": [3.0 + i * 0.1 for i in range(rows)],
            "trip_time": [900 + i for i in range(rows)],
            "driver_pay": [20.0 + i for i in range(rows)],
        }
    )


def _put_parquet(client, key: str, frame: pd.DataFrame) -> None:
    buffer = io.BytesIO()
    frame.to_parquet(buffer, index=False)
    client.put_object(Bucket=BUCKET, Key=key, Body=buffer.getvalue())


@mock_aws
def test_read_parquet_uri는_S3_객체를_s3fs_없이_읽는다():
    client = boto3.client("s3", region_name=REGION)
    _make_bucket(client)
    frame = _trip_frame(5)
    _put_parquet(client, "bronze/hvfhv/year_month=2026-08/part-0.parquet", frame)

    loaded = read_parquet_uri(f"s3://{BUCKET}/bronze/hvfhv/year_month=2026-08/part-0.parquet")

    pd.testing.assert_frame_equal(loaded, frame)


def test_read_parquet_uri는_로컬_경로도_그대로_읽는다(tmp_path):
    frame = _trip_frame(3)
    path = tmp_path / "part-0.parquet"
    frame.to_parquet(path, index=False)

    pd.testing.assert_frame_equal(read_parquet_uri(str(path)), frame)


@mock_aws
def test_S3_파티션에서_이름순_마지막_Parquet을_고른다():
    """파일 이름이 수집 시각을 담아 정렬이 곧 시간 순입니다(로컬 glob 과 같은 규칙)."""
    client = boto3.client("s3", region_name=REGION)
    _make_bucket(client)
    prefix = "bronze/hvfhv/year_month=2026-08"
    for name in ("part-20260801.parquet", "part-20260815.parquet", "_SUCCESS"):
        _put_parquet(client, f"{prefix}/{name}", _trip_frame(2))
    _put_parquet(client, "bronze/hvfhv/year_month=2026-07/part-20260701.parquet", _trip_frame(2))

    latest = _latest_partition_file(f"s3://{BUCKET}/bronze/hvfhv", "2026-08")

    assert latest == f"s3://{BUCKET}/{prefix}/part-20260815.parquet"


@mock_aws
def test_없는_월은_None을_돌려_다음_월로_넘어간다():
    client = boto3.client("s3", region_name=REGION)
    _make_bucket(client)
    _put_parquet(client, "bronze/hvfhv/year_month=2026-08/part-0.parquet", _trip_frame(2))

    assert _latest_partition_file(f"s3://{BUCKET}/bronze/hvfhv", "2026-01") is None


@mock_aws
def test_load_bootstrap_pools는_S3_bronze_에서_풀을_만든다():
    client = boto3.client("s3", region_name=REGION)
    _make_bucket(client)
    _put_parquet(client, "bronze/hvfhv/year_month=2026-08/part-0.parquet", _trip_frame(50))

    pools = load_bootstrap_pools(
        bronze_dir=f"s3://{BUCKET}/bronze/hvfhv",
        months=["2026-08"],
        sample_per_month=10,
        seed=42,
    )

    assert len(pools["trip_miles"]) == 10
    assert len(pools["trip_time_min"]) == 10


@mock_aws
def test_S3_에_대상_월이_없으면_경로를_담아_실패한다():
    """조용히 빈 풀을 돌려주면 하류에서 정체 불명의 numpy 에러로 터집니다."""
    client = boto3.client("s3", region_name=REGION)
    _make_bucket(client)

    with pytest.raises(FileNotFoundError, match="bronze/hvfhv"):
        load_bootstrap_pools(
            bronze_dir=f"s3://{BUCKET}/bronze/hvfhv",
            months=["2026-08"],
            sample_per_month=10,
            seed=42,
        )
