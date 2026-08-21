"""Monthly Taxi Trip bronze_to_silver `job.py`의 year_month range 파라미터 검증. 이슈 #297.

1. `year_month_range` 가 양끝을 포함해 순서대로 반환, 연도 경계도 처리
2. `year_month_range` 는 start가 end보다 늦으면 ValueError
3. `latest_partition_file` 은 파티션/파일이 없으면 None, 있으면 최신 수집 파일 경로
4. [필수] range로 여러 달 파티션을 한 번에 읽어 Silver로 적재
5. [필수] range 안에 파티션이 하나라도 없으면 FileNotFoundError (부분 처리 안 함)
6. [필수] 없는 파티션이 여러 개면 첫 번째에서 멈추지 않고 전부 모아서 보고
7. 한 달만 처리할 땐 start_year_month == end_year_month로 호출
8. start/end 중 하나만 주면 ValueError
9. `is_s3_path`/`resolve_path`/`latest_partition_file`은 s3://·s3a:// 경로도 처리 (이슈 #646)
10. `--env local|prod`로 기본 입출력 경로를 고름, `prod`인데 버킷이 없으면 ValueError
"""

from datetime import datetime
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

from shared.spark.common.session import get_or_create_spark_session
from main.spark.jobs.bronze_to_silver.monthly_taxi_trip_bronze_to_silver import job

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


@pytest.fixture(scope="module")
def spark():
    session = get_or_create_spark_session("test_monthly_taxi_trip_job")
    yield session
    session.stop()


def _row(**overrides) -> dict:
    row = {
        "taxi_id": "taxi-1",
        "hvfhs_license_num": "HV0003",
        "on_scene_datetime": datetime(2024, 3, 1, 9, 55, 0),
        "pickup_datetime": datetime(2024, 3, 1, 10, 0, 0),
        "dropoff_datetime": datetime(2024, 3, 1, 10, 20, 0),
        "PULocationID": 1,
        "DOLocationID": 2,
        "pickup_zone": "Central Park",
        "dropoff_zone": "JFK Airport",
        "trip_miles": 5.0,
        "trip_time": 600,
        "driver_pay": 20.0,
        "tips": 2.0,
        "estimated_service_tier": "Comfort",
    }
    row.update(overrides)
    return row


def _write_partition(spark, bronze_dir: Path, year_month: str, rows: list[dict]) -> None:
    partition_dir = bronze_dir / f"year_month={year_month}"
    spark.createDataFrame(rows).write.mode("overwrite").parquet(str(partition_dir))


def test_year_month_range은_양끝을_포함해_순서대로_반환한다():
    assert job.year_month_range("2024-01", "2024-01") == ["2024-01"]
    assert job.year_month_range("2024-01", "2024-03") == ["2024-01", "2024-02", "2024-03"]


def test_year_month_range은_연도_경계를_넘어간다():
    assert job.year_month_range("2023-11", "2024-02") == ["2023-11", "2023-12", "2024-01", "2024-02"]


def test_year_month_range은_start가_end보다_늦으면_ValueError():
    with pytest.raises(ValueError):
        job.year_month_range("2024-03", "2024-01")


def test_latest_partition_file은_파티션_디렉터리가_없으면_None(tmp_path):
    assert job.latest_partition_file(str(tmp_path), "2024-01") is None


def test_latest_partition_file은_파일이_없는_빈_파티션이면_None(tmp_path):
    (tmp_path / "year_month=2024-01").mkdir()
    assert job.latest_partition_file(str(tmp_path), "2024-01") is None


def test_latest_partition_file은_최신_수집시각_파일을_반환한다(tmp_path):
    partition = tmp_path / "year_month=2024-01"
    partition.mkdir()
    (partition / "data.parquet").touch()
    older = partition / "20240820T101530123456Z.parquet"
    latest = partition / "20240820T112205654321Z.parquet"
    older.touch()
    latest.touch()

    result = job.latest_partition_file(str(tmp_path), "2024-01")

    assert result == str(latest)


def test_latest_partition_files는_각_월의_최신파일만_고른다(tmp_path):
    january = tmp_path / "year_month=2024-01"
    february = tmp_path / "year_month=2024-02"
    january.mkdir()
    february.mkdir()
    (january / "20240820T101530123456Z.parquet").touch()
    january_latest = january / "20240820T112205654321Z.parquet"
    february_latest = february / "20240821T112205654321Z.parquet"
    january_latest.touch()
    february_latest.touch()

    assert job.latest_partition_files(str(tmp_path)) == [
        str(january_latest),
        str(february_latest),
    ]


def test_default_paths는_local이면_로컬_경로를_돌려준다():
    assert job.default_paths("local", None) == (job.DEFAULT_LOCAL_INPUT, job.DEFAULT_LOCAL_OUTPUT)


def test_default_paths는_prod면_S3_경로를_돌려준다():
    assert job.default_paths("prod", "de-theone") == (
        "s3://de-theone/bronze/monthly_taxi_trip",
        "s3://de-theone/silver/monthly_taxi_trip",
    )


def test_default_paths는_prod인데_버킷이_없으면_ValueError():
    with pytest.raises(ValueError, match="--bucket"):
        job.default_paths("prod", None)


def test_input_output_path를_둘다_명시하면_env_bucket_검증을_안한다(spark, tmp_path):
    """DAG는 항상 --input_path/--output_path를 명시적으로 넘기므로, --env prod에 --bucket이
    없어도 이 경로로는 절대 실패하면 안 됩니다."""
    bronze_dir = tmp_path / "bronze"
    _write_partition(spark, bronze_dir, "2024-01", [_row()])

    result = job.main([
        "--env", "prod",
        "--input_path", str(bronze_dir),
        "--output_path", str(tmp_path / "silver"),
        "--error_threshold", "1.0",
    ])

    assert result is not None


@pytest.mark.parametrize("scheme", ["s3", "s3a"])
def test_is_s3_path는_s3_s3a_스킴만_참이다(scheme):
    assert job.is_s3_path(f"{scheme}://bucket/prefix")
    assert not job.is_s3_path("data/bronze/monthly_taxi_trip")
    assert not job.is_s3_path("/abs/local/path")


def test_resolve_path는_S3_경로를_그대로_둔다():
    assert job.resolve_path("s3://de-theone/bronze/monthly_taxi_trip") == "s3://de-theone/bronze/monthly_taxi_trip"


def test_latest_partition_file은_S3에_파티션이_없으면_None(s3_client):
    assert job.latest_partition_file(f"s3://{S3_BUCKET}/bronze/monthly_taxi_trip", "2024-01") is None


def test_latest_partition_file은_S3에서_최신_파일_경로를_반환한다(s3_client):
    prefix = "bronze/monthly_taxi_trip/year_month=2024-01"
    s3_client.put_object(Bucket=S3_BUCKET, Key=f"{prefix}/part-0.parquet", Body=b"x")
    s3_client.put_object(Bucket=S3_BUCKET, Key=f"{prefix}/part-1.parquet", Body=b"x")
    s3_client.put_object(Bucket=S3_BUCKET, Key=f"{prefix}/_SUCCESS", Body=b"")

    result = job.latest_partition_file(f"s3://{S3_BUCKET}/bronze/monthly_taxi_trip", "2024-01")

    assert result == f"s3://{S3_BUCKET}/{prefix}/part-1.parquet"


def test_range로_여러_달_파티션을_한번에_읽어_silver로_적재한다(spark, tmp_path):
    bronze_dir = tmp_path / "bronze"
    silver_dir = tmp_path / "silver"
    _write_partition(spark, bronze_dir, "2024-01", [
        _row(trip_miles=1.0, pickup_datetime=datetime(2024, 1, 15, 10, 0, 0), dropoff_datetime=datetime(2024, 1, 15, 10, 20, 0)),
    ])
    _write_partition(spark, bronze_dir, "2024-02", [
        _row(trip_miles=2.0, pickup_datetime=datetime(2024, 2, 15, 10, 0, 0), dropoff_datetime=datetime(2024, 2, 15, 10, 20, 0)),
    ])

    job.main([
        "--input_path", str(bronze_dir),
        "--output_path", str(silver_dir),
        "--error_threshold", "1.0",
        "--start_year_month", "2024-01",
        "--end_year_month", "2024-02",
    ])

    result = spark.read.parquet(str(silver_dir))
    assert {row["year_month"] for row in result.collect()} == {"2024-01", "2024-02"}


def test_range_중간에_없는_달이_있으면_FileNotFoundError(spark, tmp_path):
    bronze_dir = tmp_path / "bronze"
    _write_partition(spark, bronze_dir, "2024-01", [
        _row(trip_miles=1.0, pickup_datetime=datetime(2024, 1, 15, 10, 0, 0), dropoff_datetime=datetime(2024, 1, 15, 10, 20, 0)),
    ])
    # 2024-02 파티션은 일부러 만들지 않음
    _write_partition(spark, bronze_dir, "2024-03", [
        _row(trip_miles=3.0, pickup_datetime=datetime(2024, 3, 15, 10, 0, 0), dropoff_datetime=datetime(2024, 3, 15, 10, 20, 0)),
    ])

    with pytest.raises(FileNotFoundError):
        job.main([
            "--input_path", str(bronze_dir),
            "--output_path", str(tmp_path / "silver"),
            "--error_threshold", "1.0",
            "--start_year_month", "2024-01",
            "--end_year_month", "2024-03",
        ])


def test_없는_달이_여러_개면_전부_모아서_보고한다(spark, tmp_path):
    bronze_dir = tmp_path / "bronze"
    _write_partition(spark, bronze_dir, "2024-01", [
        _row(trip_miles=1.0, pickup_datetime=datetime(2024, 1, 15, 10, 0, 0), dropoff_datetime=datetime(2024, 1, 15, 10, 20, 0)),
    ])
    # 2024-02, 2024-03 둘 다 일부러 만들지 않음
    _write_partition(spark, bronze_dir, "2024-04", [
        _row(trip_miles=4.0, pickup_datetime=datetime(2024, 4, 15, 10, 0, 0), dropoff_datetime=datetime(2024, 4, 15, 10, 20, 0)),
    ])

    with pytest.raises(FileNotFoundError) as exc_info:
        job.main([
            "--input_path", str(bronze_dir),
            "--output_path", str(tmp_path / "silver"),
            "--error_threshold", "1.0",
            "--start_year_month", "2024-01",
            "--end_year_month", "2024-04",
        ])

    assert "2024-02" in str(exc_info.value)
    assert "2024-03" in str(exc_info.value)


def test_range_안에_파티션이_하나도_없으면_FileNotFoundError(tmp_path):
    bronze_dir = tmp_path / "bronze"
    bronze_dir.mkdir()

    with pytest.raises(FileNotFoundError):
        job.main([
            "--input_path", str(bronze_dir),
            "--output_path", str(tmp_path / "silver"),
            "--start_year_month", "2024-01",
            "--end_year_month", "2024-02",
        ])


def test_start와_end가_같으면_한_달만_처리한다(spark, tmp_path):
    bronze_dir = tmp_path / "bronze"
    silver_dir = tmp_path / "silver"
    _write_partition(spark, bronze_dir, "2024-05", [
        _row(trip_miles=5.0, pickup_datetime=datetime(2024, 5, 15, 10, 0, 0), dropoff_datetime=datetime(2024, 5, 15, 10, 20, 0)),
    ])

    job.main([
        "--input_path", str(bronze_dir),
        "--output_path", str(silver_dir),
        "--error_threshold", "1.0",
        "--start_year_month", "2024-05",
        "--end_year_month", "2024-05",
    ])

    result = spark.read.parquet(str(silver_dir))
    assert {row["year_month"] for row in result.collect()} == {"2024-05"}


def test_존재하지_않는_단일_월은_FileNotFoundError(tmp_path):
    bronze_dir = tmp_path / "bronze"
    bronze_dir.mkdir()

    with pytest.raises(FileNotFoundError):
        job.main([
            "--input_path", str(bronze_dir),
            "--output_path", str(tmp_path / "silver"),
            "--start_year_month", "2024-01",
            "--end_year_month", "2024-01",
        ])


def test_start만_주고_end를_안_주면_ValueError(tmp_path):
    with pytest.raises(ValueError):
        job.main([
            "--input_path", str(tmp_path / "bronze"),
            "--output_path", str(tmp_path / "silver"),
            "--start_year_month", "2024-01",
        ])


def test_end만_주고_start를_안_주면_ValueError(tmp_path):
    with pytest.raises(ValueError):
        job.main([
            "--input_path", str(tmp_path / "bronze"),
            "--output_path", str(tmp_path / "silver"),
            "--end_year_month", "2024-01",
        ])
