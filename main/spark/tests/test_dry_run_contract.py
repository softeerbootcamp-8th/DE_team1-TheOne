"""Spark dry-run 무적재 계약(JVM 불필요).

1. S3 exact/wildcard 입력은 임시 로컬 파일로 staging 후 정리
2. HVFHV Silver dry-run은 정상 적재와 같은 물리 스키마를 검증
3. Gold dry-run은 집계 결과를 계산하되 CSV writer를 호출하지 않음

이슈 #771 (EMR dry-run이 로컬 staging을 잘못 탐):
4. monthly_taxi_trip Silver dry-run은 prod에서는 S3 입력을 로컬로 내리지 않는다
5. monthly_taxi_trip Silver dry-run은 local에서는 여전히 S3 입력을 로컬로 내린다
"""

from pathlib import Path

import boto3
from moto import mock_aws
import pandas as pd
import pytest

from main.spark.jobs.bronze_to_silver.monthly_taxi_trip_bronze_to_silver import (
    job as silver_job,
)
from main.spark.jobs.bronze_to_silver.monthly_taxi_trip_bronze_to_silver.transformer import (
    FINAL_SCHEMA,
)
from main.spark.jobs.silver_to_gold import job as gold_job
from shared.spark.common.io import stage_s3_parquet_inputs


BUCKET = "test-dry-run"
REGION = "ap-northeast-2"


class SchemaFrame:
    def __init__(self, fields):
        self.schema = fields
        self.columns = [field.name for field in fields]

    def drop(self, name):
        return SchemaFrame([field for field in self.schema if field.name != name])

    def count(self):
        return 7


@mock_aws
def test_S3_입력은_임시_staging후_정리한다():
    client = boto3.client("s3", region_name=REGION)
    client.create_bucket(
        Bucket=BUCKET,
        CreateBucketConfiguration={"LocationConstraint": REGION},
    )
    for key in (
        "silver/exact.parquet",
        "silver/parts/part-00000.parquet",
        "silver/parts/part-00001.parquet",
        "silver/parts/_SUCCESS",
        "silver/parts/nested/part-99999.parquet",
    ):
        client.put_object(Bucket=BUCKET, Key=key, Body=key.encode())

    with stage_s3_parquet_inputs(
        f"s3://{BUCKET}/silver/exact.parquet",
        f"s3://{BUCKET}/silver/parts/part-*.parquet",
    ) as (exact, wildcard):
        staged = [Path(path) for path in exact + wildcard]
        assert len(exact) == 1
        assert len(wildcard) == 2
        assert all(path.is_file() for path in staged)

    assert all(not path.exists() for path in staged)


class _FakeSparkContext:
    def setLogLevel(self, level):
        pass


class _FakeSpark:
    def __init__(self):
        self.sparkContext = _FakeSparkContext()


class _FakeFrame:
    def count(self):
        return 5


def _patch_monthly_taxi_trip_pipeline(monkeypatch, staged_calls):
    from contextlib import contextmanager

    @contextmanager
    def fake_stage(*groups):
        staged_calls.append(groups)
        yield tuple([group] if isinstance(group, str) else group for group in groups)

    captured_paths = []

    class FakeExtractor:
        def __init__(self, spark, path):
            captured_paths.append(path)
            self.name = "fake"

        def extract(self):
            return _FakeFrame()

    class FakeTransformer:
        def __init__(self, error_threshold):
            pass

        def transform(self, data):
            return data

    monkeypatch.setattr(silver_job, "get_or_create_spark_session", lambda *a, **k: _FakeSpark())
    monkeypatch.setattr(silver_job, "stage_s3_parquet_inputs", fake_stage)
    monkeypatch.setattr(silver_job, "SparkParquetExtractor", FakeExtractor)
    monkeypatch.setattr(silver_job, "MonthlyTaxiTripCleanTransformer", FakeTransformer)
    return captured_paths


def test_monthly_taxi_trip_dry_run은_prod에서_S3_입력을_로컬로_내리지_않는다(monkeypatch):
    staged_calls = []
    captured_paths = _patch_monthly_taxi_trip_pipeline(monkeypatch, staged_calls)
    input_path = "s3://test-bucket/bronze/monthly_taxi_trip/year_month=2026-05/20260501T000000000000Z.parquet"

    silver_job.main(
        [
            "--env", "prod",
            "--input_path", input_path,
            "--output_path", "s3://test-bucket/silver/monthly_taxi_trip",
            "--dry-run",
        ]
    )

    assert staged_calls == []
    assert captured_paths == [input_path]


def test_monthly_taxi_trip_dry_run은_local에서는_여전히_S3_입력을_로컬로_내린다(monkeypatch):
    staged_calls = []
    _patch_monthly_taxi_trip_pipeline(monkeypatch, staged_calls)
    input_path = "s3://test-bucket/bronze/monthly_taxi_trip/year_month=2026-05/20260501T000000000000Z.parquet"

    silver_job.main(
        [
            "--env", "local",
            "--input_path", input_path,
            "--output_path", "data/silver/monthly_taxi_trip",
            "--dry-run",
        ]
    )

    assert len(staged_calls) == 1


def test_HVFHV_Silver_dry_run은_물리_스키마를_검증하고_쓰지_않는다():
    frame = SchemaFrame(list(FINAL_SCHEMA))

    result = silver_job.DryRunLoader("s3://lake/silver/output.parquet").write(frame)

    assert result.location == "s3://lake/silver/output.parquet"
    assert result.row_count == 7

    invalid = SchemaFrame(list(FINAL_SCHEMA)[1:])
    with pytest.raises(ValueError, match="물리 파일 계약"):
        silver_job.DryRunLoader("s3://lake/silver/output.parquet").write(invalid)


def test_Gold_dry_run은_CSV_writer를_호출하지_않는다(monkeypatch):
    class Frame:
        def persist(self):
            return self

        def unpersist(self):
            return None

        def toPandas(self):
            return pd.DataFrame({"value": [1]})

    class Reader:
        def parquet(self, *paths):
            return Frame()

    spark = type("Spark", (), {"read": Reader()})()
    monkeypatch.setattr(gold_job, "get_or_create_spark_session", lambda *args: spark)
    monkeypatch.setattr(gold_job, "latest_fuel_price_path", lambda path: path)
    monkeypatch.setattr(gold_job, "enrich_trips_with_fuel_cost", lambda *args: Frame())
    monkeypatch.setattr(gold_job, "build_driver_monthly_aggregation", lambda *args: Frame())
    monkeypatch.setattr(gold_job, "build_driver_monthly_profit", lambda *args: Frame())
    monkeypatch.setattr(
        gold_job,
        "build_monthly_vehicle_recommendation",
        lambda *args: Frame(),
    )
    monkeypatch.setattr(gold_job, "validate_gold_business_invariants", lambda *args: None)
    monkeypatch.setattr(gold_job, "build_monthly_report", lambda *args: Frame())
    monkeypatch.setattr(
        gold_job,
        "_write_all_csv",
        lambda *args: pytest.fail("dry-run에서 CSV writer 호출"),
    )

    result = gold_job.main(
        [
            "--year", "2026",
            "--month", "8",
            "--threshold_profit_increase", "600",
            "--monthly_taxi_trip_path", "hvfhv.parquet",
            "--driver_vehicle_monthly_snapshot_path", "driver.parquet",
            "--lease_vehicle_inventory_path", "inventory.parquet",
            "--fuel_price_path", "fuel.parquet",
            "--dry-run",
        ]
    )

    assert result is None
