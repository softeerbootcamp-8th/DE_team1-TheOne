"""GX Spark 런타임 연결 시나리오.

1. GX Core는 운영 의존성이고 pyspark는 EMR 제공/로컬 dev 의존성으로 유지
2. GX Spark Datasource가 기존 SparkSession의 SparkContext를 재사용
3. 전체 Spark DataFrame Batch에 최소 Expectation을 실행

Monthly Taxi Trip 품질 규칙은 #1108, Data Docs·Airflow 노출은 #1117에서 검증한다.
"""

import tomllib
from pathlib import Path

import pytest

from shared.spark.common.session import get_or_create_spark_session


PROJECT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def spark():
    session = get_or_create_spark_session("test_gx_spark_runtime")
    yield session
    session.stop()


def test_GX_Core는_운영의존성이고_pyspark는_dev에만_둔다():
    project = tomllib.loads((PROJECT / "pyproject.toml").read_text(encoding="utf-8"))

    dependencies = project["project"]["dependencies"]
    dev_dependencies = project["dependency-groups"]["dev"]

    assert "great-expectations==1.20.0" in dependencies
    assert not any(dependency.startswith("pyspark") for dependency in dependencies)
    assert "pyspark==3.5.6" in dev_dependencies

    lock = tomllib.loads((PROJECT / "uv.lock").read_text(encoding="utf-8"))
    (spark_package,) = [package for package in lock["package"] if package["name"] == "tlc-spark"]

    assert {"name": "great-expectations"} in spark_package["dependencies"]
    assert {"name": "pyspark"} in spark_package["dev-dependencies"]["dev"]
    assert {"name": "pyspark"} not in spark_package["dependencies"]


def test_GX_Spark_Datasource가_기존_SparkContext로_전체_Batch를_검증한다(spark):
    import great_expectations as gx

    application_id = spark.sparkContext.applicationId
    dataframe = spark.createDataFrame([(1,), (2,)], ["trip_id"])

    context = gx.get_context(mode="ephemeral")
    datasource = context.data_sources.add_spark(name="runtime_smoke_source")
    batch_definition = (
        datasource.add_dataframe_asset(name="runtime_smoke_asset")
        .add_batch_definition_whole_dataframe("runtime_smoke_batch")
    )
    batch = batch_definition.get_batch(batch_parameters={"dataframe": dataframe})

    result = batch.validate(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="trip_id")
    )

    assert datasource.force_reuse_spark_context is True
    assert spark.sparkContext.applicationId == application_id
    assert result.success is True
    assert result.result["element_count"] == 2
