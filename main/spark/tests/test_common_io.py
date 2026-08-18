"""SparkParquetLoader/Extractor 파티션 overwrite 회귀 테스트.

#165: partition_by 로 쓸 때 static overwrite(기본값)는 대상 디렉터리 전체를 지우고
쓴다. 그래서 2024-03 을 다시 적재하면 이미 있던 2024-02 파티션까지 같이 사라졌다.
`SparkParquetLoader.write` 는 `partitionOverwriteMode=dynamic` 으로 고쳐져 있는데,
이 파일이 없으면 누가 그 옵션을 지우거나 되돌려도 잡히지 않는다.

1. [필수] partition_by 있으면 다른 달 파티션을 남긴다 (#165 회귀)
2. [필수] partition_by 없으면 전체를 덮어쓴다 (dynamic 모드가 의도치 않게 새지 않음)
3. WriteResult.row_count 가 실제 기록 행 수와 같다
4. WriteResult.location 이 생성자에 준 경로와 같다
5. SparkParquetExtractor.extract 가 파티션 컬럼을 복원해서 읽는다
"""

import pytest

from shared.spark.common.io import SparkParquetExtractor, SparkParquetLoader
from shared.spark.common.session import get_or_create_spark_session


@pytest.fixture(scope="module")
def spark():
    session = get_or_create_spark_session("test_common_io")
    yield session
    session.stop()


def test_partition_by_있으면_다른_달_파티션을_남긴다_165_회귀(spark, tmp_path):
    path = str(tmp_path / "silver")

    feb = spark.createDataFrame([{"value": 1, "year_month": "2024-02"}])
    SparkParquetLoader(path, partition_by=["year_month"]).write(feb)

    mar = spark.createDataFrame([{"value": 2, "year_month": "2024-03"}])
    SparkParquetLoader(path, partition_by=["year_month"]).write(mar)

    result = SparkParquetExtractor(spark, path).extract()
    year_months = {row["year_month"] for row in result.collect()}
    assert year_months == {"2024-02", "2024-03"}


def test_partition_by_없으면_전체를_덮어쓴다(spark, tmp_path):
    path = str(tmp_path / "full")

    first = spark.createDataFrame([{"value": 1}])
    SparkParquetLoader(path).write(first)

    second = spark.createDataFrame([{"value": 2}])
    SparkParquetLoader(path).write(second)

    result = SparkParquetExtractor(spark, path).extract()
    assert [row["value"] for row in result.collect()] == [2]


def test_row_count가_실제_기록_행_수와_같다(spark, tmp_path):
    path = str(tmp_path / "count")
    df = spark.createDataFrame([{"value": i} for i in range(5)])

    result = SparkParquetLoader(path).write(df)

    assert result.row_count == 5
    assert SparkParquetExtractor(spark, path).extract().count() == 5


def test_location이_생성자_경로와_같다(spark, tmp_path):
    path = str(tmp_path / "loc")
    df = spark.createDataFrame([{"value": 1}])

    result = SparkParquetLoader(path).write(df)

    assert result.location == path


def test_extract가_파티션_컬럼을_복원한다(spark, tmp_path):
    path = str(tmp_path / "partitioned")
    df = spark.createDataFrame([{"value": 1, "year_month": "2024-01"}])
    SparkParquetLoader(path, partition_by=["year_month"]).write(df)

    result = SparkParquetExtractor(spark, path).extract()

    assert "year_month" in result.columns
    assert result.collect()[0]["year_month"] == "2024-01"
