from typing import Optional

from pyspark.sql import DataFrame, SparkSession

from pipeline_core.extractor import Extractor
from pipeline_core.loader import Loader, WriteResult


class SparkParquetExtractor(Extractor):
    """
    Bronze/Silver파티션을 parquet으로 읽어 DataFrame 반환.

    데이터셋마다 새로 안 만들고 경로만 다르게 재사용
    """

    def __init__(self, spark: SparkSession, path: str):
        self._spark = spark
        self._path = path
        self.name = f"spark_parquet:{path}"

    def extract(self) -> DataFrame:
        return self._spark.read.parquet(self._path)


class SparkParquetLoader(Loader):
    """
    DataFrame 을 parquet으로 적재
    Gold 용 PostgresLoader는 별도 추가
    """

    def __init__(self, path: str, partition_by: Optional[list[str]] = None):
        self._path = path
        self._partition_by = partition_by or []

    def write(self, data: DataFrame) -> WriteResult:
        writer = data.write.mode("overwrite")
        if self._partition_by:
            writer = writer.option("partitionOverwriteMode", "dynamic").partitionBy(*self._partition_by)
        writer.parquet(self._path)
        return WriteResult(location=self._path, row_count=data.count())
