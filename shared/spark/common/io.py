from contextlib import contextmanager
from fnmatch import fnmatchcase
from pathlib import Path, PurePosixPath
from tempfile import TemporaryDirectory
from typing import Iterator, Optional, Union
from urllib.parse import urlsplit

import boto3

from pyspark.sql import DataFrame, SparkSession

from pipeline_core.extractor import Extractor
from pipeline_core.loader import Loader, WriteResult
from shared.common.s3_reader import list_keys


@contextmanager
def stage_s3_parquet_inputs(
    *path_groups: Union[str, list[str]],
) -> Iterator[tuple[list[str], ...]]:
    """S3 Parquet 입력만 임시 로컬 파일로 내려 Spark Hadoop S3 의존성을 피합니다."""
    groups = [[group] if isinstance(group, str) else group for group in path_groups]
    if not any(
        urlsplit(path).scheme in {"s3", "s3a"}
        for group in groups
        for path in group
    ):
        yield tuple(groups)
        return

    with TemporaryDirectory(prefix="spark-s3-input-") as temporary_dir:
        client = boto3.client("s3")
        staged_groups: list[list[str]] = []
        file_index = 0
        for group in groups:
            staged: list[str] = []
            for path in group:
                parsed = urlsplit(path)
                if parsed.scheme not in {"s3", "s3a"}:
                    staged.append(path)
                    continue
                bucket, key = parsed.netloc, parsed.path.lstrip("/")
                if not bucket or not key:
                    raise ValueError(f"S3 Parquet 경로가 올바르지 않습니다: {path}")

                if "*" not in key:
                    keys = [key]
                elif PurePosixPath(key).name == "part-*.parquet" and key.count("*") == 1:
                    parent = PurePosixPath(key).parent
                    prefix = f"{parent.as_posix().rstrip('/')}/part-"
                    keys = sorted(
                        candidate
                        for candidate in list_keys(bucket, prefix)
                        if PurePosixPath(candidate).parent == parent
                        and fnmatchcase(PurePosixPath(candidate).name, "part-*.parquet")
                    )
                    if not keys:
                        raise FileNotFoundError(f"S3 Parquet 파일이 없습니다: {path}")
                else:
                    raise ValueError(f"지원하지 않는 S3 Parquet 패턴입니다: {path}")

                for object_key in keys:
                    target = Path(temporary_dir) / str(file_index) / PurePosixPath(object_key).name
                    target.parent.mkdir()
                    client.download_file(bucket, object_key, str(target))
                    staged.append(str(target))
                    file_index += 1
            staged_groups.append(staged)
        yield tuple(staged_groups)


class SparkParquetExtractor(Extractor):
    """
    Bronze/Silver파티션을 parquet으로 읽어 DataFrame 반환.

    데이터셋마다 새로 안 만들고 경로만 다르게 재사용.
    path 에 여러 파티션 파일 경로를 리스트로 주면 한 번에 합쳐서 읽음 (예: year_month range 백필).
    """

    def __init__(self, spark: SparkSession, path: Union[str, list[str]]):
        self._spark = spark
        self._path = path
        display_path = path if isinstance(path, str) else ",".join(path)
        self.name = f"spark_parquet:{display_path}"

    def extract(self) -> DataFrame:
        if isinstance(self._path, list):
            return self._spark.read.parquet(*self._path)
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
