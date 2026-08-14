from typing import Optional

from pyspark.sql import SparkSession


def get_or_create_spark_session(app_name: str, driver_memory: Optional[str] = None) -> SparkSession:
    """local[4] 세션 생성. driver_memory 는 프로세스의 첫 세션 생성 전에만 적용됨(JVM 힙은 이후 재조정 불가)."""
    builder = SparkSession.builder.appName(app_name).master("local[4]").config("spark.driver.bindAddress", "127.0.0.1")
    if driver_memory:
        builder = builder.config("spark.driver.memory", driver_memory)
    return builder.getOrCreate()

