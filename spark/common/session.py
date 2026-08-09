from pyspark.sql import SparkSession


def get_or_create_spark_session(app_name: str) -> SparkSession:
    return SparkSession.builder.appName(app_name).master("local[1]").getOrCreate()
