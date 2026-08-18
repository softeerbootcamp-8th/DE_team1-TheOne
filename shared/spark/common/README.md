# shared/spark/common

Spark job 2개 이상이 같이 쓰는 코드 
pyspark 등 Spark 전용 의존성 사용 가능

- ex, `session.py`, `get_or_create_spark_session(app_name)`
- ex, `io.py`, `SparkParquetExtractor`, `SparkParquetLoader`