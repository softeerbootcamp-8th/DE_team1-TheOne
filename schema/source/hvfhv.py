"""driver_assignment가 TLC 원천을 정제할 때 사용하는 Spark 스키마.

합성 API를 읽는 main 파이프라인의 Silver 계약은 ``schema.silver``가 소유합니다.
이 스키마는 TLC 27컬럼을 쓰는 별도 source job용입니다.

pyspark 의존이라 schema/source/__init__.py 와 분리합니다 — aws_lambda 쪽처럼
pyspark 가 없는 환경에서도 다른 schema.source 스키마는 그대로 import 되어야 합니다.
"""

from pyspark.sql.types import (
    DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType
)

FINAL_SCHEMA = StructType([
    StructField("trip_key", StringType(), False),
    StructField("pickup_datetime", TimestampType(), True),
    StructField("dropoff_datetime", TimestampType(), True),
    StructField("PULocationID", IntegerType(), True),
    StructField("DOLocationID", IntegerType(), True),
    StructField("trip_miles", DoubleType(), True),
    StructField("trip_time", LongType(), True),
    StructField("base_passenger_fare", DoubleType(), True),
    StructField("tolls", DoubleType(), True),
    StructField("bcf", DoubleType(), True),
    StructField("sales_tax", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True),
    StructField("airport_fee", DoubleType(), True),
    StructField("tips", DoubleType(), True),
    StructField("driver_pay", DoubleType(), True),
    StructField("platform_name", StringType(), False),
    StructField("estimated_service_tier", StringType(), False),
    StructField("taxi_id", StringType(), False),
    StructField("driver_id", StringType(), True),
    StructField("taxi_model_id", StringType(), True),
    StructField("year_month", StringType(), True),
    StructField("pickup_borough", StringType(), True),
    StructField("pickup_zone", StringType(), True),
    StructField("pickup_service_zone", StringType(), True),
    StructField("dropoff_borough", StringType(), True),
    StructField("dropoff_zone", StringType(), True),
    StructField("dropoff_service_zone", StringType(), True)
])

REQUIRED_COLUMNS = [
    "pickup_datetime",
    "dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "trip_miles",
    "trip_time",
    "base_passenger_fare",
    "driver_pay",
    "hvfhs_license_num",
    "taxi_id",
]
