"""[NYC TLC HVFHV Trip Record] Silver 스키마.

spark 런타임(pyspark `StructType`)입니다 — `schema/silver/`의 다른 파일들(pyarrow)과
타입 표현이 다릅니다. lambda 이미지에는 pyspark 가 없으니 이 모듈은 spark 쪽
(`spark/jobs/bronze_to_silver/hvfhv/transformer.py`)에서만 import 합니다.
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
    StructField("taxi_id", StringType(), True),
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
]
