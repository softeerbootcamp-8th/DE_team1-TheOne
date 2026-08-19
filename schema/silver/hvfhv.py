"""[월별 택시 운행 기록] Spark Silver 스키마."""

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


FINAL_SCHEMA = StructType(
    [
        StructField("taxi_id", StringType(), False),
        StructField("hvfhs_license_num", StringType(), False),
        StructField("on_scene_datetime", TimestampType(), True),
        StructField("pickup_datetime", TimestampType(), True),
        StructField("dropoff_datetime", TimestampType(), True),
        StructField("PULocationID", IntegerType(), True),
        StructField("DOLocationID", IntegerType(), True),
        StructField("pickup_zone", StringType(), True),
        StructField("dropoff_zone", StringType(), True),
        StructField("trip_miles", DoubleType(), True),
        StructField("trip_time", LongType(), True),
        StructField("driver_pay", DoubleType(), True),
        StructField("tips", DoubleType(), True),
        StructField("estimated_service_tier", StringType(), False),
        StructField("year_month", StringType(), True),
    ]
)

REQUIRED_COLUMNS = [
    "taxi_id",
    "hvfhs_license_num",
    "on_scene_datetime",
    "pickup_datetime",
    "dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "pickup_zone",
    "dropoff_zone",
    "trip_miles",
    "trip_time",
    "driver_pay",
    "tips",
    "estimated_service_tier",
]
