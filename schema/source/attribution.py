"""합성 운행을 기사·차량에 배정한 attribution 저장 스키마."""

from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


ATTRIBUTION_SCHEMA = StructType(
    [
        StructField("trip_key", StringType(), False),
        StructField("driver_id", StringType(), False),
        StructField("taxi_id", StringType(), False),
        StructField("service_date", DateType(), False),
        StructField("trip_sequence", IntegerType(), False),
        StructField("pickup_datetime", TimestampType(), False),
        StructField("dropoff_datetime", TimestampType(), False),
        StructField("PULocationID", IntegerType(), False),
        StructField("DOLocationID", IntegerType(), False),
        StructField("preference_score", DoubleType(), False),
        StructField("tie_break", StringType(), False),
        StructField("deadhead_minutes", DoubleType(), False),
    ]
)
