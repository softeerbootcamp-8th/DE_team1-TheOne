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

from schema.silver import CLEAN_MONTHLY_TAXI_TRIP_REQUIRED_NON_NULL


FINAL_SCHEMA = StructType(
    [
        StructField("taxi_id", StringType(), False),
        StructField("hvfhs_license_num", StringType(), False),
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

# 존재·타입 검사는 REQUIRED_COLUMNS 전체, 필수값(non-null) 검사는 이 목록으로 합니다.
# 순서를 스키마에서 그대로 가져와 로그·검증 순서가 계약과 같게 둡니다.
REQUIRED_NON_NULL_COLUMNS = [
    name for name in REQUIRED_COLUMNS if name in CLEAN_MONTHLY_TAXI_TRIP_REQUIRED_NON_NULL
]
