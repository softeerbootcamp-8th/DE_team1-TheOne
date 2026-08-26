"""기사 배정에서 사용하는 택시 구역쌍 이동시간 스키마."""

from pyspark.sql.types import DoubleType, LongType, StructField, StructType


TAXI_ZONE_TRAVEL_TIMES_SCHEMA = StructType(
    [
        StructField("from_location_id", LongType(), False),
        StructField("to_location_id", LongType(), False),
        StructField("travel_minutes", DoubleType(), False),
        # 몇 건에서 나온 값인지. 배정 결과를 의심할 때 먼저 보는 값입니다.
        StructField("trip_count", LongType(), False),
    ]
)
