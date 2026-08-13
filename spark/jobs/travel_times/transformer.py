"""HVFHV 실측 운행에서 구역쌍 이동시간을 뽑습니다.

기사 배정(`jobs/driver_assignment/allocator.py`)은 "직전 운행이 끝난 구역에서 다음
운행 출발 구역까지 몇 분 걸리나" 를 알아야 공차(deadhead)를 계산합니다. 그 값을
외부에서 새로 받아올 필요는 없습니다 — **이미 가진 트립의 `trip_time` 이 그 구역쌍의
실측 이동시간**입니다.

대표값은 평균이 아니라 **중앙값**입니다. 같은 구역쌍이라도 공항 진입 정체나 사고로
몇 배씩 튀는 트립이 섞이는데, 평균은 그런 꼬리에 끌려 올라갑니다. 이동시간이 실제보다
길게 잡히면 배정에서 후보가 과하게 걸러져 배정률이 조용히 떨어집니다.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, expr
from pyspark.sql.types import DoubleType, LongType, StructField, StructType

# `allocator.TRAVEL_COLUMNS` 와 맞춰야 합니다. 한쪽만 바꾸면 실패하지 않고
# 배정 후보가 통째로 걸러집니다 (없는 구역쌍은 예외가 아니라 후보 제외라서).
SCHEMA = StructType(
    [
        StructField("from_location_id", LongType(), False),
        StructField("to_location_id", LongType(), False),
        StructField("travel_minutes", DoubleType(), False),
        # 몇 건에서 나온 값인지. 배정 결과를 의심할 때 먼저 보는 값입니다.
        StructField("trip_count", LongType(), False),
    ]
)

REQUIRED_COLUMNS = ("PULocationID", "DOLocationID", "trip_time")

# 이보다 적게 관측된 구역쌍은 버립니다. 1건짜리 이동시간은 그 트립의 사정일 뿐이고,
# 그 값이 크면 해당 구역쌍으로 이어지는 배정이 전부 막힙니다.
DEFAULT_MIN_TRIPS = 5

# 이 범위를 벗어난 트립은 이동시간 표본에서 뺍니다. Silver 정제가 이미 0 < trip_time
# <= 86400 을 걸렀지만, 6시간짜리 구역 간 이동은 대표값으로 쓸 수 없습니다.
MAX_TRIP_MINUTES = 180.0


def build_travel_times(
    trips: DataFrame, min_trips: int = DEFAULT_MIN_TRIPS
) -> DataFrame:
    """구역쌍별 이동시간 중앙값. 입력은 HVFHV Silver 입니다."""
    missing = [name for name in REQUIRED_COLUMNS if name not in trips.columns]
    if missing:
        raise ValueError(f"HVFHV Silver 에 필수 컬럼이 없습니다: {missing}")
    if min_trips < 1:
        raise ValueError(f"min_trips 는 1 이상이어야 합니다: {min_trips}")

    minutes = col("trip_time") / 60.0
    usable = trips.filter(
        col("PULocationID").isNotNull()
        & col("DOLocationID").isNotNull()
        & col("trip_time").isNotNull()
        & (minutes > 0)
        & (minutes <= MAX_TRIP_MINUTES)
    )

    aggregated = (
        usable.groupBy(
            col("PULocationID").cast("long").alias("from_location_id"),
            col("DOLocationID").cast("long").alias("to_location_id"),
        )
        .agg(
            # percentile_approx 는 정확한 중앙값이 아니지만, 구역쌍당 수천~수만 건이라
            # 오차가 분 단위 판단에 영향을 주지 않습니다. 정확한 백분위는 전체 정렬이
            # 필요해 월 2천만 행에서는 비쌉니다.
            expr("percentile_approx(trip_time / 60.0, 0.5)").alias("travel_minutes"),
            count("*").alias("trip_count"),
        )
        .filter(col("trip_count") >= min_trips)
    )

    return aggregated.select(
        col("from_location_id"),
        col("to_location_id"),
        col("travel_minutes").cast("double"),
        col("trip_count"),
    )
