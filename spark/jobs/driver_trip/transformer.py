"""HVFHV Clean Silver 와 기사 리스 Clean Silver 를 운행 시점 기준으로 조인합니다.

기사-운행 매칭은 더 이상 여기서 만들지 않습니다. 가짜 데이터 API 가 운행마다
`taxi_id` 를 붙여 내보내고(#450), 그 `taxi_id` 를 리스 이력과 **기간으로** 이으면
운행 시점의 기사가 결정됩니다. 그래서 이 모듈에는 배정 알고리즘도 seed 도 없고,
같은 입력이면 항상 같은 결과가 나옵니다.

`lease_ended_on` 은 **배타적 상한**입니다 — 그 날부터 계약이 무효입니다.
`silver_to_gold/transformer.py::_lease_days_in_month` 도 같은 규칙을 씁니다.
"""

from datetime import date

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import coalesce, col, count, countDistinct, lit, to_date, when

from schema.silver.driver_vehicle_leases import SCHEMA as LEASE_SCHEMA
from schema.silver.hvfhv import FINAL_SCHEMA as TRIP_SCHEMA

# HVFHV Clean Silver 가 NULL 자리표시로 들고 있는 컬럼입니다 — 채우는 값은 리스 쪽에
# 있습니다. 빼지 않으면 아래 select 에 같은 이름이 두 번 들어가는데, `select` 는 중복
# 이름을 허용해 조용히 지나가고 **쓰기 단계에서야** COLUMN_ALREADY_EXISTS 로 죽습니다.
TRIP_PLACEHOLDER_COLUMNS = ("driver_id", "taxi_model_id")
TRIP_COLUMNS = [
    field.name for field in TRIP_SCHEMA if field.name not in TRIP_PLACEHOLDER_COLUMNS
]
# `taxi_id` 는 조인 키라 양쪽 값이 같습니다. 운행 쪽 하나만 싣습니다.
LEASE_COLUMNS = [name for name in LEASE_SCHEMA.names if name != "taxi_id"]

REQUIRED_TRIP_COLUMNS = {"trip_key", "taxi_id", "pickup_datetime", "year_month"}

# 진행 중인 계약은 `lease_ended_on` 이 NULL 입니다. 비교식에 NULL 을 그대로 두면 식
# 전체가 NULL(=거짓 취급)이 되어 열린 계약이 아무 운행에도 안 걸립니다.
OPEN_ENDED_DATE = "9999-12-31"


def _lease_ends(alias: str) -> Column:
    # Column 은 SparkContext 가 살아 있을 때만 만들 수 있어 모듈 상수로 둘 수 없습니다.
    return coalesce(col(f"{alias}.lease_ended_on"), to_date(lit(OPEN_ENDED_DATE)))


def lease_match_condition(trip_alias: str = "t", lease_alias: str = "l") -> Column:
    """`taxi_id` 가 같고 승차일이 `[lease_started_on, lease_ended_on)` 에 드는 조건.

    검증과 실제 조인이 **같은 식**을 써야 합니다. 따로 쓰면 검증을 통과한 입력이
    조인에서 다른 행 수를 내도 아무도 모릅니다.
    """
    pickup_date = to_date(col(f"{trip_alias}.pickup_datetime"))
    return (
        (col(f"{trip_alias}.taxi_id") == col(f"{lease_alias}.taxi_id"))
        & (col(f"{lease_alias}.lease_started_on") <= pickup_date)
        & (pickup_date < _lease_ends(lease_alias))
    )


def validate_trips(trips: DataFrame, year_month: str) -> None:
    """대상 월 운행만 있고, `trip_key` 가 null 없이 고유한지 봅니다."""
    missing = REQUIRED_TRIP_COLUMNS - set(trips.columns)
    if missing:
        raise ValueError(f"HVFHV Clean Silver 컬럼 누락: {sorted(missing)}")

    # 아래 넷을 따로 세면 그때마다 파티션을 다시 읽습니다. 한 번의 스캔으로 봅니다.
    stats = trips.agg(
        count(lit(1)).alias("rows"),
        countDistinct("trip_key").alias("distinct_keys"),
        count(when(col("trip_key").isNull(), 1)).alias("null_keys"),
        count(
            when(col("year_month").isNull() | (col("year_month") != year_month), 1)
        ).alias("other_month"),
        count(when(col("taxi_id").isNull(), 1)).alias("null_taxi"),
    ).first()

    if not stats or stats["rows"] == 0:
        raise ValueError(f"대상 월 HVFHV 운행이 없습니다: {year_month}")
    if stats["other_month"]:
        raise ValueError(
            f"요청 월이 아닌 운행이 {stats['other_month']}건 섞여 있습니다: {year_month}"
        )
    if stats["null_keys"] or stats["distinct_keys"] != stats["rows"]:
        raise ValueError("trip_key 는 null 없이 고유해야 합니다")
    if stats["null_taxi"]:
        raise ValueError("taxi_id 가 비어 있는 운행이 있습니다")


def validate_lease_periods(leases: DataFrame) -> None:
    """`lease_id` 가 고유하고, 같은 `taxi_id` 의 계약 기간이 겹치지 않는지 봅니다.

    기간이 겹치면 운행 한 건에 기사가 둘 붙습니다. 그 상태는 아래
    `validate_single_lease_per_trip` 에서도 걸리지만, 원인이 운행이 아니라 **리스
    쪽**이라는 것을 여기서 먼저 말해 줘야 어디를 고칠지 알 수 있습니다.
    """
    stats = leases.agg(
        count(lit(1)).alias("rows"),
        countDistinct("lease_id").alias("distinct_leases"),
    ).first()
    if not stats or stats["rows"] == 0:
        raise ValueError("기사 리스 Silver 가 비어 있습니다")
    if stats["rows"] != stats["distinct_leases"]:
        raise ValueError("기사 리스 Silver 의 lease_id 가 중복됩니다")

    a, b = leases.alias("a"), leases.alias("b")
    # 두 반개구간 [s1, e1) 과 [s2, e2) 는 s1 < e2 이고 s2 < e1 일 때 겹칩니다.
    # `lease_id` 부등호로 같은 쌍을 한 번만, 자기 자신은 아예 안 보게 합니다.
    overlapped = (
        a.join(
            b,
            (col("a.taxi_id") == col("b.taxi_id"))
            & (col("a.lease_id") < col("b.lease_id"))
            & (col("a.lease_started_on") < _lease_ends("b"))
            & (col("b.lease_started_on") < _lease_ends("a")),
            "inner",
        )
        .limit(1)
        .count()
    )
    if overlapped:
        raise ValueError("같은 taxi_id 의 리스 기간이 겹칩니다")


def validate_single_lease_per_trip(trips: DataFrame, leases: DataFrame) -> None:
    """운행 한 건이 리스 정확히 한 건에 걸리는지 봅니다 (운행당 기사 1명).

    조인 키 세 컬럼만 뽑아 따로 한 번 더 조인합니다. 본 조인 결과를 캐시해 두고
    재사용하는 편이 스캔은 한 번 적지만, 미매칭까지 보려면 outer join 결과를 통째로
    들고 있어야 합니다 — 운행 2천만 행의 전 컬럼이 그 대상입니다. 여기서는 세
    컬럼만 읽으므로(Parquet 컬럼 프루닝) 다시 조인하는 쪽이 쌉니다.
    """
    matched = trips.select("trip_key", "taxi_id", "pickup_datetime").alias("t").join(
        leases.select(
            "taxi_id", "lease_id", "lease_started_on", "lease_ended_on"
        ).alias("l"),
        lease_match_condition(),
        "left",
    )
    per_trip = matched.groupBy(col("t.trip_key")).agg(
        count(col("l.lease_id")).alias("_matches")
    )
    stats = per_trip.agg(
        count(when(col("_matches") == 0, 1)).alias("unmatched"),
        count(when(col("_matches") > 1, 1)).alias("multiple"),
    ).first()

    if stats["unmatched"]:
        raise ValueError(
            f"운행 시점의 리스를 찾지 못한 운행이 {stats['unmatched']}건 있습니다"
        )
    if stats["multiple"]:
        raise ValueError(
            f"리스가 둘 이상 걸린 운행이 {stats['multiple']}건 있습니다"
        )


def build_driver_trip(
    trips: DataFrame,
    leases: DataFrame,
    *,
    year_month: str,
    snapshot_date: date,
) -> DataFrame:
    """운행 한 건에 그 시점의 기사·계약·차량을 붙인 기사 운행 이력."""
    validate_trips(trips, year_month)
    validate_lease_periods(leases)
    validate_single_lease_per_trip(trips, leases)

    t, l = trips.alias("t"), leases.alias("l")
    return (
        t.join(l, lease_match_condition(), "inner")
        .select(
            *(col(f"t.{name}") for name in TRIP_COLUMNS),
            *(col(f"l.{name}") for name in LEASE_COLUMNS),
        )
        .withColumn("snapshot_date", lit(snapshot_date).cast("date"))
    )
