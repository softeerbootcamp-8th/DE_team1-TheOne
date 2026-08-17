"""기사 운행 이력 Silver 생성 시나리오. 이슈 #456.

1. 운행에 그 시점의 기사·계약·차량이 붙고, 같은 이름 컬럼이 두 번 들어가지 않는다
2. 진행 중인 계약(`lease_ended_on` NULL)도 매칭된다
3. 미매칭·다중매칭·리스 기간 겹침·월 불일치·trip_key 중복은 명시적 실패
4. 같은 입력 재실행 시 결과가 같다
"""

from datetime import date, datetime

import pytest
from pyspark.sql.functions import lit

from common.session import get_or_create_spark_session
from jobs.driver_trip.transformer import build_driver_trip

TRIP_SCHEMA = (
    "trip_key string, pickup_datetime timestamp, dropoff_datetime timestamp, "
    "PULocationID int, DOLocationID int, trip_miles double, trip_time bigint, "
    "base_passenger_fare double, tolls double, bcf double, sales_tax double, "
    "congestion_surcharge double, airport_fee double, tips double, driver_pay double, "
    "platform_name string, estimated_service_tier string, taxi_id string, "
    "driver_id string, taxi_model_id string, year_month string, pickup_borough string, "
    "pickup_zone string, pickup_service_zone string, dropoff_borough string, "
    "dropoff_zone string, dropoff_service_zone string"
)
LEASE_SCHEMA = (
    "lease_id string, customer_id string, driver_id string, taxi_id string, "
    "make_key string, model_key string, model_year bigint, "
    "lease_started_on date, lease_ended_on date"
)


@pytest.fixture(scope="module")
def spark():
    session = get_or_create_spark_session("test_driver_trip")
    yield session
    session.stop()


def _trip(**overrides):
    row = {
        "trip_key": "t1", "pickup_datetime": datetime(2024, 3, 4, 9),
        "dropoff_datetime": datetime(2024, 3, 4, 9, 20), "PULocationID": 1,
        "DOLocationID": 2, "trip_miles": 5.0, "trip_time": 1200,
        "base_passenger_fare": 30.0, "tolls": 0.0, "bcf": 0.0, "sales_tax": 0.0,
        "congestion_surcharge": 0.0, "airport_fee": 0.0, "tips": 2.0,
        "driver_pay": 20.0, "platform_name": "Uber",
        "estimated_service_tier": "Standard", "taxi_id": "x1",
        # HVFHV Clean Silver 는 이 둘을 NULL 자리표시로 들고 옵니다.
        "driver_id": None, "taxi_model_id": None, "year_month": "2024-03",
        "pickup_borough": "Manhattan", "pickup_zone": "A", "pickup_service_zone": "Boro",
        "dropoff_borough": "Queens", "dropoff_zone": "B", "dropoff_service_zone": "Boro",
    }
    row.update(overrides)
    return row


def _lease(**overrides):
    row = {
        "lease_id": "l1", "customer_id": "c1", "driver_id": "d1", "taxi_id": "x1",
        "make_key": "TOYOTA", "model_key": "CAMRY", "model_year": 2023,
        "lease_started_on": date(2024, 1, 1), "lease_ended_on": None,
    }
    row.update(overrides)
    return row


def _build(spark, trips=None, leases=None):
    # `or` 로 기본값을 주면 "빈 입력" 시나리오가 조용히 기본값으로 바뀝니다.
    trips = [_trip()] if trips is None else trips
    leases = [_lease()] if leases is None else leases
    return build_driver_trip(
        spark.createDataFrame(trips, schema=TRIP_SCHEMA),
        spark.createDataFrame(leases, schema=LEASE_SCHEMA),
        year_month="2024-03",
        snapshot_date=date(2024, 3, 1),
    )


def test_운행에_그_시점의_기사와_계약과_차량이_붙는다(spark):
    result = _build(spark)
    row = result.first()

    assert (row.trip_key, row.taxi_id, row.driver_id) == ("t1", "x1", "d1")
    assert (row.customer_id, row.lease_id) == ("c1", "l1")
    assert (row.make_key, row.model_key, row.model_year) == ("TOYOTA", "CAMRY", 2023)
    assert (row.year_month, row.snapshot_date) == ("2024-03", date(2024, 3, 1))
    # 배정이 사라졌으니 그 흔적도 남으면 안 됩니다.
    assert not {"assignment_version", "assignment_seed", "trip_sequence"} & set(result.columns)
    # `select` 는 중복 이름을 허용해 조용히 지나가고 **쓰기 단계에서야**
    # COLUMN_ALREADY_EXISTS 로 죽습니다. 여기서 잡아야 합니다.
    duplicated = sorted({c for c in result.columns if result.columns.count(c) > 1})
    assert not duplicated, f"중복 컬럼: {duplicated}"


def test_계약_기간_밖의_운행은_그_계약에_붙지_않는다(spark):
    """`lease_ended_on` 은 배타적 상한이라 그 날 운행은 다음 계약 몫입니다."""
    leases = [
        _lease(lease_id="l1", driver_id="d1", lease_started_on=date(2024, 1, 1),
               lease_ended_on=date(2024, 3, 4)),
        _lease(lease_id="l2", driver_id="d2", lease_started_on=date(2024, 3, 4),
               lease_ended_on=None),
    ]

    assert _build(spark, leases=leases).first().driver_id == "d2"


@pytest.mark.parametrize(
    "violation",
    ["unmatched", "multiple", "overlap", "other_month", "duplicate_key", "empty_lease"],
)
def test_미매칭_다중매칭_기간겹침_월불일치_키중복은_ValueError다(spark, violation):
    trips, leases = [_trip()], [_lease()]
    if violation == "unmatched":
        trips = [_trip(taxi_id="x9")]
    elif violation == "multiple":
        # 기간이 안 겹치게 나눠 놓은 두 계약이 아니라, 같은 날을 함께 덮는 두 계약.
        leases = [_lease(lease_id="l1"), _lease(lease_id="l2", driver_id="d2")]
    elif violation == "overlap":
        leases = [
            _lease(lease_id="l1", lease_ended_on=date(2024, 6, 1)),
            _lease(lease_id="l2", driver_id="d2", lease_started_on=date(2024, 5, 1)),
        ]
    elif violation == "other_month":
        trips = [_trip(year_month="2024-02")]
    elif violation == "duplicate_key":
        trips = [_trip(), _trip(pickup_datetime=datetime(2024, 3, 5, 9))]
    else:
        leases = []

    with pytest.raises(ValueError):
        _build(spark, trips, leases).count()


def test_같은_입력을_다시_돌려도_행수와_키와_값이_같다(spark):
    """배정 seed 가 없어졌으니 재실행 결과가 흔들릴 자리가 없어야 합니다."""
    first = sorted(row.asDict().items() for row in _build(spark).collect())
    second = sorted(row.asDict().items() for row in _build(spark).collect())

    assert first == second


def test_다른_월_파티션은_재실행에도_보존된다(spark, tmp_path):
    from common.io import SparkParquetLoader

    loader = SparkParquetLoader(str(tmp_path), partition_by=["year_month"])
    march = _build(spark)
    loader.write(march.withColumn("year_month", lit("2024-02")))
    loader.write(march)
    loader.write(march)

    restored = spark.read.parquet(str(tmp_path))
    assert {
        row.year_month: row["count"]
        for row in restored.groupBy("year_month").count().collect()
    } == {"2024-02": 1, "2024-03": 1}
