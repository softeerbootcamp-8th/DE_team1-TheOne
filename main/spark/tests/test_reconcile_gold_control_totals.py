"""Silver 운행 합계가 기사별 집계를 거쳐 보존되는지 확인.

조인 키 누락은 `_require_all_join_keys_match` 가 막지만 그건 "짝이 있나" 를 본다.
행 수가 맞아도 값이 밀리는 사고는 합계로만 잡힌다 — 그래서 별도 검사가 필요하다.

실측(NYC 2026-01, 운행 675,676건)에서 mileage·driver_pay·tips 가 센트 단위까지
보존됐다. 그 상태를 고정한다.
"""

import pytest

from main.spark.jobs.silver_to_gold.transformer import reconcile_gold_control_totals
from shared.spark.common.session import get_or_create_spark_session


@pytest.fixture(scope="module")
def spark():
    session = get_or_create_spark_session("test_reconcile_gold_control_totals")
    yield session
    session.stop()


_TRIP_COLUMNS = ["taxi_id", "trip_miles", "driver_pay", "tips"]
_METRIC_COLUMNS = ["driver_id", "monthly_mileage", "monthly_driver_pay", "monthly_tips"]


def _trips(spark, rows):
    return spark.createDataFrame(rows, _TRIP_COLUMNS)


def _metrics(spark, rows):
    return spark.createDataFrame(rows, _METRIC_COLUMNS)


def test_합계가_보존되면_통과한다(spark):
    trips = _trips(spark, [
        ("taxi-1", 10.0, 100.0, 5.0),
        ("taxi-1", 2.5, 30.0, 1.0),
        ("taxi-2", 7.5, 70.0, 4.0),
    ])
    # 기사별로 묶여도 총합은 같아야 한다.
    metrics = _metrics(spark, [
        ("driver-1", 12.5, 130.0, 6.0),
        ("driver-2", 7.5, 70.0, 4.0),
    ])

    reconcile_gold_control_totals(trips, metrics)


def test_운행이_조인에서_빠지면_막는다(spark):
    """Silver 운행에는 driver_id 가 없어 스냅샷 조인으로 기사가 붙는다.

    스냅샷이 없는 taxi_id 의 운행은 조인에서 빠지는데, 지금까지는 그게 일어나도
    아무도 몰랐다.
    """
    trips = _trips(spark, [
        ("taxi-1", 10.0, 100.0, 5.0),
        ("taxi-2", 7.5, 70.0, 4.0),   # 스냅샷이 없어 집계에서 사라진 운행
    ])
    metrics = _metrics(spark, [("driver-1", 10.0, 100.0, 5.0)])

    with pytest.raises(ValueError, match="운행 합계가 보존되지 않았습니다"):
        reconcile_gold_control_totals(trips, metrics)


def test_행_수가_맞아도_값이_밀리면_막는다(spark):
    """건수 대조만으로는 못 잡는 계열 — 합계가 있어야 걸린다."""
    trips = _trips(spark, [("taxi-1", 10.0, 100.0, 5.0)])
    metrics = _metrics(spark, [("driver-1", 10.0, 999.0, 5.0)])

    with pytest.raises(ValueError, match="driver_pay"):
        reconcile_gold_control_totals(trips, metrics)


def test_부동소수점_오차는_통과한다(spark):
    """Spark 는 집계 순서가 실행마다 달라질 수 있어 정확히 같기를 요구하면 안 된다."""
    trips = _trips(spark, [("taxi-1", 0.1, 0.2, 0.3), ("taxi-1", 0.2, 0.1, 0.3)])
    metrics = _metrics(spark, [
        ("driver-1", 0.1 + 0.2, 0.2 + 0.1, 0.3 + 0.3 + 1e-12),
    ])

    reconcile_gold_control_totals(trips, metrics)


def test_tips_가_NULL_이면_0으로_센다(spark):
    """집계는 `coalesce(tips, 0.0)` 로 넣으므로 대조도 같은 규칙이어야 한다."""
    from pyspark.sql.types import (
        DoubleType,
        StringType,
        StructField,
        StructType,
    )

    schema = StructType([
        StructField("taxi_id", StringType()),
        StructField("trip_miles", DoubleType()),
        StructField("driver_pay", DoubleType()),
        StructField("tips", DoubleType()),
    ])
    trips = spark.createDataFrame([("taxi-1", 10.0, 100.0, None)], schema)
    metrics = _metrics(spark, [("driver-1", 10.0, 100.0, 0.0)])

    reconcile_gold_control_totals(trips, metrics)
