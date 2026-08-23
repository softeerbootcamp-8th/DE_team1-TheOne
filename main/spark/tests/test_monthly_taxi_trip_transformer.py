"""Monthly Taxi Trip Bronze→Silver 새 스키마와 원천 운행 등급 전달 회귀 테스트.

1. 새 14컬럼을 Silver 스키마와 순서·타입까지 맞춰 전달
2. Uber Comfort와 Lyft Extra Comfort를 추정 없이 보존
3. 누락 컬럼과 잘못된 license·등급 조합을 실패 처리
4. 임계치 미만의 잘못된 행만 제거
5. 필수값이 아닌 `on_scene_datetime` 이 전 행 NULL 이어도 살아남음
"""

from datetime import datetime

import pytest

from main.spark.jobs.bronze_to_silver.monthly_taxi_trip_bronze_to_silver.transformer import (
    FINAL_SCHEMA,
    MonthlyTaxiTripCleanTransformer,
)
from pyspark.sql.types import StructType
from shared.spark.common.session import get_or_create_spark_session

# 입력(Bronze)은 Silver 계약에서 파생 컬럼만 뺀 모양입니다.
BRONZE_INPUT_SCHEMA = StructType(
    [field for field in FINAL_SCHEMA if field.name != "year_month"]
)


@pytest.fixture(scope="module")
def spark():
    session = get_or_create_spark_session("test_monthly_taxi_trip_transformer")
    yield session
    session.stop()


def _row(**overrides) -> dict:
    row = {
        "taxi_id": "taxi-1",
        "hvfhs_license_num": "HV0003",
        "on_scene_datetime": datetime(2026, 8, 1, 9, 55),
        "pickup_datetime": datetime(2026, 8, 1, 10, 0),
        "dropoff_datetime": datetime(2026, 8, 1, 10, 20),
        "PULocationID": 1,
        "DOLocationID": 2,
        "pickup_zone": "Central Park",
        "dropoff_zone": "JFK Airport",
        "trip_miles": 5.0,
        "trip_time": 1200,
        "driver_pay": 20.0,
        "tips": 2.0,
        "estimated_service_tier": "Comfort",
    }
    row.update(overrides)
    return row


def test_on_scene_datetime이_전부_NULL이어도_행이_살아남는다(spark):
    """원천이 이 컬럼을 채우지 않는 달이 있습니다(#582). 필수값 검사에서 빠졌으므로
    전 행 NULL 이어도 불합격 0건이어야 합니다 — 예전 계약에서는 100% 탈락했습니다."""
    rows = [_row(on_scene_datetime=None), _row(taxi_id="taxi-2", on_scene_datetime=None)]

    result = MonthlyTaxiTripCleanTransformer(error_threshold=0.05).transform(
        spark.createDataFrame(rows, schema=BRONZE_INPUT_SCHEMA)
    )

    assert result.count() == 2
    assert result.filter("on_scene_datetime is not null").count() == 0


def test_새_14컬럼과_원천_운행등급을_Silver에_그대로_전달한다(spark):
    rows = [
        _row(),
        _row(
            taxi_id="taxi-2",
            hvfhs_license_num="HV0005",
            estimated_service_tier="Extra Comfort",
        ),
    ]

    result = MonthlyTaxiTripCleanTransformer(error_threshold=0.5).transform(
        spark.createDataFrame(rows)
    )

    assert result.schema.names == FINAL_SCHEMA.names
    assert [field.dataType for field in result.schema] == [
        field.dataType for field in FINAL_SCHEMA
    ]
    assert {
        (row["hvfhs_license_num"], row["estimated_service_tier"])
        for row in result.collect()
    } == {("HV0003", "Comfort"), ("HV0005", "Extra Comfort")}
    assert {row["year_month"] for row in result.collect()} == {"2026-08"}


def test_필수컬럼이_누락되면_실패한다(spark):
    row = _row()
    del row["estimated_service_tier"]

    with pytest.raises(ValueError, match="필수 컬럼"):
        MonthlyTaxiTripCleanTransformer().transform(spark.createDataFrame([row]))


def test_license와_등급_조합이_잘못되고_임계치_이상이면_실패한다(spark):
    rows = [_row(), _row(estimated_service_tier="Extra Comfort")]

    with pytest.raises(ValueError, match="불합격 비율"):
        MonthlyTaxiTripCleanTransformer(error_threshold=0.5).transform(
            spark.createDataFrame(rows)
        )


def test_임계치_미만의_잘못된_등급_행만_제거한다(spark):
    rows = [
        _row(taxi_id=f"taxi-{index}", trip_miles=float(index + 1))
        for index in range(20)
    ]
    rows[-1]["estimated_service_tier"] = "Extra Comfort"

    result = MonthlyTaxiTripCleanTransformer(error_threshold=0.1).transform(
        spark.createDataFrame(rows)
    )

    assert result.count() == 19
