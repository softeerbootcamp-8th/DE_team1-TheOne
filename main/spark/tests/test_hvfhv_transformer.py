"""HVFHV Bronze→Silver 새 스키마와 원천 운행 등급 전달 회귀 테스트.

1. 새 14컬럼을 Silver 스키마와 순서·타입까지 맞춰 전달
2. Uber Comfort와 Lyft Extra Comfort를 추정 없이 보존
3. 누락 컬럼과 잘못된 license·등급 조합을 실패 처리
4. 임계치 미만의 잘못된 행만 제거
"""

from datetime import datetime

import pytest

from main.spark.jobs.bronze_to_silver.hvfhv.transformer import (
    FINAL_SCHEMA,
    HVFHVCleanTransformer,
)
from shared.spark.common.session import get_or_create_spark_session


@pytest.fixture(scope="module")
def spark():
    session = get_or_create_spark_session("test_hvfhv_transformer")
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


def test_새_14컬럼과_원천_운행등급을_Silver에_그대로_전달한다(spark):
    rows = [
        _row(),
        _row(
            taxi_id="taxi-2",
            hvfhs_license_num="HV0005",
            estimated_service_tier="Extra Comfort",
        ),
    ]

    result = HVFHVCleanTransformer(error_threshold=0.5).transform(
        spark.createDataFrame(rows)
    )

    assert result.schema.names == [*FINAL_SCHEMA.names, "year_month"]
    assert [field.dataType.simpleString() for field in result.schema][:-1] == [
        "string",
        "string",
        "timestamp",
        "timestamp",
        "timestamp",
        "int",
        "int",
        "string",
        "string",
        "double",
        "bigint",
        "double",
        "double",
        "string",
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
        HVFHVCleanTransformer().transform(spark.createDataFrame([row]))


def test_license와_등급_조합이_잘못되고_임계치_이상이면_실패한다(spark):
    rows = [_row(), _row(estimated_service_tier="Extra Comfort")]

    with pytest.raises(ValueError, match="불합격 비율"):
        HVFHVCleanTransformer(error_threshold=0.5).transform(
            spark.createDataFrame(rows)
        )


def test_임계치_미만의_잘못된_등급_행만_제거한다(spark):
    rows = [
        _row(taxi_id=f"taxi-{index}", trip_miles=float(index + 1))
        for index in range(20)
    ]
    rows[-1]["estimated_service_tier"] = "Extra Comfort"

    result = HVFHVCleanTransformer(error_threshold=0.1).transform(
        spark.createDataFrame(rows)
    )

    assert result.count() == 19
