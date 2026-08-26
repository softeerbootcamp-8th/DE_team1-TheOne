"""Monthly Taxi Trip Bronze→Silver 새 스키마와 원천 운행 등급 전달 회귀 테스트.

1. 새 13컬럼을 Silver 스키마와 순서·타입까지 맞춰 전달
2. Uber Comfort와 Lyft Extra Comfort를 추정 없이 보존
3. 누락 컬럼과 잘못된 license·등급 조합을 실패 처리
4. 임계치 미만의 잘못된 행만 제거
5. `on_scene_datetime` 없는 입력을 정상 처리하고 출력 계약에서도 제외
6. GX Spark가 필터 전 전체 레코드의 NULL·타입·범위·등급을 판정
7. 경고 임계치는 관측하고 실패 임계치부터 작업을 중단
8. 원천 추가 컬럼을 GX까지 전달해 reconciliation에 보존
"""

import inspect
from datetime import datetime

import pytest

from main.spark.jobs.bronze_to_silver.monthly_taxi_trip_bronze_to_silver.transformer import (
    FINAL_SCHEMA,
    MonthlyTaxiTripCleanTransformer,
)
from main.spark.jobs.bronze_to_silver.monthly_taxi_trip_bronze_to_silver import (
    quality as quality_module,
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


def test_on_scene_datetime_없는_입력을_정상처리한다(spark):
    rows = [_row(), _row(taxi_id="taxi-2")]

    result = MonthlyTaxiTripCleanTransformer(error_threshold=0.05).transform(
        spark.createDataFrame(rows, schema=BRONZE_INPUT_SCHEMA)
    )

    assert result.count() == 2
    assert "on_scene_datetime" not in result.columns


def test_새_13컬럼과_원천_운행등급을_Silver에_그대로_전달한다(spark):
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


def test_제외_건수를_밖으로_내보낸다(spark):
    """Airflow 가 Bronze·Silver 행 수와 맞대볼 수 있도록 변환기가 센 값을 남긴다.

    `invalid` 는 사유별 합이 아니라 `total - valid` 다 — 한 행이 여러 사유에 걸릴 수
    있어 사유별로 더하면 중복 집계된다.
    """
    rows = [
        _row(),                                   # 통과
        _row(taxi_id="taxi-2", trip_miles=None),   # 필수값 NULL
        _row(taxi_id="taxi-3", trip_miles=5000.0),  # 값 범위 밖
        _row(taxi_id="taxi-4", estimated_service_tier="Lux"),  # 등급 밖
    ]

    transformer = MonthlyTaxiTripCleanTransformer(error_threshold=0.9)
    result = transformer.transform(spark.createDataFrame(rows, schema=BRONZE_INPUT_SCHEMA))

    recon = transformer.recon
    assert recon is not None, "변환기가 센 값을 남기지 않으면 대조할 수 없습니다"
    assert recon.total == 4
    assert recon.valid == result.count() == 1
    assert recon.invalid == 3
    assert recon.invalid == recon.total - recon.valid, "사유별 합이 아니라 total-valid"
    assert recon.as_payload()["invalid"] == 3


def test_변환_전에는_센_값이_없다():
    """`transform()` 전에는 셀 대상이 없다 — Loader 가 콜러블로 받는 이유다."""
    assert MonthlyTaxiTripCleanTransformer().recon is None


def test_GX는_필터전_전체_Spark_DataFrame을_직접_검증한다(
    spark, monkeypatch
):
    rows = [
        _row(taxi_id=f"taxi-{index}", trip_miles=float(index + 1))
        for index in range(100)
    ]
    rows[-1]["pickup_datetime"] = None
    rows[-1]["trip_miles"] = -1.0
    captured = {}
    original = quality_module._validate_gx_batch

    def capture(dataframe, expectations):
        captured["rows"] = dataframe.count()
        captured["columns"] = set(dataframe.columns)
        return original(dataframe, expectations)

    monkeypatch.setattr(quality_module, "_validate_gx_batch", capture)
    transformer = MonthlyTaxiTripCleanTransformer(
        warning_threshold=0.01,
        error_threshold=0.05,
    )

    result = transformer.transform(
        spark.createDataFrame(rows, schema=BRONZE_INPUT_SCHEMA)
    )

    assert captured["rows"] == 100
    assert {
        quality_module.MISSING_OR_TYPE_VALID_COLUMN,
        quality_module.VALUE_VALID_COLUMN,
        quality_module.SERVICE_TIER_VALID_COLUMN,
        quality_module.RECORD_VALID_COLUMN,
    } <= captured["columns"]
    assert result.count() == 99
    assert transformer.recon.invalid == 1
    assert transformer.recon.missing_or_type_mismatch == 1
    assert transformer.recon.invalid_value == 1
    assert transformer.recon.warning is True


def test_타입변환실패도_GX_NULL타입규칙으로_집계한다(spark):
    rows = []
    for index in range(20):
        row = {
            key: str(value) if not isinstance(value, datetime) else value.isoformat()
            for key, value in _row(taxi_id=f"taxi-{index}").items()
        }
        rows.append(row)
    rows[-1]["trip_miles"] = "not-a-number"
    transformer = MonthlyTaxiTripCleanTransformer(
        warning_threshold=0.01,
        error_threshold=0.1,
    )

    result = transformer.transform(spark.createDataFrame(rows))

    assert result.count() == 19
    assert transformer.recon.missing_or_type_mismatch == 1


def test_불량률이_실패임계치와_같으면_GX가_작업을_중단한다(spark):
    rows = [
        _row(taxi_id=f"taxi-{index}", trip_miles=float(index + 1))
        for index in range(20)
    ]
    rows[-1]["estimated_service_tier"] = "Lux"

    with pytest.raises(ValueError, match="불합격 비율.*5.00%.*이상"):
        MonthlyTaxiTripCleanTransformer(
            warning_threshold=0.01,
            error_threshold=0.05,
        ).transform(spark.createDataFrame(rows, schema=BRONZE_INPUT_SCHEMA))


def test_운영_GX검증은_collect와_toPandas를_사용하지_않는다():
    source = inspect.getsource(quality_module)

    assert ".collect(" not in source
    assert ".toPandas(" not in source


def test_원천_추가컬럼을_GX와_reconciliation에_남긴다(spark):
    from pyspark.sql.functions import lit

    dataframe = spark.createDataFrame([_row()]).withColumn("airport_fee", lit(2.5))
    transformer = MonthlyTaxiTripCleanTransformer(error_threshold=0.05)

    result = transformer.transform(dataframe)

    assert "airport_fee" not in result.columns
    assert transformer.recon.extra_columns == ("airport_fee",)
