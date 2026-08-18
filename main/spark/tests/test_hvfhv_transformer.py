"""HVFHV bronze_to_silver 정제 규칙(`transformer.py`) 검증. 이슈 #199.

1. [필수] 불합격 비율이 error_threshold 이상이면 ValueError (경계값 포함)
2. [필수] REQUIRED_COLUMNS 가 하나라도 없으면 ValueError
3. [필수] 출력이 FINAL_SCHEMA 와 컬럼 순서·타입까지 일치
4. 동일 원본은 입력 순서·재실행과 무관하게 같은 trip_key 생성
5. 키 구성 원본값이 다르면 다른 trip_key 생성
6. 완전히 동일한 원본 운행도 서로 다른 trip_key 로 보존되고, 전량 중복 적재만 실패
7. trip_key 는 Parquet 왕복 후에도 null 없는 문자열로 보존
8. trip_miles/trip_time/base_passenger_fare/driver_pay 경계값이 걸러짐
9. hvfhs_license_num 4종이 플랫폼명으로, 미지값은 Unknown으로 매핑
10. 플랫폼·OD별 관측 20건 이상이고 중앙값의 115% 이상인 운임에 추정 서비스 등급 부여
11. zone 조인에 실패한 행이 null로 남고 사라지지 않음 (left join)
12. 문자열 타입 불일치 행 격리 및 사유별 카운트 로그
"""

from datetime import datetime

import pytest

from shared.spark.common.session import get_or_create_spark_session
from main.spark.jobs.bronze_to_silver.hvfhv.transformer import (
    FINAL_SCHEMA,
    REQUIRED_COLUMNS,
    HVFHVCleanTransformer,
)


@pytest.fixture(scope="module")
def spark():
    session = get_or_create_spark_session("test_hvfhv_transformer")
    yield session
    session.stop()


def _row(**overrides) -> dict:
    row = {
        "pickup_datetime": datetime(2024, 3, 1, 10, 0, 0),
        "dropoff_datetime": datetime(2024, 3, 1, 10, 20, 0),
        "PULocationID": 1,
        "DOLocationID": 2,
        "trip_miles": 5.0,
        "trip_time": 600,
        "base_passenger_fare": 10.0,
        "driver_pay": 20.0,
        "hvfhs_license_num": "HV0003",
        "taxi_id": "taxi-1",
    }
    row.update(overrides)
    return row


def test_불합격_비율이_error_threshold와_정확히_같으면_ValueError(spark):
    rows = [_row(), _row(), _row(trip_miles=0.0), _row(trip_miles=0.0)]
    df = spark.createDataFrame(rows)
    transformer = HVFHVCleanTransformer(error_threshold=0.5)

    with pytest.raises(ValueError):
        transformer.transform(df)


def test_불합격_비율이_error_threshold보다_낮으면_통과한다(spark):
    rows = [_row(trip_miles=1.0), _row(trip_miles=2.0), _row(trip_miles=0.0)]
    df = spark.createDataFrame(rows)
    transformer = HVFHVCleanTransformer(error_threshold=0.5)

    result = transformer.transform(df)

    assert result.count() == 2


def test_타입불일치_행은_격리하고_사유별_건수를_로그로_남긴다(
    spark, caplog
):
    rows = [
        _row(trip_miles="5.0"),
        _row(trip_miles="not-a-number"),
        _row(trip_miles="-1"),
        _row(trip_miles=None),
    ]
    df = spark.createDataFrame(rows)

    with caplog.at_level("WARNING"):
        result = HVFHVCleanTransformer(error_threshold=1.0).transform(df)

    assert [row["trip_miles"] for row in result.collect()] == [5.0]
    assert "타입 불일치=1 범위 이탈=1 NULL=1" in caplog.text


@pytest.mark.parametrize("missing_column", REQUIRED_COLUMNS)
def test_required_columns가_하나라도_없으면_ValueError(spark, missing_column):
    row = _row()
    del row[missing_column]
    df = spark.createDataFrame([row])
    transformer = HVFHVCleanTransformer(error_threshold=1.0)

    with pytest.raises(ValueError):
        transformer.transform(df)


def test_출력_스키마가_FINAL_SCHEMA와_순서_타입까지_일치한다(spark):
    df = spark.createDataFrame([_row()])
    transformer = HVFHVCleanTransformer(error_threshold=1.0)

    result = transformer.transform(df)

    assert [field.name for field in result.schema] == [field.name for field in FINAL_SCHEMA]
    assert [field.dataType for field in result.schema] == [field.dataType for field in FINAL_SCHEMA]


def test_원천_taxi_id를_Silver에_그대로_보존한다(spark):
    result = HVFHVCleanTransformer(error_threshold=1.0).transform(
        spark.createDataFrame([_row(taxi_id="taxi-42")])
    )

    assert result.select("taxi_id").first()["taxi_id"] == "taxi-42"


def test_동일_원본은_입력_순서와_재실행에_관계없이_같은_trip_key를_갖는다(spark):
    rows = [_row(trip_miles=1.0), _row(trip_miles=2.0)]
    transformer = HVFHVCleanTransformer(error_threshold=1.0)

    first = transformer.transform(spark.createDataFrame(rows))
    second = transformer.transform(spark.createDataFrame(list(reversed(rows))))

    first_keys = {row["trip_miles"]: row["trip_key"] for row in first.collect()}
    second_keys = {row["trip_miles"]: row["trip_key"] for row in second.collect()}
    assert first_keys == second_keys
    assert all(first_keys.values())


@pytest.mark.parametrize(
    ("field", "changed_value"),
    [
        ("pickup_datetime", datetime(2024, 3, 1, 10, 1, 0)),
        ("PULocationID", 3),
        ("trip_miles", 6.0),
        ("trip_time", 601),
        ("base_passenger_fare", 11.0),
        ("hvfhs_license_num", "HV0005"),
    ],
)
def test_키_구성_원본값이_달라지면_trip_key도_달라진다(spark, field, changed_value):
    rows = [_row(), _row(**{field: changed_value})]

    result = HVFHVCleanTransformer(error_threshold=1.0).transform(spark.createDataFrame(rows))

    keys = [row["trip_key"] for row in result.select("trip_key").collect()]
    assert len(set(keys)) == 2


def test_완전히_동일한_원본_운행도_서로_다른_trip_key로_보존된다(spark):
    """실데이터(2024-01, 19,663,930행)에 9컬럼이 완전히 같은 별개 운행이 2쌍 있다.
    실제로 다른 운행이므로 지워서도, 실패해서도 안 된다."""
    rows = [_row(), _row()] + [_row(trip_miles=float(i)) for i in range(2, 100)]

    result = HVFHVCleanTransformer(error_threshold=1.0).transform(spark.createDataFrame(rows))

    keys = [row["trip_key"] for row in result.select("trip_key").collect()]
    assert len(keys) == len(rows)
    assert len(set(keys)) == len(rows)


def test_같은_달을_통째로_중복_적재하면_ValueError(spark):
    rows = [_row(trip_miles=float(i)) for i in range(1, 51)]

    with pytest.raises(ValueError, match="자연키 충돌"):
        HVFHVCleanTransformer(error_threshold=1.0).transform(spark.createDataFrame(rows + rows))


def test_동일_원본_운행이_중복돼도_재실행하면_같은_trip_key_집합이_나온다(spark):
    rows = [_row(), _row()] + [_row(trip_miles=float(i)) for i in range(2, 100)]
    transformer = HVFHVCleanTransformer(error_threshold=1.0)

    first = transformer.transform(spark.createDataFrame(rows))
    second = transformer.transform(spark.createDataFrame(list(reversed(rows))))

    assert {row["trip_key"] for row in first.collect()} == {row["trip_key"] for row in second.collect()}


def test_trip_key는_Parquet_왕복_후에도_null_없는_문자열로_보존된다(spark, tmp_path):
    result = HVFHVCleanTransformer(error_threshold=1.0).transform(
        spark.createDataFrame([_row(trip_miles=1.0), _row(trip_miles=2.0)])
    )
    output_path = tmp_path / "hvfhv"

    result.write.mode("overwrite").parquet(str(output_path))
    restored = spark.read.parquet(str(output_path))

    assert restored.schema["trip_key"].dataType.simpleString() == "string"
    assert restored.filter(restored.trip_key.isNull()).count() == 0
    assert restored.select("trip_key").distinct().count() == restored.count()


def test_trip_miles_경계값이_걸러진다(spark):
    rows = [
        _row(trip_miles=0.0),
        _row(trip_miles=1.0),
        _row(trip_miles=1000.0),
        _row(trip_miles=1001.0),
    ]
    df = spark.createDataFrame(rows)
    transformer = HVFHVCleanTransformer(error_threshold=1.0)

    result = transformer.transform(df)

    assert {row["trip_miles"] for row in result.collect()} == {1.0, 1000.0}


def test_trip_time_경계값이_걸러진다(spark):
    rows = [
        _row(trip_time=0),
        _row(trip_time=1),
        _row(trip_time=86400),
        _row(trip_time=86401),
    ]
    df = spark.createDataFrame(rows)
    transformer = HVFHVCleanTransformer(error_threshold=1.0)

    result = transformer.transform(df)

    assert {row["trip_time"] for row in result.collect()} == {1, 86400}


def test_driver_pay_경계값이_걸러진다(spark):
    rows = [
        _row(driver_pay=-1.0),
        _row(driver_pay=0.0),
        _row(driver_pay=5000.0),
        _row(driver_pay=5001.0),
    ]
    df = spark.createDataFrame(rows)
    transformer = HVFHVCleanTransformer(error_threshold=1.0)

    result = transformer.transform(df)

    assert {row["driver_pay"] for row in result.collect()} == {0.0, 5000.0}


def test_기본운임_null_음수와_하차구역_null이_걸러진다(spark):
    rows = [
        _row(trip_miles=1.0, base_passenger_fare=None),
        _row(trip_miles=2.0, base_passenger_fare=-1.0),
        _row(trip_miles=3.0, DOLocationID=None),
        _row(trip_miles=4.0, base_passenger_fare=0.0),
    ]
    result = HVFHVCleanTransformer(error_threshold=1.0).transform(spark.createDataFrame(rows))

    assert [row["trip_miles"] for row in result.collect()] == [4.0]


def test_hvfhs_license_num이_플랫폼명으로_매핑되고_미지값은_Unknown이다(spark):
    rows = [
        _row(trip_miles=1.0, hvfhs_license_num="HV0002"),
        _row(trip_miles=2.0, hvfhs_license_num="HV0003"),
        _row(trip_miles=3.0, hvfhs_license_num="HV0004"),
        _row(trip_miles=4.0, hvfhs_license_num="HV0005"),
        _row(trip_miles=5.0, hvfhs_license_num="HV9999"),
    ]
    df = spark.createDataFrame(rows)
    transformer = HVFHVCleanTransformer(error_threshold=1.0)

    result = transformer.transform(df)

    mapping = {row["trip_miles"]: row["platform_name"] for row in result.collect()}
    assert mapping == {1.0: "Juno", 2.0: "Uber", 3.0: "Via", 4.0: "Lyft", 5.0: "Unknown"}


def test_중앙값의_정확히_115퍼센트인_Uber_운임은_Comfort다(spark):
    rows = [_row(trip_miles=float(i), base_passenger_fare=10.0) for i in range(1, 20)]
    rows.append(_row(trip_miles=20.0, base_passenger_fare=11.5))
    result = HVFHVCleanTransformer(error_threshold=1.0).transform(spark.createDataFrame(rows))

    by_miles = {row["trip_miles"]: row["estimated_service_tier"] for row in result.collect()}
    assert by_miles[20.0] == "Comfort"
    assert by_miles[1.0] == "Standard"


def test_OD_관측이_19건이면_고액_Uber_운임도_Standard다(spark):
    rows = [_row(trip_miles=float(i), base_passenger_fare=10.0) for i in range(1, 19)]
    rows.append(_row(trip_miles=19.0, base_passenger_fare=100.0))
    result = HVFHVCleanTransformer(error_threshold=1.0).transform(spark.createDataFrame(rows))

    by_miles = {row["trip_miles"]: row["estimated_service_tier"] for row in result.collect()}
    assert by_miles[19.0] == "Standard"


def test_평균만_115퍼센트_이상이고_중앙값_기준_미달이면_Standard다(spark):
    rows = [
        _row(trip_miles=float(i), base_passenger_fare=0.0 if i <= 9 else 10.0)
        for i in range(1, 20)
    ]
    rows.append(_row(trip_miles=20.0, base_passenger_fare=9.0))
    result = HVFHVCleanTransformer(error_threshold=1.0).transform(spark.createDataFrame(rows))

    by_miles = {row["trip_miles"]: row["estimated_service_tier"] for row in result.collect()}
    assert by_miles[20.0] == "Standard"


def test_플랫폼과_OD가_다르면_운임_통계가_섞이지_않는다(spark):
    rows = [
        _row(trip_miles=float(i), base_passenger_fare=10.0, PULocationID=1, DOLocationID=2)
        for i in range(1, 20)
    ]
    rows.extend([
        _row(trip_miles=20.0, base_passenger_fare=11.5, PULocationID=1, DOLocationID=2),
        _row(trip_miles=21.0, base_passenger_fare=100.0, PULocationID=1, DOLocationID=3),
        _row(
            trip_miles=22.0,
            base_passenger_fare=100.0,
            PULocationID=1,
            DOLocationID=2,
            hvfhs_license_num="HV0005",
        ),
    ])
    result = HVFHVCleanTransformer(error_threshold=1.0).transform(spark.createDataFrame(rows))

    by_miles = {row["trip_miles"]: row["estimated_service_tier"] for row in result.collect()}
    assert by_miles[20.0] == "Comfort"
    assert by_miles[21.0] == "Standard"
    assert by_miles[22.0] == "Standard"


def test_zone_조인에_실패한_행은_null로_남고_사라지지_않는다(spark):
    df_zone = spark.createDataFrame([
        {"LocationID": "1", "Borough": "Manhattan", "Zone": "Central Park", "service_zone": "Yellow Zone"},
    ])
    rows = [
        _row(trip_miles=1.0, PULocationID=1, DOLocationID=1),
        _row(trip_miles=2.0, PULocationID=99, DOLocationID=99),
    ]
    df = spark.createDataFrame(rows)
    transformer = HVFHVCleanTransformer(df_zone=df_zone, error_threshold=1.0)

    result = transformer.transform(df)

    assert result.count() == 2
    by_miles = {row["trip_miles"]: row for row in result.collect()}
    assert by_miles[1.0]["pickup_borough"] == "Manhattan"
    assert by_miles[1.0]["dropoff_borough"] == "Manhattan"
    assert by_miles[2.0]["pickup_borough"] is None
    assert by_miles[2.0]["dropoff_borough"] is None


# --- 시간 순서 계약 -------------------------------------------------------
#
# TLC 원본에 승차보다 하차가 앞선 행이 실제로 들어옵니다. 2026-06 에서 2천만 건 중
# 1건이었고, `trip_time` 이 양수라 다른 검사를 전부 통과해 Silver 까지 올라왔습니다.
# 그 한 행이 기사 배정의 계약 검증에서 전체 job 을 죽입니다.


def test_하차가_승차보다_앞선_행은_Silver_로_넘기지_않는다(spark):
    """`trip_time` 이 양수여도 시각이 모순이면 버려야 합니다.

    실제 원본과 같은 모양으로 둡니다 — pickup 23:17:23 / dropoff 23:15:19 / trip_time 97.
    """
    rows = [
        _row(trip_miles=1.0),
        _row(trip_miles=2.0),
        _row(
            trip_miles=3.0,
            pickup_datetime=datetime(2024, 3, 1, 23, 17, 23),
            dropoff_datetime=datetime(2024, 3, 1, 23, 15, 19),
            trip_time=97,
        ),
    ]

    result = HVFHVCleanTransformer(error_threshold=0.5).transform(
        spark.createDataFrame(rows)
    )

    assert result.count() == 2
    assert all(
        row["pickup_datetime"] < row["dropoff_datetime"] for row in result.collect()
    )


def test_승하차_시각이_같은_행도_버린다(spark):
    """0초짜리 운행은 배정에서 시간 축을 못 만듭니다."""
    same = datetime(2024, 3, 1, 10, 0, 0)
    rows = [
        _row(trip_miles=1.0),
        _row(trip_miles=2.0),
        _row(trip_miles=3.0, pickup_datetime=same, dropoff_datetime=same),
    ]

    result = HVFHVCleanTransformer(error_threshold=0.5).transform(
        spark.createDataFrame(rows)
    )

    assert result.count() == 2
