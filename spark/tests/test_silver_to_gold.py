"""HVFHV Silver → Gold 3종 산출 시나리오. 이슈 #367.

1. 정상 집계: GAS 차량 기사 monthly_net_profit = Σ(driver_pay+tip) - Σ(fuel_cost) - rental_fee
2. 연료 단가는 유종에 따라 달라짐(GAS: gas_price/mpg, EV: ev_price*kwh/100)
3. vehicle_master에 vendor 2개 이상이면 ValueError
4. Comfort 자격 차량은 이번 달 그 등급을 서비스한 적 없는 기사에게도 후보로 들어감
   (등급 구분 없이 전 차종이 누구에게나 후보)
5. recommendation_reason: 연비/렌트료가 개선되면 함께 표기, 등급이 같으면 "차량등급"은 안 붙음
6. recommendation_reason: 추천 차량이 현재 차량과 동일하면 "현재 차량 유지"
6-1. 이미 가진 등급 자격에는 상승 매출을 가정하지 않음 — 차를 안 바꾸면 증가액 0 (#403)
6-2. 현재 차량도 make/model 로 나옴 — taxi_id 만으로는 무슨 차인지 알 수 없음 (#415)
7. zone이 3개 미만인 기사는 top2/top3_zone_id가 None
8. trip이 vehicle_master/gas_ev_price에 매칭 안 되면 ValueError
9. monthly_report: profit_increase 기준을 넘어도 revenue_increase<0이면 recommended_driver_count에서 제외
10. monthly_report: 아무도 기준을 못 넘으면 평균/합계가 null이 아니라 0.0
11. driver_aggregation/driver_car_suggestion 출력 컬럼 순서가 schema/gold dataclass와 정확히 일치
12. Comfort 자격 차량을 고르면 그 zone(승하차 zone 쌍)의 Comfort 요금 배수만큼 올린
    가정 매출로 순수익을 계산(가격·연비가 같아도 그 배수만으로 추천이 갈릴 수 있음)
13. lease가 이번 달 중간에 시작하면 렌트료(현재/후보 차량 모두)를 시작일부터만 계산
14. lease가 이번 달 중간에 끝나면(lease_ended_on은 배타적 상한) 렌트료를 종료 하루 전까지만 계산
16. monthly_report 에 계보(배정 버전·마스터 수집일·연료비 월)가 실림 (#418)
17. 배정 버전이 섞여 있으면 ValueError — 어느 규칙의 결과인지 말할 수 없음 (#418)
15. service_tier는 트립 이력 최빈값이 아니라 현재 차량의 vehicle_master 자격
    (Comfort/Extra Comfort eligible)에서 파생. 둘 다 자격이면 Extra Comfort 우선
18. 월말 늦은 시각 운행도 그 달 가격에 붙음 — 세션 타임존이 머신 설정을 따라가면
    to_date 가 밀려 다음 달로 넘어가고 조인이 깨진다 (#460)
"""

from dataclasses import fields
from datetime import date, datetime

import pytest

from common.session import get_or_create_spark_session
from jobs.silver_to_gold.job import partition_value
from jobs.silver_to_gold.transformer import (
    build_driver_monthly_aggregation,
    build_monthly_report,
    build_monthly_vehicle_recommendation,
    enrich_trips_with_fuel_cost,
)
from schema.gold.driver_aggregation import DriverMonthlyAggregation
from schema.gold.driver_car_suggestion import MonthlyVehicleRecommendation

YEAR_MONTH = "2024-03"
DAYS_IN_MONTH = 7  # 7 로 두면 weekly_price_usd * (7/7) == weekly_price_usd 라 계산이 깔끔해짐


@pytest.fixture(scope="module")
def spark():
    session = get_or_create_spark_session("test_silver_to_gold")
    yield session
    session.stop()


def _vehicle_master(spark, rows):
    """row 는 vendor/make_key/model_key/fuel_type/weekly_price_usd/combined_mpg_min/max/
    combined_kwh_per_100mi_min/max/spec_year_max 는 필수. platform/product/min_year 는
    등급 자격이 없으면 생략(None)해도 됨."""
    # platform/product 는 None 대신 "" — Spark 스키마 추론이 모든 행에서 null인 컬럼의
    # 타입을 못 정해서 (CANNOT_DETERMINE_TYPE) 실패한다. eligibility 필터는 어차피
    # "" 를 실제 platform/product 값과 매칭하지 않으니 결과는 같다.
    defaults = {
        "vendor": "v1", "platform": "", "product": "", "min_year": 0,
        "combined_kwh_per_100mi_min": 0.0, "combined_kwh_per_100mi_max": 0.0,
    }
    return spark.createDataFrame([{**defaults, **row} for row in rows])


def _gas_ev_price(spark, rows):
    return spark.createDataFrame(rows)


def _trip(**overrides) -> dict:
    row = {
        "driver_id": "d1", "taxi_id": "tx1", "make_key": "TOYOTA", "model_key": "COROLLA",
        "pickup_datetime": datetime(2024, 3, 1, 9), "trip_miles": 10.0,
        "driver_pay": 20.0, "tips": 2.0, "PULocationID": 10, "DOLocationID": 20,
        "estimated_service_tier": "Standard", "platform_name": "Uber",
        # lease_started_on 은 YEAR_MONTH(2024-03)보다 앞선 달, lease_ended_on 은 먼 미래로 둬서
        # 이번 달 내내 유효한 lease(=기존 전월 계약 유지) 케이스를 기본값으로 삼는다. 전부 None인
        # 컬럼은 Spark가 타입을 못 정해 CANNOT_DETERMINE_TYPE 로 실패하니 None 대신 sentinel을 쓴다.
        "lease_id": "l1", "lease_started_on": date(2024, 1, 1), "lease_ended_on": date(2099, 1, 1),
    }
    row.update(overrides)
    return row


def test_정상_집계는_렌트료를_차감한_순수익을_낸다(spark):
    vehicle_master = _vehicle_master(spark, [{
        "make_key": "TOYOTA", "model_key": "COROLLA", "fuel_type": "GAS",
        "weekly_price_usd": 20.0, "combined_mpg_min": 28.0, "combined_mpg_max": 32.0,
        "spec_year_max": 2025,
    }])
    trips = spark.createDataFrame([
        _trip(trip_miles=10.0, driver_pay=20.0, tips=2.0),
        _trip(trip_miles=5.0, driver_pay=10.0, tips=1.0),
    ])
    gas_ev_price = _gas_ev_price(spark, [
        {"date": date(2024, 3, 1), "gas_price": 3.0, "ev_price": 0.5},
    ])

    enriched = enrich_trips_with_fuel_cost(trips, gas_ev_price, vehicle_master)
    row = build_driver_monthly_aggregation(enriched, vehicle_master, YEAR_MONTH, DAYS_IN_MONTH).first()

    # fuel_cost = 15mi * (3.0/30mpg) = 1.5, rental_fee = 20.0(7일/7일)
    # net_profit = (20+2+10+1) - 1.5 - 20.0 = 11.5
    assert row.monthly_mileage == pytest.approx(15.0)
    assert row.combined_mpg == pytest.approx(30.0)
    assert row.monthly_fuel_cost == pytest.approx(1.5)
    assert row.monthly_rental_fee == pytest.approx(20.0)
    assert row.monthly_net_profit == pytest.approx(11.5)


@pytest.mark.parametrize(
    "fuel_type, expected_fuel_cost",
    [
        ("GAS", 10.0 * 3.0 / 30.0),  # gas_price / combined_mpg
        ("EV", 10.0 * 0.5 * 30.0 / 100),  # ev_price * combined_kwh_per_100mi / 100
    ],
)
def test_연료_단가는_유종에_따라_다른_공식을_쓴다(spark, fuel_type, expected_fuel_cost):
    vehicle_master = _vehicle_master(spark, [{
        "make_key": "TOYOTA", "model_key": "COROLLA", "fuel_type": fuel_type,
        "weekly_price_usd": 20.0, "combined_mpg_min": 28.0, "combined_mpg_max": 32.0,
        "combined_kwh_per_100mi_min": 28.0, "combined_kwh_per_100mi_max": 32.0,
        "spec_year_max": 2025,
    }])
    trips = spark.createDataFrame([_trip(trip_miles=10.0)])
    gas_ev_price = _gas_ev_price(spark, [
        {"date": date(2024, 3, 1), "gas_price": 3.0, "ev_price": 0.5},
    ])

    row = enrich_trips_with_fuel_cost(trips, gas_ev_price, vehicle_master).first()
    assert row["_fuel_cost"] == pytest.approx(expected_fuel_cost)


def test_vehicle_master에_vendor가_둘이면_ValueError다(spark):
    vehicle_master = _vehicle_master(spark, [
        {"vendor": "v1", "make_key": "TOYOTA", "model_key": "COROLLA", "fuel_type": "GAS",
         "weekly_price_usd": 20.0, "combined_mpg_min": 28.0, "combined_mpg_max": 32.0, "spec_year_max": 2025},
        {"vendor": "v2", "make_key": "TOYOTA", "model_key": "COROLLA", "fuel_type": "GAS",
         "weekly_price_usd": 25.0, "combined_mpg_min": 28.0, "combined_mpg_max": 32.0, "spec_year_max": 2025},
    ])
    trips = spark.createDataFrame([_trip()])
    gas_ev_price = _gas_ev_price(spark, [{"date": date(2024, 3, 1), "gas_price": 3.0, "ev_price": 0.5}])

    with pytest.raises(ValueError):
        enrich_trips_with_fuel_cost(trips, gas_ev_price, vehicle_master)


def _standard_and_comfort_vehicle_master(spark):
    return _vehicle_master(spark, [
        {"make_key": "A", "model_key": "STD", "fuel_type": "GAS", "weekly_price_usd": 200.0,
         "combined_mpg_min": 20.0, "combined_mpg_max": 20.0, "spec_year_max": 2025},
        {"make_key": "B", "model_key": "COMFORT", "fuel_type": "GAS", "weekly_price_usd": 100.0,
         "combined_mpg_min": 20.0, "combined_mpg_max": 20.0, "spec_year_max": 2025,
         "platform": "uber", "product": "Comfort", "min_year": 2000},
    ])


def test_comfort_자격_차량은_서비스한_적_없는_기사에게도_후보다(spark):
    vehicle_master = _standard_and_comfort_vehicle_master(spark)
    trips = spark.createDataFrame([
        _trip(make_key="A", model_key="STD", estimated_service_tier="Standard"),
    ])
    gas_ev_price = _gas_ev_price(spark, [{"date": date(2024, 3, 1), "gas_price": 3.0, "ev_price": 0.5}])

    enriched = enrich_trips_with_fuel_cost(trips, gas_ev_price, vehicle_master)
    driver_aggregation = build_driver_monthly_aggregation(enriched, vehicle_master, YEAR_MONTH, DAYS_IN_MONTH)
    recommendation = build_monthly_vehicle_recommendation(
        enriched, vehicle_master, driver_aggregation, YEAR_MONTH, DAYS_IN_MONTH
    ).first()

    # 이 기사는 이번 달 Comfort를 한 번도 안 서비스했지만, 등급 자격 게이트가 없으니
    # B(COMFORT, mpg는 같고 렌트료가 훨씬 쌈)가 후보에 들어 이긴다.
    assert recommendation.recommended_make_key == "B"


@pytest.mark.parametrize(
    "current_make_key, expected_reason",
    [
        ("W", "연비, 더 저렴한 렌트료"),
        ("B", "현재 차량 유지"),
    ],
)
def test_recommendation_reason은_개선된_항목만_나열한다(spark, current_make_key, expected_reason):
    vehicle_master = _vehicle_master(spark, [
        {"make_key": "B", "model_key": "BETTER", "fuel_type": "GAS", "weekly_price_usd": 100.0,
         "combined_mpg_min": 40.0, "combined_mpg_max": 40.0, "spec_year_max": 2025},
        {"make_key": "W", "model_key": "WORSE", "fuel_type": "GAS", "weekly_price_usd": 200.0,
         "combined_mpg_min": 20.0, "combined_mpg_max": 20.0, "spec_year_max": 2025},
    ])
    model_key = "BETTER" if current_make_key == "B" else "WORSE"
    trips = spark.createDataFrame([_trip(make_key=current_make_key, model_key=model_key)])
    gas_ev_price = _gas_ev_price(spark, [{"date": date(2024, 3, 1), "gas_price": 3.0, "ev_price": 0.5}])

    enriched = enrich_trips_with_fuel_cost(trips, gas_ev_price, vehicle_master)
    driver_aggregation = build_driver_monthly_aggregation(enriched, vehicle_master, YEAR_MONTH, DAYS_IN_MONTH)
    recommendation = build_monthly_vehicle_recommendation(
        enriched, vehicle_master, driver_aggregation, YEAR_MONTH, DAYS_IN_MONTH
    ).first()

    assert recommendation.recommended_make_key == "B"
    assert recommendation.recommendation_reason == expected_reason


def test_이미_가진_등급자격에는_상승매출을_가정하지_않는다(spark):
    """차를 안 바꾸는데 순수익이 오르면 안 됩니다 (#403).

    이미 Comfort 자격 차를 타는 기사는 그 자격으로 Standard 운행을 한 것이 관측된
    사실입니다. 거기에 등급 배수를 다시 곱하면 "현재 차량 유지" 추천에 이득이 붙고,
    그대로 `build_monthly_report` 의 성과 집계에까지 들어갑니다.
    """
    vehicle_master = _vehicle_master(spark, [
        {"make_key": "B", "model_key": "COMFORT", "fuel_type": "GAS", "weekly_price_usd": 100.0,
         "combined_mpg_min": 20.0, "combined_mpg_max": 20.0, "spec_year_max": 2025,
         "platform": "uber", "product": "Comfort", "min_year": 2000},
    ])
    # 같은 zone 쌍에서 Standard($20) 와 Comfort($40) 를 둘 다 뛰어 배수 2.0 이 만들어집니다.
    trips = spark.createDataFrame([
        _trip(make_key="B", model_key="COMFORT", estimated_service_tier="Standard", driver_pay=20.0),
        _trip(make_key="B", model_key="COMFORT", estimated_service_tier="Comfort", driver_pay=40.0),
    ])
    gas_ev_price = _gas_ev_price(spark, [{"date": date(2024, 3, 1), "gas_price": 3.0, "ev_price": 0.5}])

    enriched = enrich_trips_with_fuel_cost(trips, gas_ev_price, vehicle_master)
    driver_aggregation = build_driver_monthly_aggregation(enriched, vehicle_master, YEAR_MONTH, DAYS_IN_MONTH)
    recommendation = build_monthly_vehicle_recommendation(
        enriched, vehicle_master, driver_aggregation, YEAR_MONTH, DAYS_IN_MONTH
    ).first()

    assert recommendation.recommendation_reason == "현재 차량 유지"
    assert recommendation.expected_net_profit_increase == pytest.approx(0.0)


def test_zone이_3개_미만이면_top2_top3는_None이다(spark):
    vehicle_master = _vehicle_master(spark, [{
        "make_key": "TOYOTA", "model_key": "COROLLA", "fuel_type": "GAS",
        "weekly_price_usd": 20.0, "combined_mpg_min": 28.0, "combined_mpg_max": 32.0, "spec_year_max": 2025,
    }])
    trips = spark.createDataFrame([
        _trip(PULocationID=10), _trip(PULocationID=10), _trip(PULocationID=10),
        _trip(PULocationID=20),
    ])
    gas_ev_price = _gas_ev_price(spark, [{"date": date(2024, 3, 1), "gas_price": 3.0, "ev_price": 0.5}])

    enriched = enrich_trips_with_fuel_cost(trips, gas_ev_price, vehicle_master)
    row = build_driver_monthly_aggregation(enriched, vehicle_master, YEAR_MONTH, DAYS_IN_MONTH).first()

    assert (row.top1_zone_id, row.top1_zone_ratio) == (10, pytest.approx(0.75))
    assert (row.top2_zone_id, row.top2_zone_ratio) == (20, pytest.approx(0.25))
    assert row.top3_zone_id is None and row.top3_zone_ratio is None


@pytest.mark.parametrize("violation", ["vehicle", "price"])
def test_매칭_안되는_운행이_있으면_ValueError다(spark, violation):
    vehicle_master = _vehicle_master(spark, [{
        "make_key": "TOYOTA", "model_key": "COROLLA", "fuel_type": "GAS",
        "weekly_price_usd": 20.0, "combined_mpg_min": 28.0, "combined_mpg_max": 32.0, "spec_year_max": 2025,
    }])
    trip = _trip(make_key="HONDA", model_key="CIVIC") if violation == "vehicle" else _trip()
    trips = spark.createDataFrame([trip])
    price_date = date(2024, 3, 2) if violation == "price" else date(2024, 3, 1)
    gas_ev_price = _gas_ev_price(spark, [{"date": price_date, "gas_price": 3.0, "ev_price": 0.5}])

    with pytest.raises(ValueError):
        enrich_trips_with_fuel_cost(trips, gas_ev_price, vehicle_master)


def _int96_trips_parquet(spark, tmp_path, pickup: datetime):
    """운행 1건을 실제 Silver 와 같은 물리 타입(INT96)으로 써서 읽어옵니다.

    타임존 민감성은 INT96 에서만 나타납니다 — `createDataFrame` 은 만들 때와 읽을 때
    같은 타임존을 쓰므로 상쇄되어 재현되지 않고, pyarrow 기본 INT64 도 그렇습니다.
    실제 `hvfhv_driver_trip` Silver 의 `pickup_datetime` 이 INT96 입니다.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    row = _trip(pickup_datetime=pickup)
    columns = {
        key: pa.array([value], pa.timestamp("us")) if key == "pickup_datetime"
        else pa.array([value])
        for key, value in row.items()
    }
    path = str(tmp_path / "trips.parquet")
    pq.write_table(pa.table(columns), path, use_deprecated_int96_timestamps=True)
    return spark.read.parquet(path)


def _corolla_master(spark):
    return _vehicle_master(spark, [{
        "make_key": "TOYOTA", "model_key": "COROLLA", "fuel_type": "GAS",
        "weekly_price_usd": 20.0, "combined_mpg_min": 28.0, "combined_mpg_max": 32.0,
        "spec_year_max": 2025,
    }])


def test_월말_늦은_시각_운행도_그달_가격에_붙는다(spark, tmp_path):
    """세션 타임존이 머신 설정을 따라가면 이 케이스가 조용히 깨집니다.

    `pickup_datetime` 은 뉴욕 현지 벽시계가 INT96 으로 담긴 값입니다. 세션 타임존이
    UTC 가 아니면 `to_date` 결과가 밀려서 그 달 마지막 날 늦은 시각 운행이 다음 달로
    넘어가고, 다음 달 가격은 그 파일에 없으니 조인이 깨집니다. 실측으로 2025-05 운행
    254,848건 중 5,091건(2.0%)이 여기 걸렸습니다.
    """
    trips = _int96_trips_parquet(spark, tmp_path, datetime(2024, 3, 31, 23))
    gas_ev_price = _gas_ev_price(spark, [
        {"date": date(2024, 3, 31), "gas_price": 3.0, "ev_price": 0.5},
    ])

    enriched = enrich_trips_with_fuel_cost(trips, gas_ev_price, _corolla_master(spark))

    assert enriched.first().gas_price == pytest.approx(3.0)


def test_세션_타임존은_머신_설정과_무관하게_UTC다(spark):
    # 머신 TZ 를 따라가면 같은 코드·같은 입력이 개발자마다 다른 결과를 냅니다.
    assert spark.conf.get("spark.sql.session.timeZone") == "UTC"


def test_파이썬과_세션이_같은_타임존을_쓴다(spark):
    """둘이 어긋나면 파이썬으로 만든 데이터가 읽을 때 밀립니다.

    `createDataFrame` 은 파이썬 datetime 변환에 프로세스 TZ 를, `to_date` 는 세션
    타임존을 씁니다. 세션만 고정했을 때 실제로 테스트 5개가 깨졌습니다.
    """
    from datetime import datetime as dt

    from pyspark.sql import functions as F

    naive = dt(2024, 3, 31, 23)
    row = spark.createDataFrame([{"ts": naive}]).select(F.to_date("ts").alias("d")).first()

    assert row.d == naive.date()


def test_타임존이_UTC가_아니면_월말_운행이_깨진다(spark, tmp_path):
    """위 고정이 실제로 무엇을 막는지 증명합니다.

    CI 가 UTC 머신이면 `test_월말_늦은_시각_운행도_그달_가격에_붙는다` 는 고정이
    없어도 통과해버립니다. 그래서 타임존을 일부러 밀어 실패를 재현해 둡니다 — 이게
    안 깨지면 고정이 더는 필요 없다는 뜻이므로 그때 둘 다 지우면 됩니다.
    """
    trips = _int96_trips_parquet(spark, tmp_path, datetime(2024, 3, 31, 23))
    gas_ev_price = _gas_ev_price(spark, [
        {"date": date(2024, 3, 31), "gas_price": 3.0, "ev_price": 0.5},
    ])

    original = spark.conf.get("spark.sql.session.timeZone")
    spark.conf.set("spark.sql.session.timeZone", "Asia/Seoul")
    try:
        with pytest.raises(ValueError, match="매칭되지 않는 운행"):
            enrich_trips_with_fuel_cost(trips, gas_ev_price, _corolla_master(spark))
    finally:
        spark.conf.set("spark.sql.session.timeZone", original)


def test_매출_증가액이_음수면_기준을_넘어도_report_집계에서_빠진다(spark):
    recommendation = spark.createDataFrame([
        {"expected_net_profit_increase": 50.0, "expected_revenue_increase": 10.0},
        {"expected_net_profit_increase": 50.0, "expected_revenue_increase": -5.0},
    ])
    report = build_monthly_report(
        recommendation, YEAR_MONTH, threshold_profit_increase=30.0,
        vehicle_master_collected_date="2024-03-15", gas_ev_price_month=YEAR_MONTH,
    ).first()

    assert report.recommended_driver_count == 1
    assert report.avg_net_profit_increase_per_driver == pytest.approx(50.0)
    assert report.avg_revenue_increase_per_driver == pytest.approx(10.0)
    assert report.total_revenue_increase == pytest.approx(10.0)


def test_아무도_기준을_못넘으면_평균합계는_0이다(spark):
    recommendation = spark.createDataFrame([
        {"expected_net_profit_increase": 1.0, "expected_revenue_increase": 1.0},
    ])
    report = build_monthly_report(
        recommendation, YEAR_MONTH, threshold_profit_increase=999.0,
        vehicle_master_collected_date="2024-03-15", gas_ev_price_month=YEAR_MONTH,
    ).first()

    assert report.recommended_driver_count == 0
    assert report.avg_net_profit_increase_per_driver == 0.0
    assert report.avg_revenue_increase_per_driver == 0.0
    assert report.total_revenue_increase == 0.0


def test_현재_차량은_추천_차량과_별개로_이름이_나온다(spark):
    """`taxi_id` 만으로는 사람이 무슨 차인지 알 수 없습니다 (#415).

    콜 리스트에서 "지금 <현재 차량> 타시는데 <추천 차량> 으로" 를 쓰려면 현재 차량도
    make/model 이어야 합니다. 추천 차량과 값이 갈리는 상황으로 확인합니다.
    """
    vehicle_master = _vehicle_master(spark, [
        {"make_key": "W", "model_key": "WORSE", "fuel_type": "GAS", "weekly_price_usd": 200.0,
         "combined_mpg_min": 20.0, "combined_mpg_max": 20.0, "spec_year_max": 2025},
        {"make_key": "B", "model_key": "BETTER", "fuel_type": "GAS", "weekly_price_usd": 100.0,
         "combined_mpg_min": 40.0, "combined_mpg_max": 40.0, "spec_year_max": 2025},
    ])
    trips = spark.createDataFrame([_trip(make_key="W", model_key="WORSE")])
    gas_ev_price = _gas_ev_price(spark, [{"date": date(2024, 3, 1), "gas_price": 3.0, "ev_price": 0.5}])

    enriched = enrich_trips_with_fuel_cost(trips, gas_ev_price, vehicle_master)
    driver_aggregation = build_driver_monthly_aggregation(
        enriched, vehicle_master, YEAR_MONTH, DAYS_IN_MONTH
    )
    recommendation = build_monthly_vehicle_recommendation(
        enriched, vehicle_master, driver_aggregation, YEAR_MONTH, DAYS_IN_MONTH
    ).first()
    aggregation = driver_aggregation.first()

    assert (aggregation.current_make_key, aggregation.current_model_key) == ("W", "WORSE")
    assert (recommendation.recommended_make_key, recommendation.recommended_model_key) == ("B", "BETTER")


def test_출력_컬럼_순서가_schema_gold_dataclass와_일치한다(spark):
    vehicle_master = _vehicle_master(spark, [{
        "make_key": "TOYOTA", "model_key": "COROLLA", "fuel_type": "GAS",
        "weekly_price_usd": 20.0, "combined_mpg_min": 28.0, "combined_mpg_max": 32.0, "spec_year_max": 2025,
    }])
    trips = spark.createDataFrame([_trip()])
    gas_ev_price = _gas_ev_price(spark, [{"date": date(2024, 3, 1), "gas_price": 3.0, "ev_price": 0.5}])

    enriched = enrich_trips_with_fuel_cost(trips, gas_ev_price, vehicle_master)
    driver_aggregation = build_driver_monthly_aggregation(enriched, vehicle_master, YEAR_MONTH, DAYS_IN_MONTH)
    recommendation = build_monthly_vehicle_recommendation(
        enriched, vehicle_master, driver_aggregation, YEAR_MONTH, DAYS_IN_MONTH
    )

    assert driver_aggregation.columns == [f.name for f in fields(DriverMonthlyAggregation)]
    assert recommendation.columns == [f.name for f in fields(MonthlyVehicleRecommendation)]


def test_comfort_자격_차량은_zone_요금_배수만큼_매출을_가정한다(spark):
    # A(현재 차량)와 Z(Comfort 자격)는 가격·연비가 완전히 같아서, 매출 가정을
    # 안 걸면 이 둘은 동률(사전순으로 A 승) — Z 가 이기면 배수 로직이 실제로 반영된 것.
    vehicle_master = _vehicle_master(spark, [
        {"make_key": "A", "model_key": "STD", "fuel_type": "GAS", "weekly_price_usd": 100.0,
         "combined_mpg_min": 20.0, "combined_mpg_max": 20.0, "spec_year_max": 2025},
        {"make_key": "Z", "model_key": "CMF", "fuel_type": "GAS", "weekly_price_usd": 100.0,
         "combined_mpg_min": 20.0, "combined_mpg_max": 20.0, "spec_year_max": 2025,
         "platform": "uber", "product": "Comfort", "min_year": 2000},
    ])
    trips = spark.createDataFrame([
        # d1: Standard/Uber, zone(10,20) 에서 driver_pay 10 / 5마일 = 시급 2.0/mile
        _trip(driver_id="d1", make_key="A", model_key="STD", driver_pay=10.0, tips=0.0,
              trip_miles=5.0, PULocationID=10, DOLocationID=20,
              estimated_service_tier="Standard", platform_name="Uber"),
        # d2: 같은 zone(10,20) 에서 Comfort 시급 4.0/mile 관측치를 남겨 배수(=2.0)를 만듦
        _trip(driver_id="d2", make_key="Z", model_key="CMF", driver_pay=20.0, tips=0.0,
              trip_miles=5.0, PULocationID=10, DOLocationID=20,
              estimated_service_tier="Comfort", platform_name="Uber"),
    ])
    gas_ev_price = _gas_ev_price(spark, [{"date": date(2024, 3, 1), "gas_price": 3.0, "ev_price": 0.5}])

    enriched = enrich_trips_with_fuel_cost(trips, gas_ev_price, vehicle_master)
    driver_aggregation = build_driver_monthly_aggregation(enriched, vehicle_master, YEAR_MONTH, DAYS_IN_MONTH)
    recommendation = build_monthly_vehicle_recommendation(
        enriched, vehicle_master, driver_aggregation, YEAR_MONTH, DAYS_IN_MONTH
    )
    row = recommendation.filter(recommendation.driver_id == "d1").first()

    # fuel_cost = 5mi * 3.0/20mpg = 0.75, rental_fee = 100.0(7일/7일)
    # 실제 매출 = 10 (A 채택 시), 배수 적용 매출 = 10*multiplier(2.0) = 20 (Z 채택 시)
    # net_profit(A) = 10-0.75-100 = -90.75, net_profit(Z) = 20-0.75-100 = -80.75 로 Z 승
    assert row.recommended_make_key == "Z"
    assert row.expected_monthly_net_profit == pytest.approx(-80.75)


def test_lease가_이번_달_중간에_시작하면_렌트료를_시작일부터만_계산한다(spark):
    vehicle_master = _vehicle_master(spark, [{
        "make_key": "TOYOTA", "model_key": "COROLLA", "fuel_type": "GAS",
        "weekly_price_usd": 70.0, "combined_mpg_min": 28.0, "combined_mpg_max": 32.0, "spec_year_max": 2025,
    }])
    trips = spark.createDataFrame([_trip(lease_started_on=date(2024, 3, 4))])
    gas_ev_price = _gas_ev_price(spark, [{"date": date(2024, 3, 1), "gas_price": 3.0, "ev_price": 0.5}])

    enriched = enrich_trips_with_fuel_cost(trips, gas_ev_price, vehicle_master)
    driver_aggregation = build_driver_monthly_aggregation(enriched, vehicle_master, YEAR_MONTH, DAYS_IN_MONTH)
    recommendation = build_monthly_vehicle_recommendation(
        enriched, vehicle_master, driver_aggregation, YEAR_MONTH, DAYS_IN_MONTH
    ).first()

    # 이번 달(3/1~3/7, DAYS_IN_MONTH=7) 중 lease는 3/4에 시작 -> 3/4~3/7 = 4일치만 렌트료.
    # monthly_rental_fee = recommended_monthly_rental_fee = 70.0 * (4/7) = 40.0
    assert driver_aggregation.first().monthly_rental_fee == pytest.approx(40.0)
    assert recommendation.recommended_monthly_rental_fee == pytest.approx(40.0)


def test_lease가_이번_달_중간에_끝나면_렌트료를_종료_전날까지만_계산한다(spark):
    vehicle_master = _vehicle_master(spark, [{
        "make_key": "TOYOTA", "model_key": "COROLLA", "fuel_type": "GAS",
        "weekly_price_usd": 70.0, "combined_mpg_min": 28.0, "combined_mpg_max": 32.0, "spec_year_max": 2025,
    }])
    trips = spark.createDataFrame([_trip(lease_ended_on=date(2024, 3, 5))])
    gas_ev_price = _gas_ev_price(spark, [{"date": date(2024, 3, 1), "gas_price": 3.0, "ev_price": 0.5}])

    enriched = enrich_trips_with_fuel_cost(trips, gas_ev_price, vehicle_master)
    driver_aggregation = build_driver_monthly_aggregation(enriched, vehicle_master, YEAR_MONTH, DAYS_IN_MONTH)
    recommendation = build_monthly_vehicle_recommendation(
        enriched, vehicle_master, driver_aggregation, YEAR_MONTH, DAYS_IN_MONTH
    ).first()

    # lease_ended_on=3/5 는 배타적 상한(그 날부터 무효)이라 실제 마지막 유효일은 3/4.
    # 3/1~3/4 = 4일치만 렌트료. monthly_rental_fee = recommended_monthly_rental_fee = 70.0 * (4/7) = 40.0
    assert driver_aggregation.first().monthly_rental_fee == pytest.approx(40.0)
    assert recommendation.recommended_monthly_rental_fee == pytest.approx(40.0)


@pytest.mark.parametrize(
    "vehicle_rows, expected_service_tier",
    [
        (
            [{"make_key": "STD", "model_key": "STD", "fuel_type": "GAS", "weekly_price_usd": 100.0,
              "combined_mpg_min": 20.0, "combined_mpg_max": 20.0, "spec_year_max": 2025}],
            "Standard",
        ),
        (
            [{"make_key": "CMF", "model_key": "CMF", "fuel_type": "GAS", "weekly_price_usd": 100.0,
              "combined_mpg_min": 20.0, "combined_mpg_max": 20.0, "spec_year_max": 2025,
              "platform": "uber", "product": "Comfort", "min_year": 2000}],
            "Comfort",
        ),
        (
            [{"make_key": "XCMF", "model_key": "XCMF", "fuel_type": "GAS", "weekly_price_usd": 100.0,
              "combined_mpg_min": 20.0, "combined_mpg_max": 20.0, "spec_year_max": 2025,
              "platform": "lyft", "product": "Extra Comfort", "min_year": 2000}],
            "Extra Comfort",
        ),
        (
            [
                {"make_key": "BOTH", "model_key": "BOTH", "fuel_type": "GAS", "weekly_price_usd": 100.0,
                 "combined_mpg_min": 20.0, "combined_mpg_max": 20.0, "spec_year_max": 2025,
                 "platform": "uber", "product": "Comfort", "min_year": 2000},
                {"make_key": "BOTH", "model_key": "BOTH", "fuel_type": "GAS", "weekly_price_usd": 100.0,
                 "combined_mpg_min": 20.0, "combined_mpg_max": 20.0, "spec_year_max": 2025,
                 "platform": "lyft", "product": "Extra Comfort", "min_year": 2000},
            ],
            "Extra Comfort",
        ),
    ],
    ids=["standard", "comfort_only", "extra_comfort_only", "both_eligible_extra_comfort_우선"],
)
def test_service_tier는_현재_차량의_vehicle_master_자격에서_파생된다(
    spark, vehicle_rows, expected_service_tier
):
    vehicle_master = _vehicle_master(spark, vehicle_rows)
    make_key, model_key = vehicle_rows[0]["make_key"], vehicle_rows[0]["model_key"]
    trips = spark.createDataFrame([_trip(make_key=make_key, model_key=model_key)])
    gas_ev_price = _gas_ev_price(spark, [{"date": date(2024, 3, 1), "gas_price": 3.0, "ev_price": 0.5}])

    enriched = enrich_trips_with_fuel_cost(trips, gas_ev_price, vehicle_master)
    driver_aggregation = build_driver_monthly_aggregation(enriched, vehicle_master, YEAR_MONTH, DAYS_IN_MONTH)
    recommendation = build_monthly_vehicle_recommendation(
        enriched, vehicle_master, driver_aggregation, YEAR_MONTH, DAYS_IN_MONTH
    ).first()

    assert recommendation.service_tier == expected_service_tier


def test_monthly_report에_계보가_실린다(spark):
    """Gold 만 보고 어떤 입력으로 나온 숫자인지 알 수 있어야 합니다 (#418)."""
    vehicle_master = _vehicle_master(spark, [{
        "make_key": "TOYOTA", "model_key": "COROLLA", "fuel_type": "GAS",
        "weekly_price_usd": 20.0, "combined_mpg_min": 30.0, "combined_mpg_max": 30.0,
        "spec_year_max": 2025,
    }])
    trips = spark.createDataFrame([_trip()])
    gas_ev_price = _gas_ev_price(spark, [{"date": date(2024, 3, 1), "gas_price": 3.0, "ev_price": 0.5}])

    enriched = enrich_trips_with_fuel_cost(trips, gas_ev_price, vehicle_master)
    driver_aggregation = build_driver_monthly_aggregation(enriched, vehicle_master, YEAR_MONTH, DAYS_IN_MONTH)
    recommendation = build_monthly_vehicle_recommendation(
        enriched, vehicle_master, driver_aggregation, YEAR_MONTH, DAYS_IN_MONTH
    )

    report = build_monthly_report(
        recommendation, YEAR_MONTH, threshold_profit_increase=30.0,
        # 대상 월(2024-03)과 다른 시점 — 물러서 쓴 경우가 결과에 드러나야 합니다.
        vehicle_master_collected_date="2026-08-15",
        gas_ev_price_month="2026-08",
    ).first()

    assert report.vehicle_master_collected_date == "2026-08-15"
    assert report.gas_ev_price_month == "2026-08"


def test_report_에_배정_버전_컬럼이_남아있지_않다(spark):
    """기사-운행 매칭이 가짜 데이터 API 로 옮겨가 배정 규칙이라는 것이 없어졌습니다.
    Silver 가 안 싣는 값을 Gold 가 계보로 들고 있으면 그 자리가 조용히 비거나 죽습니다."""
    recommendation = spark.createDataFrame([
        {"expected_net_profit_increase": 50.0, "expected_revenue_increase": 10.0},
    ])

    report = build_monthly_report(
        recommendation, YEAR_MONTH, threshold_profit_increase=30.0,
        vehicle_master_collected_date="2024-03-15", gas_ev_price_month=YEAR_MONTH,
    )

    assert "assignment_version" not in report.columns


@pytest.mark.parametrize(
    "path, key, expected",
    [
        ("../data/silver/vehicle_master/collected_date=2026-08-15/city=new-york/vehicle_master.parquet",
         "collected_date", "2026-08-15"),
        ("../data/silver/gas_ev_price/year_month=2025-05/gas_ev_price.parquet",
         "year_month", "2025-05"),
    ],
)
def test_입력_경로에서_계보_값을_읽는다(path, key, expected):
    assert partition_value(path, key) == expected


def test_규칙과_다른_경로면_조용히_빈값을_쓰지_않고_실패한다():
    with pytest.raises(ValueError, match="collected_date= 파티션을 찾지 못했습니다"):
        partition_value("../data/silver/vehicle_master/vehicle_master.parquet", "collected_date")
