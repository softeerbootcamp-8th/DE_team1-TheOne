"""HVFHV Silver → Gold 3종 산출 시나리오. 이슈 #367.

1. 정상 집계: GAS 차량 기사 monthly_net_profit = Σ(driver_pay+tip) - Σ(fuel_cost) - rental_fee
2. 연료 단가는 유종에 따라 달라짐(GAS: gas_price/mpg, EV: ev_price*kwh/100)
3. vehicle_master에 vendor 2개 이상이면 ValueError
4. Comfort 자격 차량은 이번 달 그 등급을 서비스한 적 없는 기사에게도 후보로 들어감
   (등급 구분 없이 전 차종이 누구에게나 후보)
5. recommendation_reason: 연비/렌트료가 개선되면 함께 표기, 등급이 같으면 "차량등급"은 안 붙음
6. recommendation_reason: 추천 차량이 현재 차량과 동일하면 "현재 차량 유지"
7. zone이 3개 미만인 기사는 top2/top3_zone_id가 None
8. trip이 vehicle_master/gas_ev_price에 매칭 안 되면 ValueError
9. monthly_report: profit_increase 기준을 넘어도 revenue_increase<0이면 recommended_driver_count에서 제외
10. monthly_report: 아무도 기준을 못 넘으면 평균/합계가 null이 아니라 0.0
11. driver_aggregation/driver_car_suggestion 출력 컬럼 순서가 schema/gold dataclass와 정확히 일치
12. Comfort 자격 차량을 고르면 그 zone(승하차 zone 쌍)의 Comfort 요금 배수만큼 올린
    가정 매출로 순수익을 계산(가격·연비가 같아도 그 배수만으로 추천이 갈릴 수 있음)
"""

from dataclasses import fields
from datetime import date, datetime

import pytest

from common.session import get_or_create_spark_session
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


def test_매출_증가액이_음수면_기준을_넘어도_report_집계에서_빠진다(spark):
    recommendation = spark.createDataFrame([
        {"expected_net_profit_increase": 50.0, "expected_revenue_increase": 10.0},
        {"expected_net_profit_increase": 50.0, "expected_revenue_increase": -5.0},
    ])
    report = build_monthly_report(recommendation, YEAR_MONTH, threshold_profit_increase=30.0).first()

    assert report.recommended_driver_count == 1
    assert report.avg_net_profit_increase_per_driver == pytest.approx(50.0)
    assert report.avg_revenue_increase_per_driver == pytest.approx(10.0)
    assert report.total_revenue_increase == pytest.approx(10.0)


def test_아무도_기준을_못넘으면_평균합계는_0이다(spark):
    recommendation = spark.createDataFrame([
        {"expected_net_profit_increase": 1.0, "expected_revenue_increase": 1.0},
    ])
    report = build_monthly_report(recommendation, YEAR_MONTH, threshold_profit_increase=999.0).first()

    assert report.recommended_driver_count == 0
    assert report.avg_net_profit_increase_per_driver == 0.0
    assert report.avg_revenue_increase_per_driver == 0.0
    assert report.total_revenue_increase == 0.0


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
