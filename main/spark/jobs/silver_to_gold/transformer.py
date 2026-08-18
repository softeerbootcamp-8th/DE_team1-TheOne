"""HVFHV 기사 배정 Silver → Gold 3종 집계 로직.

기사 1명 x 1개월 단위로 운행 패턴·연료비·순수익(렌탈료 차감 후)을 집계(``DriverMonthlyAggregation``),
전 차종 후보 중 그 순수익이 최대인 1대를 추천(``MonthlyVehicleRecommendation``),
추천 결과를 임계값으로 요약(``MonthlyReport``)한다. 정확한 필드는 ``schema/gold/*.py`` 참조.

``vehicle_master`` 는 실제 보유 차량(taxi_id)이 아니라 (vendor, make_key, model_key)
단위 스펙 카탈로그라 다음 두 가지를 항상 대표값 하나로 접어서 쓴다:

* 연비/전력소비 대표값 = (min + max) / 2 — 트림 범위의 중간값. 대표 트림을 알 방법이
  없어 범위 양끝의 평균을 근사로 쓴다 (combined_mpg 는 전기차도 vehicle_master
  관례상 이미 MPGe 로 채워져 있어 유종과 무관하게 동일 공식이 적용됨).
* 추천 차량 연식 = spec_year_max — 스펙 트림 범위 중 가장 최신 연식. 실제 보유
  차량이 아니라 (make_key, model_key, 연식) 3개로 추천 차량을 식별한다.

Standard 등급 기사에게 Comfort/Extra Comfort 자격 차량을 추천할 때는, 그 zone에서 실제
관측된 Comfort/Extra Comfort 요금이 Standard 대비 몇 배인지(``_zone_tier_multipliers``)를
그 기사의 실제 운행에 곱해 "등급을 올렸다면의 매출"을 가정한다(``_driver_revenue_scenarios``).
"""

from __future__ import annotations

from dataclasses import fields

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from schema.gold.driver_aggregation import DriverMonthlyAggregation
from schema.gold.driver_car_suggestion import MonthlyVehicleRecommendation
from schema.gold.monthly_report import MonthlyReport

TIME_BLOCK_LABELS = [
    "ratio_00_03", "ratio_03_06", "ratio_06_09", "ratio_09_12",
    "ratio_12_15", "ratio_15_18", "ratio_18_21", "ratio_21_24",
]
TOP_ZONE_RANKS = (1, 2, 3)
SERVICE_TIERS = ("Standard", "Comfort", "Extra Comfort")
# tier -> (vehicle_master.platform, vehicle_master.product). driver_assignment/candidates.py 의
# vehicle_eligible 규칙과 동일 — Standard 는 자격 조건 없이 전 차종이 후보.
_TIER_ELIGIBILITY = {
    "Comfort": ("uber", "Comfort"),
    "Extra Comfort": ("lyft", "Extra Comfort"),
}


def _representative_vehicle_spec(vehicle_master: DataFrame) -> DataFrame:
    """(make_key, model_key) 별 대표 차량 스펙 한 행.

    Output
    1. fuel_type: 휘발유/전기/하이브리드
    2. weekly_price_usd: 렌트비(USD)
    3. combined_mpg: 연비(MPG)
    4. combined_kwh_per_100mi: 전기차 kWh/100mi
    5. recommended_model_year: 추천 차량 연식 — 스펙 트림 범위 중 가장 최신 연식.
    """
    vendors = [row["vendor"] for row in vehicle_master.select("vendor").distinct().collect()]
    if len(vendors) > 1:
        raise ValueError(f"차량 마스터에 업체가 둘 이상입니다: {sorted(vendors)}")

    return vehicle_master.groupBy("make_key", "model_key").agg(
        F.first("fuel_type").alias("fuel_type"),
        F.first("weekly_price_usd").alias("weekly_price_usd"),
        ((F.avg("combined_mpg_min") + F.avg("combined_mpg_max")) / 2).alias("combined_mpg"),
        (
            (F.avg("combined_kwh_per_100mi_min") + F.avg("combined_kwh_per_100mi_max")) / 2
        ).alias("combined_kwh_per_100mi"),
        F.max("spec_year_max").alias("recommended_model_year"),
    )


def _cost_per_mile():
    """마일당 연료비. 
    전기차(EV)는 ``ev_price * combined_kwh_per_100mi / 100``
    (GAS/HYBRID/PHEV 등)는 ``gas_price / combined_mpg`` 
    ``fuel_type``/``gas_price``/``ev_price``/``combined_mpg``/``combined_kwh_per_100mi``
    컬럼이 있는 DataFrame 에 그대로 적용하는 Column 식."""
    return F.when(
        F.col("fuel_type") == "EV",
        F.col("ev_price") * F.col("combined_kwh_per_100mi") / 100,
    ).otherwise(F.col("gas_price") / F.col("combined_mpg"))


def _eligible_vehicles(vehicle_master: DataFrame, tier: str) -> DataFrame:
    """``tier`` 자격을 만족하는 차종의 대표 스펙."""
    if tier not in SERVICE_TIERS:
        raise ValueError(f"알 수 없는 estimated_service_tier 입니다: {tier}")
    base = _representative_vehicle_spec(vehicle_master)
    if tier == "Standard":
        return base
    platform, product = _TIER_ELIGIBILITY[tier]
    qualifying_keys = (
        vehicle_master.filter(
            (F.col("platform") == platform)
            & (F.col("product") == product)
            & (F.col("spec_year_max") >= F.col("min_year"))
        )
        .select("make_key", "model_key")
        .distinct()
    )
    return base.join(qualifying_keys, ["make_key", "model_key"], "inner")


def _vehicle_groups(vehicle_master: DataFrame) -> DataFrame:
    """차종별 서비스 등급 자격. Comfort/Extra Comfort 자격 boolean 두 개와, 그걸 모두/하나/전혀
    못 갖췄는지에 따른 vehicle_group(BOTH/SINGLE/STANDARD) — driver_assignment 생성 시 쓴 것과
    동일한 규칙(sub/scripts/synthetic_company_snapshot/snapshot.py::build_vehicle_pool)."""
    all_keys = vehicle_master.select("make_key", "model_key").distinct()
    uber_comfort = _eligible_vehicles(vehicle_master, "Comfort").select("make_key", "model_key")
    lyft_extra_comfort = _eligible_vehicles(vehicle_master, "Extra Comfort").select("make_key", "model_key")
    return (
        all_keys
        .join(uber_comfort.withColumn("_uber", F.lit(1)), ["make_key", "model_key"], "left")
        .join(lyft_extra_comfort.withColumn("_lyft", F.lit(1)), ["make_key", "model_key"], "left")
        .withColumn("uber_comfort_eligible", F.col("_uber") == 1)
        .withColumn("lyft_extra_comfort_eligible", F.col("_lyft") == 1)
        .withColumn(
            "vehicle_group",
            F.when(F.col("uber_comfort_eligible") & F.col("lyft_extra_comfort_eligible"), F.lit("BOTH"))
             .when(F.col("uber_comfort_eligible") | F.col("lyft_extra_comfort_eligible"), F.lit("SINGLE"))
             .otherwise(F.lit("STANDARD")),
        )
        .select("make_key", "model_key", "vehicle_group", "uber_comfort_eligible", "lyft_extra_comfort_eligible")
    )


def _grade_rank(column: str):
    """vehicle_group 문자열을 등급 폭 비교용 정수로: STANDARD < SINGLE < BOTH."""
    return (
        F.when(F.col(column) == "BOTH", F.lit(2))
        .when(F.col(column) == "SINGLE", F.lit(1))
        .otherwise(F.lit(0))
    )


def _lease_days_in_month(year_month: str, days_in_month: int):
    """이번 달과 (lease_started_on, lease_ended_on) 이 겹치는 일수.

    ``lease_started_on``/``lease_ended_on`` 컬럼이 있는 DataFrame 에 그대로 적용하는
    Column 식. lease_ended_on 은 배타적 상한(그 날부터 무효 — driver_assignment/
    silver_job.py 와 동일 규칙)이라 실질 마지막 유효일은 하루 전이다. 주 단위로
    청구되는 렌트료를, 이번 달 실제로 그 lease 가 유효했던 일수만큼만 안분하는 데 쓴다
    — 현재 차량의 실제 렌트료와 후보 차량의 예상 렌트료 모두 이 기준을 같이 써야
    "같은 기간"을 비교하게 된다.
    """
    month_start = F.to_date(F.lit(f"{year_month}-01"))
    month_end = F.date_add(month_start, days_in_month - 1)
    lease_end_inclusive = F.coalesce(F.date_sub(F.col("lease_ended_on"), 1), month_end)
    return (
        F.datediff(
            F.least(lease_end_inclusive, month_end),
            F.greatest(F.col("lease_started_on"), month_start),
        )
        + 1
    )


def _current_vehicle_facts(enriched: DataFrame, vehicle_master: DataFrame) -> DataFrame:
    """기사별 현재 차량의 make/model·연비·렌트비·등급 — 추천 근거 비교의 기준선.

    기사당 이번 달 lease_started_on 이 가장 늦은 (가장 최근) 한 건을 그대로 현재 차량으로 쓴다.
    lease_started_on/lease_ended_on 은 build_driver_monthly_aggregation 이 월 렌트료를 실제 계약 일수로 안분하는 데 쓴다.
    """
    ranked = enriched.withColumn(
        "_rank",
        F.row_number().over(
            Window.partitionBy("driver_id").orderBy(
                F.col("lease_started_on").desc(), F.col("lease_id").asc()
            )
        ),
    )
    current_vehicle = ranked.filter(F.col("_rank") == 1).select(
        "driver_id",
        F.col("taxi_id").alias("current_taxi_id"),
        "make_key", "model_key", "lease_started_on", "lease_ended_on",
    )
    return (
        current_vehicle
        .join(_representative_vehicle_spec(vehicle_master), ["make_key", "model_key"], "left")
        .join(_vehicle_groups(vehicle_master), ["make_key", "model_key"], "left")
    )


def enrich_trips_with_fuel_cost(
    trips: DataFrame, gas_ev_price: DataFrame, vehicle_master: DataFrame
) -> DataFrame:
    """운행 이력에 현재 차량 스펙·그날 유가/전기요금·연료비·순수익을 붙인다.

    마일당 연료비 공식은 ``_cost_per_mile`` 참조.
    """
    current_spec: DataFrame = _representative_vehicle_spec(vehicle_master)
    prices: DataFrame = gas_ev_price.select(
        F.col("date").alias("_price_date"), "gas_price", "ev_price"
    )
    enriched: DataFrame = (
        trips.withColumn("_pickup_date", F.to_date("pickup_datetime"))
        .join(current_spec, ["make_key", "model_key"], "left")
        .join(prices, F.col("_pickup_date") == F.col("_price_date"), "left")
    )
    unmatched: int = enriched.filter(
        F.col("combined_mpg").isNull() | F.col("gas_price").isNull()
    ).limit(1).count()
    if unmatched:
        raise ValueError(
            "vehicle_master 또는 gas_ev_price 에 매칭되지 않는 운행 이력이 있습니다"
        )

    return (
        enriched.withColumn("_cost_per_mile", _cost_per_mile())
        .withColumn("_fuel_cost", F.col("trip_miles") * F.col("_cost_per_mile"))
        .withColumn("_net_profit", F.col("driver_pay") + F.col("tips") - F.col("_fuel_cost"))
    )


def _time_block_ratios(enriched: DataFrame) -> DataFrame:
    blocked = enriched.withColumn("_block", (F.hour("pickup_datetime") / 3).cast("int"))
    counts = blocked.groupBy("driver_id").agg(
        F.count("*").alias("_total"),
        *[
            F.sum(F.when(F.col("_block") == i, 1).otherwise(0)).alias(f"_count_{i}")
            for i in range(8)
        ],
    )
    ratios = [
        (F.col(f"_count_{i}") / F.col("_total")).alias(label)
        for i, label in enumerate(TIME_BLOCK_LABELS)
    ]
    return counts.select("driver_id", *ratios)


def _top_zones(enriched: DataFrame) -> DataFrame:
    """승차 zone(PULocationID) 상위 3개와 비중. 3개 미만이면 나머지는 null."""
    counts = enriched.groupBy("driver_id", "PULocationID").agg(F.count("*").alias("_n"))
    totals = counts.groupBy("driver_id").agg(F.sum("_n").alias("_total"))
    ranked = (
        counts.join(totals, "driver_id")
        .withColumn("_ratio", F.col("_n") / F.col("_total"))
        .withColumn(
            "_rank",
            F.row_number().over(
                Window.partitionBy("driver_id").orderBy(
                    F.col("_n").desc(), F.col("PULocationID").asc()
                )
            ),
        )
    )
    zone_columns = [
        F.max(F.when(F.col("_rank") == rank, F.col("PULocationID"))).alias(f"top{rank}_zone_id")
        for rank in TOP_ZONE_RANKS
    ]
    ratio_columns = [
        F.max(F.when(F.col("_rank") == rank, F.col("_ratio"))).alias(f"top{rank}_zone_ratio")
        for rank in TOP_ZONE_RANKS
    ]
    return ranked.groupBy("driver_id").agg(*zone_columns, *ratio_columns)


def build_driver_monthly_aggregation(
    enriched: DataFrame, vehicle_master: DataFrame, year_month: str, days_in_month: int
) -> DataFrame:
    """기사 1명 x 1개월 운행 패턴·연료비·순수익 집계. ``schema.gold.DriverMonthlyAggregation`` 과 컬럼 순서 일치.

    monthly_rental_fee: 리스가 실제 청구한 렌트료. 
    lease_started_on/lease_ended_on 을 고려해 이번 달 중 실제 계약 일수만큼만 렌트료를 물린다.
    """
    totals = enriched.groupBy("driver_id").agg(
        F.sum("trip_miles").alias("monthly_mileage"),
        F.sum("_fuel_cost").alias("monthly_fuel_cost"),
        F.sum("_net_profit").alias("_gross_net_profit"),
    )
    current_spec = _current_vehicle_facts(enriched, vehicle_master)

    result = (
        totals.join(_time_block_ratios(enriched), "driver_id")
        .join(_top_zones(enriched), "driver_id")
        .join(current_spec, "driver_id")
        .withColumn("year_month", F.lit(year_month))
        .withColumn(
            "monthly_rental_fee",
            F.col("weekly_price_usd") * (_lease_days_in_month(year_month, days_in_month) / F.lit(7.0)),
        )
        .withColumn("monthly_net_profit", F.col("_gross_net_profit") - F.col("monthly_rental_fee"))
        # `_current_vehicle_facts` 가 조인 키로 들고 있는 값을 그대로 싣습니다. taxi_id 만
        # 있으면 사람이 무슨 차인지 알 수 없어 Silver 로 되짚어야 합니다.
        .withColumn("current_make_key", F.col("make_key"))
        .withColumn("current_model_key", F.col("model_key"))
    )
    columns = [f.name for f in fields(DriverMonthlyAggregation)]
    return result.select(*columns)


def _zone_tier_multipliers(enriched: DataFrame) -> DataFrame:
    """(승차 zone, 하차 zone) 조합별 Comfort/Extra Comfort 거리당 요금이 Standard 대비 몇 배인지.

    요금은 승차 zone 하나만으로 안 갈리고 실제 이동 경로(출발~도착)에 따라 갈리는 경우가
    많아 PULocationID 단독이 아니라 (PULocationID, DOLocationID) 쌍으로 파티션한다.
    관측치가 없는 등급은 null(호출부에서 1.0 = 가정 안 함으로 처리)."""
    rates = (
        enriched.withColumn("_rate_per_mile", F.col("driver_pay") / F.col("trip_miles"))
        .groupBy("PULocationID", "DOLocationID", "estimated_service_tier")
        .agg(F.avg("_rate_per_mile").alias("_avg_rate"))
    )
    pivoted = rates.groupBy("PULocationID", "DOLocationID").pivot(
        "estimated_service_tier", list(SERVICE_TIERS)
    ).agg(F.first("_avg_rate"))
    return pivoted.select(
        "PULocationID", "DOLocationID",
        (F.col("Comfort") / F.col("Standard")).alias("comfort_multiplier"),
        (F.col("Extra Comfort") / F.col("Standard")).alias("extra_comfort_multiplier"),
    )


def _driver_revenue_scenarios(enriched: DataFrame) -> DataFrame:
    """기사별 실제 매출과, Comfort/Extra Comfort 자격 차량으로 바꿨다면의 가정 매출 3종
    (Comfort만/Extra Comfort만/둘 다 가능한 차량 기준).

    Standard 등급으로 뛴 운행만, 그 운행의 플랫폼(Uber→Comfort, Lyft→Extra Comfort)에 맞는
    등급 요금 배수(_zone_tier_multipliers)를 곱해 가정한다 — 이미 프리미엄 요금으로 뛴
    운행에 또 곱하면 중복 가산이라 실제 요금을 그대로 둔다.
    """
    multipliers = _zone_tier_multipliers(enriched)
    with_multiplier = (
        enriched.join(multipliers, ["PULocationID", "DOLocationID"], "left")
        .withColumn("_comfort_multiplier", F.coalesce(F.col("comfort_multiplier"), F.lit(1.0)))
        .withColumn("_extra_comfort_multiplier", F.coalesce(F.col("extra_comfort_multiplier"), F.lit(1.0)))
    )
    is_upgradable_uber_trip = (F.col("estimated_service_tier") == "Standard") & (F.col("platform_name") == "Uber")
    is_upgradable_lyft_trip = (F.col("estimated_service_tier") == "Standard") & (F.col("platform_name") == "Lyft")
    pay_if_comfort = F.when(
        is_upgradable_uber_trip, F.col("driver_pay") * F.col("_comfort_multiplier")
    ).otherwise(F.col("driver_pay"))
    pay_if_extra_comfort = F.when(
        is_upgradable_lyft_trip, F.col("driver_pay") * F.col("_extra_comfort_multiplier")
    ).otherwise(F.col("driver_pay"))
    pay_if_both = (
        F.when(is_upgradable_uber_trip, F.col("driver_pay") * F.col("_comfort_multiplier"))
        .when(is_upgradable_lyft_trip, F.col("driver_pay") * F.col("_extra_comfort_multiplier"))
        .otherwise(F.col("driver_pay"))
    )
    return with_multiplier.groupBy("driver_id").agg(
        F.sum(F.col("driver_pay") + F.col("tips")).alias("_revenue_actual"),
        F.sum(pay_if_comfort + F.col("tips")).alias("_revenue_if_comfort"),
        F.sum(pay_if_extra_comfort + F.col("tips")).alias("_revenue_if_extra_comfort"),
        F.sum(pay_if_both + F.col("tips")).alias("_revenue_if_both"),
    )


def build_monthly_vehicle_recommendation(
    enriched: DataFrame,
    vehicle_master: DataFrame,
    driver_aggregation: DataFrame,
    year_month: str,
    days_in_month: int,
) -> DataFrame:
    """기사별 자격 내 후보 차량 중 예상 순수익(렌탈료 차감 후) 최대인 1대 추천.

    ``schema.gold.MonthlyVehicleRecommendation`` 과 컬럼 순서 일치. threshold 는 이 선정에
    쓰지 않는다 — 그 차를 "추천 대상"으로 집계할지는 build_monthly_report 의 몫이고,
    여기는 항상 driver_aggregation 과 1:1 로 기사별 최선 1대를 낸다.

    Comfort/Extra Comfort 자격 차량 후보는, 그 등급 요금을 새로 받을 수 있다는 가정의
    매출(_driver_revenue_scenarios)을 쓴다 — Standard 자격 차량 후보는 실제 매출 그대로.
    단 **현재 차량에 없던 자격일 때만** 그 가정을 쓴다. 이미 가진 자격에 또 곱하면
    차를 안 바꿔도 순수익이 오르는 값이 나온다 (#403).
    """
    current_facts = _current_vehicle_facts(enriched, vehicle_master)
    lease_dates = current_facts.select("driver_id", "lease_started_on", "lease_ended_on")
    # 등급 상승 매출은 **현재 차량에 없던 자격**을 얻을 때만 가정할 수 있다. 이미 그
    # 자격을 가진 기사는 그 자격으로 Standard 운행을 한 것이 관측된 사실이라, 거기에
    # 배수를 곱하면 "안 바꿔도 오른다"는 값이 나온다.
    # `_vehicle_groups` 는 left join 결과라 자격 없음이 false 가 아니라 null 이다.
    current_eligibility = current_facts.select(
        "driver_id",
        F.coalesce(F.col("uber_comfort_eligible"), F.lit(False)).alias("_current_uber_comfort"),
        F.coalesce(F.col("lyft_extra_comfort_eligible"), F.lit(False)).alias("_current_lyft_extra_comfort"),
    )

    daily = enriched.groupBy("driver_id", "_price_date").agg(
        F.sum("trip_miles").alias("_daily_miles"),
        F.first("gas_price").alias("gas_price"),
        F.first("ev_price").alias("ev_price"),
    )
    revenue = _driver_revenue_scenarios(enriched)
    drivers = enriched.select("driver_id").distinct()

    all_cars = _representative_vehicle_spec(vehicle_master).join(
        _vehicle_groups(vehicle_master), ["make_key", "model_key"], "left"
    )
    driver_candidates = drivers.crossJoin(all_cars)
    cost_per_mile = _cost_per_mile()
    # 후보 차량이 **새로** 주는 자격만 센다. 현재 차량이 이미 가진 자격은 실제 매출에
    # 이미 반영돼 있으므로 다시 곱하면 중복 가산이다.
    gains_comfort = F.coalesce(F.col("uber_comfort_eligible"), F.lit(False)) & ~F.col("_current_uber_comfort")
    gains_extra_comfort = (
        F.coalesce(F.col("lyft_extra_comfort_eligible"), F.lit(False)) & ~F.col("_current_lyft_extra_comfort")
    )
    revenue_for_candidate = (
        F.when(gains_comfort & gains_extra_comfort, F.col("_revenue_if_both"))
        .when(gains_comfort, F.col("_revenue_if_comfort"))
        .when(gains_extra_comfort, F.col("_revenue_if_extra_comfort"))
        .otherwise(F.col("_revenue_actual"))
    )

    hypothetical = (
        driver_candidates.join(daily, "driver_id")
        .join(lease_dates, "driver_id")
        .withColumn("_daily_fuel_cost", F.col("_daily_miles") * cost_per_mile)
        .groupBy(
            "driver_id", "make_key", "model_key", "vehicle_group",
            "uber_comfort_eligible", "lyft_extra_comfort_eligible",
            "combined_mpg", "weekly_price_usd", "recommended_model_year",
            "lease_started_on", "lease_ended_on",
        )
        .agg(F.sum("_daily_fuel_cost").alias("expected_monthly_fuel_cost"))
        .join(revenue, "driver_id")
        .join(current_eligibility, "driver_id")
        .withColumn("_revenue_for_candidate", revenue_for_candidate)
        .withColumn(
            # 후보 차량도 현재 차량과 "같은 기간"(이번 달 실제 lease 유효 일수)만 렌트했다고
            # 가정해야 아래 expected_net_profit_increase 비교가 같은 기간 기준이 된다.
            "recommended_monthly_rental_fee",
            F.col("weekly_price_usd") * (_lease_days_in_month(year_month, days_in_month) / F.lit(7.0)),
        )
        .withColumn(
            "expected_monthly_net_profit",
            F.col("_revenue_for_candidate") - F.col("expected_monthly_fuel_cost")
            - F.col("recommended_monthly_rental_fee"),
        )
    )

    tie_break = [F.col("make_key").asc(), F.col("model_key").asc()]
    ranked = hypothetical.withColumn(
        "_rank",
        F.row_number().over(
            Window.partitionBy("driver_id").orderBy(F.col("expected_monthly_net_profit").desc(), *tie_break)
        ),
    )
    best = ranked.filter(F.col("_rank") == 1).drop("_rank")

    current = (
        current_facts
        .withColumn(
            "service_tier",
            F.when(F.col("lyft_extra_comfort_eligible"), F.lit("Extra Comfort"))
            .when(F.col("uber_comfort_eligible"), F.lit("Comfort"))
            .otherwise(F.lit("Standard")),
        )
        .select(
            "driver_id",
            "service_tier",
            F.col("combined_mpg").alias("_current_combined_mpg"),
            F.col("weekly_price_usd").alias("_current_weekly_price_usd"),
            F.col("vehicle_group").alias("_current_vehicle_group"),
        )
    )

    reasons = [
        F.when(F.col("combined_mpg") > F.col("_current_combined_mpg"), F.lit("연비")),
        F.when(_grade_rank("vehicle_group") > _grade_rank("_current_vehicle_group"), F.lit("차량등급")),
        F.when(F.col("weekly_price_usd") < F.col("_current_weekly_price_usd"), F.lit("더 저렴한 렌트료")),
    ]

    result = (
        best.join(current, "driver_id")
        .join(
            driver_aggregation.select(
                "driver_id",
                F.col("monthly_net_profit").alias("_current_net_profit"),
                F.col("monthly_rental_fee").alias("_current_rental_fee"),
            ),
            "driver_id",
        )
        .withColumn(
            "expected_net_profit_increase",
            F.col("expected_monthly_net_profit") - F.col("_current_net_profit"),
        )
        .withColumn(
            "expected_revenue_increase",
            F.col("recommended_monthly_rental_fee") - F.col("_current_rental_fee"),
        )
        .withColumn("_reason", F.concat_ws(", ", *reasons))
        .withColumn(
            "recommendation_reason",
            F.when(F.col("_reason") == "", F.lit("현재 차량 유지")).otherwise(F.col("_reason")),
        )
        .withColumn("year_month", F.lit(year_month))
        .withColumnRenamed("make_key", "recommended_make_key")
        .withColumnRenamed("model_key", "recommended_model_key")
    )
    columns = [f.name for f in fields(MonthlyVehicleRecommendation)]
    return result.select(*columns)


def build_monthly_report(
    recommendation: DataFrame,
    year_month: str,
    threshold_profit_increase: float,
    *,
    vehicle_master_collected_date: str,
    gas_ev_price_month: str,
) -> DataFrame:
    """추천 결과를 임계값으로 요약한 1행. ``schema.gold.MonthlyReport`` 와 컬럼 순서 일치.

    expected_revenue_increase(매출 증가액)가 음수인 추천은 집계에서 뺀다 — 회사 입장에서
    렌탈료 매출이 오히려 줄어드는 추천을 "성과"로 세면 안 되기 때문.

    계보 두 값은 호출부가 넘긴다 — 입력 경로에 있어서 이 함수가 받는 `recommendation`
    만으로는 알 수 없다.
    """
    recommended = recommendation.filter(
        (F.col("expected_net_profit_increase") >= F.lit(threshold_profit_increase))
        & (F.col("expected_revenue_increase") >= 0)
    )
    summary = recommended.agg(
        F.count("*").alias("recommended_driver_count"),
        F.coalesce(F.avg("expected_net_profit_increase"), F.lit(0.0)).alias(
            "avg_net_profit_increase_per_driver"
        ),
        F.coalesce(F.avg("expected_revenue_increase"), F.lit(0.0)).alias(
            "avg_revenue_increase_per_driver"
        ),
        F.coalesce(F.sum("expected_revenue_increase"), F.lit(0.0)).alias("total_revenue_increase"),
    )
    return (
        summary.withColumn("year_month", F.lit(year_month))
        .withColumn("threshold_profit_increase", F.lit(threshold_profit_increase))
        .withColumn("vehicle_master_collected_date", F.lit(vehicle_master_collected_date))
        .withColumn("gas_ev_price_month", F.lit(gas_ev_price_month))
        .select(*[f.name for f in fields(MonthlyReport)])
    )
