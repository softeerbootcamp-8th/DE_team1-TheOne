"""HVFHV 기사 배정 Silver → Gold 3종 집계 로직.

기사 1명 x 1개월 단위로 운행 패턴·연료비·순수익을 집계(``DriverMonthlyAggregation``),
서비스 등급 자격 내 후보 차량 중 순수익 증가가 최대인 1대를 추천(``MonthlyVehicleRecommendation``),
추천 결과를 임계값으로 요약(``MonthlyReport``)한다. 정확한 필드는 ``schema/gold/*.py`` 참조.

``vehicle_master`` 는 실제 보유 차량(taxi_id)이 아니라 (vendor, make_key, model_key)
단위 스펙 카탈로그라 다음 두 가지를 항상 대표값 하나로 접어서 쓴다:

* 연비/전력소비 대표값 = (min + max) / 2 — 트림 범위의 중간값. 대표 트림을 알 방법이
  없어 범위 양끝의 평균을 근사로 쓴다 (combined_mpg 는 전기차도 vehicle_master
  관례상 이미 MPGe 로 채워져 있어 유종과 무관하게 동일 공식이 적용됨).
* 추천 차량 연식 = spec_year_max — 스펙 트림 범위 중 가장 최신 연식. 실제 보유
  차량이 아니라 (make_key, model_key, 연식) 3개로 추천 차량을 식별한다.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

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

    weekly_price_usd/fuel_type 은 같은 (vendor, make_key, model_key) 안에서 상수라
    first() 로 충분하다. vendor 가 둘 이상이면 같은 차종이라도 업체별로 리스비가
    갈릴 수 있어 first() 가 실행마다 다른 값을 조용히 고를 수 있다 — 그 전에 막는다
    (scripts/synthetic_company_snapshot/snapshot.py::build_vehicle_pool 과 동일한 가드).
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


def enrich_trips_with_fuel_cost(
    trips: DataFrame, gas_ev_price: DataFrame, vehicle_master: DataFrame
) -> DataFrame:
    """운행 이력에 현재 차량 스펙·그날 유가/전기요금·연료비·순수익을 붙인다.

    연료/충전 단가: 유종차는 그날 gas_price / combined_mpg, 전기차는 그날
    ev_price * combined_kwh_per_100mi / 100. HYBRID/PHEV/MIXED 도 combined_mpg 가
    이미 해당 유종의 종합 연비라 유종차와 같은 공식을 쓴다(EV 만 충전 경로 분기).
    """
    current_spec = _representative_vehicle_spec(vehicle_master)
    prices = gas_ev_price.select(
        F.col("date").alias("_price_date"), "gas_price", "ev_price"
    )
    enriched = (
        trips.withColumn("_pickup_date", F.to_date("pickup_datetime"))
        .join(current_spec, ["make_key", "model_key"], "left")
        .join(prices, F.col("_pickup_date") == F.col("_price_date"), "left")
    )
    unmatched = enriched.filter(
        F.col("combined_mpg").isNull() | F.col("gas_price").isNull()
    ).limit(1).count()
    if unmatched:
        raise ValueError(
            "vehicle_master 또는 gas_ev_price 에 매칭되지 않는 운행 이력이 있습니다"
        )

    cost_per_mile = F.when(
        F.col("fuel_type") == "EV",
        F.col("ev_price") * F.col("combined_kwh_per_100mi") / 100,
    ).otherwise(F.col("gas_price") / F.col("combined_mpg"))

    return (
        enriched.withColumn("_cost_per_mile", cost_per_mile)
        .withColumn("_fuel_cost", F.col("trip_miles") * F.col("_cost_per_mile"))
        .withColumn("_net_profit", F.col("driver_pay") + F.col("tips") - F.col("_fuel_cost"))
    )


def _modal(enriched: DataFrame, *group_cols: str) -> DataFrame:
    """기사(driver_id)별 ``group_cols`` 조합의 최빈값 한 행. 동률이면 사전순으로 고정."""
    counts = enriched.groupBy("driver_id", *group_cols).agg(F.count("*").alias("_n"))
    ranked = counts.withColumn(
        "_rank",
        F.row_number().over(
            Window.partitionBy("driver_id").orderBy(
                F.col("_n").desc(), *[F.col(c).asc() for c in group_cols]
            )
        ),
    )
    return ranked.filter(F.col("_rank") == 1).select("driver_id", *group_cols)


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
    """기사 1명 x 1개월 운행 패턴·연료비·순수익 집계. ``schema.gold.DriverMonthlyAggregation`` 과 컬럼 순서 일치."""
    totals = enriched.groupBy("driver_id").agg(
        F.sum("trip_miles").alias("monthly_mileage"),
        F.sum("_fuel_cost").alias("monthly_fuel_cost"),
        F.sum("_net_profit").alias("monthly_net_profit"),
    )
    current_vehicle = _modal(enriched, "taxi_id", "make_key", "model_key").withColumnRenamed(
        "taxi_id", "current_taxi_id"
    )
    current_spec = current_vehicle.join(
        _representative_vehicle_spec(vehicle_master), ["make_key", "model_key"], "left"
    )

    result = (
        totals.join(_time_block_ratios(enriched), "driver_id")
        .join(_top_zones(enriched), "driver_id")
        .join(current_spec, "driver_id")
        .withColumn("year_month", F.lit(year_month))
        .withColumn("monthly_rental_fee", F.col("weekly_price_usd") * (F.lit(days_in_month) / 7.0))
    )
    # top*_zone_id/top*_zone_ratio 를 (id, ratio) 순서로 인터리브
    zone_cols = []
    for rank in TOP_ZONE_RANKS:
        zone_cols += [f"top{rank}_zone_id", f"top{rank}_zone_ratio"]
    columns = [
        "driver_id", "year_month",
        *TIME_BLOCK_LABELS,
        *zone_cols,
        "current_taxi_id", "combined_mpg", "monthly_mileage", "monthly_fuel_cost",
        "monthly_rental_fee", "monthly_net_profit",
    ]
    return result.select(*columns)


def build_monthly_vehicle_recommendation(
    enriched: DataFrame,
    vehicle_master: DataFrame,
    driver_aggregation: DataFrame,
    year_month: str,
    days_in_month: int,
    threshold_profit_increase: float,
) -> DataFrame:
    """기사별 자격 내 후보 차량 중 1대 추천. ``schema.gold.MonthlyVehicleRecommendation`` 과 컬럼 순서 일치.

    선정 기준: expected_net_profit_increase >= threshold_profit_increase 인 후보 중
    expected_revenue_increase(객단가 증가액) 최대인 1대. 그런 후보가 하나도 없는
    기사는(threshold 를 못 넘는 경우) expected_monthly_net_profit 최대인 후보로 대체 —
    driver_aggregation 과 1:1 이라 후보가 있는데도 행을 비울 수 없음.
    """
    service_tier = _modal(enriched, "estimated_service_tier").withColumnRenamed(
        "estimated_service_tier", "service_tier"
    )
    daily = enriched.groupBy("driver_id", "_price_date").agg(
        F.sum("trip_miles").alias("_daily_miles"),
        F.first("gas_price").alias("gas_price"),
        F.first("ev_price").alias("ev_price"),
    )
    revenue_total = enriched.groupBy("driver_id").agg(
        F.sum(F.col("driver_pay") + F.col("tips")).alias("_revenue_total")
    )

    candidates = None
    for tier in SERVICE_TIERS:
        tagged = _eligible_vehicles(vehicle_master, tier).withColumn("service_tier", F.lit(tier))
        candidates = tagged if candidates is None else candidates.unionByName(tagged)

    driver_candidates = service_tier.join(candidates, "service_tier")
    cost_per_mile = F.when(
        F.col("fuel_type") == "EV",
        F.col("ev_price") * F.col("combined_kwh_per_100mi") / 100,
    ).otherwise(F.col("gas_price") / F.col("combined_mpg"))

    hypothetical = (
        driver_candidates.join(daily, "driver_id")
        .withColumn("_daily_fuel_cost", F.col("_daily_miles") * cost_per_mile)
        .groupBy(
            "driver_id", "service_tier", "make_key", "model_key",
            "combined_mpg", "weekly_price_usd", "recommended_model_year",
        )
        .agg(F.sum("_daily_fuel_cost").alias("expected_monthly_fuel_cost"))
        .join(revenue_total, "driver_id")
        .withColumn(
            "expected_monthly_net_profit",
            F.col("_revenue_total") - F.col("expected_monthly_fuel_cost"),
        )
        .withColumn(
            "recommended_monthly_rental_fee",
            F.col("weekly_price_usd") * (F.lit(days_in_month) / 7.0),
        )
    )

    with_increase = hypothetical.join(
        driver_aggregation.select(
            "driver_id",
            F.col("monthly_net_profit").alias("_current_net_profit"),
            F.col("monthly_rental_fee").alias("_current_rental_fee"),
        ),
        "driver_id",
    ).withColumn(
        "expected_net_profit_increase",
        F.col("expected_monthly_net_profit") - F.col("_current_net_profit"),
    ).withColumn(
        "expected_revenue_increase",
        F.col("recommended_monthly_rental_fee") - F.col("_current_rental_fee"),
    )

    tie_break = [F.col("make_key").asc(), F.col("model_key").asc()]
    qualifying_rank = F.row_number().over(
        Window.partitionBy("driver_id").orderBy(F.col("expected_revenue_increase").desc(), *tie_break)
    )
    primary = (
        with_increase.filter(F.col("expected_net_profit_increase") >= F.lit(threshold_profit_increase))
        .withColumn("_rank", qualifying_rank)
        .filter(F.col("_rank") == 1)
    )

    fallback_rank = F.row_number().over(
        Window.partitionBy("driver_id").orderBy(F.col("expected_monthly_net_profit").desc(), *tie_break)
    )
    fallback_only = (
        with_increase.withColumn("_rank", fallback_rank)
        .filter(F.col("_rank") == 1)
        .join(primary.select("driver_id"), "driver_id", "left_anti")
    )

    result = (
        primary.unionByName(fallback_only)
        .drop("_rank")
        .withColumn("year_month", F.lit(year_month))
        .withColumnRenamed("make_key", "recommended_make_key")
        .withColumnRenamed("model_key", "recommended_model_key")
    )
    columns = [
        "driver_id", "year_month", "service_tier",
        "recommended_make_key", "recommended_model_key", "recommended_model_year",
        "combined_mpg", "recommended_monthly_rental_fee", "expected_monthly_fuel_cost",
        "expected_monthly_net_profit", "expected_net_profit_increase", "expected_revenue_increase",
    ]
    return result.select(*columns)


def build_monthly_report(
    recommendation: DataFrame, year_month: str, threshold_profit_increase: float
) -> DataFrame:
    """추천 결과를 임계값으로 요약한 1행. ``schema.gold.MonthlyReport`` 와 컬럼 순서 일치."""
    recommended = recommendation.filter(
        F.col("expected_net_profit_increase") >= F.lit(threshold_profit_increase)
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
        .select(
            "year_month", "threshold_profit_increase", "recommended_driver_count",
            "avg_net_profit_increase_per_driver", "avg_revenue_increase_per_driver",
            "total_revenue_increase",
        )
    )
