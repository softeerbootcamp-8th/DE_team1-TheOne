"""원천 Silver 4종을 직접 결합해 월별 Gold 3종을 만듭니다."""

from calendar import monthrange
from dataclasses import fields
from datetime import datetime

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from schema.gold import DriverMonthlyProfit, MonthlyReport, MonthlyVehicleRecommendation
from schema.silver import (
    CLEAN_DRIVER_VEHICLE_MONTHLY_SNAPSHOT_SCHEMA,
    CLEAN_FUEL_PRICE_SCHEMA,
    CLEAN_LEASE_VEHICLE_INVENTORY_SCHEMA,
    CLEAN_MONTHLY_TAXI_TRIP_SCHEMA,
)
KWH_PER_GALLON_EQUIVALENT = 33.7
MONTHLY_WEEKS = 4.0


def _columns(model: type) -> list[str]:
    return [field.name for field in fields(model)]


def _validate_year_month(year_month: str) -> None:
    parsed = datetime.strptime(year_month, "%Y-%m")
    if parsed.strftime("%Y-%m") != year_month:
        raise ValueError(f"year_month는 YYYY-MM 형식이어야 합니다: {year_month}")


def _require_columns(dataframe: DataFrame, required: set[str], dataset: str) -> None:
    missing = required - set(dataframe.columns)
    if missing:
        raise ValueError(f"{dataset} 필수 컬럼 누락: {sorted(missing)}")
    if dataframe.isEmpty():
        raise ValueError(f"{dataset} 데이터가 비어 있습니다")


def _has_rows(dataframe: DataFrame) -> bool:
    return dataframe.limit(1).count() > 0


def _validate_dimensions(
    driver_snapshot: DataFrame,
    inventory: DataFrame,
    fuel_price: DataFrame,
    year_month: str,
) -> None:
    snapshot_stats = driver_snapshot.agg(
        F.count(F.lit(1)).alias("rows"),
        F.count("driver_id").alias("driver_ids"),
        F.countDistinct("driver_id").alias("distinct_driver_ids"),
        F.count("taxi_id").alias("taxi_ids"),
        F.countDistinct("taxi_id").alias("distinct_taxi_ids"),
    ).first()
    if (
        snapshot_stats["rows"] != snapshot_stats["driver_ids"]
        or snapshot_stats["rows"] != snapshot_stats["distinct_driver_ids"]
        or snapshot_stats["rows"] != snapshot_stats["taxi_ids"]
        or snapshot_stats["rows"] != snapshot_stats["distinct_taxi_ids"]
    ):
        raise ValueError(
            "기사 차량 스냅샷의 driver_id와 taxi_id는 null 없이 고유해야 합니다"
        )

    inventory_stats = inventory.agg(
        F.count(F.lit(1)).alias("rows"),
        F.count("vehicle_model_id").alias("model_ids"),
        F.countDistinct("vehicle_model_id").alias("distinct_model_ids"),
    ).first()
    if (
        inventory_stats["rows"] != inventory_stats["model_ids"]
        or inventory_stats["rows"] != inventory_stats["distinct_model_ids"]
    ):
        raise ValueError("보유 차량의 vehicle_model_id는 null 없이 고유해야 합니다")

    expected_days = monthrange(*map(int, year_month.split("-")))[1]
    fuel_stats = fuel_price.agg(
        F.count(F.lit(1)).alias("rows"),
        F.count("date").alias("dates"),
        F.countDistinct("date").alias("distinct_dates"),
    ).first()
    if (
        fuel_stats["rows"] != expected_days
        or fuel_stats["dates"] != expected_days
        or fuel_stats["distinct_dates"] != expected_days
    ):
        raise ValueError(
            f"연료비 Silver는 {year_month}의 {expected_days}일이 모두 고유해야 합니다"
        )


def enrich_trips_with_fuel_cost(
    trips: DataFrame,
    driver_snapshot: DataFrame,
    inventory: DataFrame,
    fuel_price: DataFrame,
    year_month: str,
) -> DataFrame:
    """운행에 해당 월의 기사·현재 차량·일별 연료비를 붙입니다."""
    _validate_year_month(year_month)
    _require_columns(
        trips,
        set(CLEAN_MONTHLY_TAXI_TRIP_SCHEMA.names),
        "HVFHV Silver",
    )
    _require_columns(
        driver_snapshot,
        set(CLEAN_DRIVER_VEHICLE_MONTHLY_SNAPSHOT_SCHEMA.names),
        "기사 차량 스냅샷 Silver",
    )
    _require_columns(
        inventory,
        set(CLEAN_LEASE_VEHICLE_INVENTORY_SCHEMA.names),
        "보유 차량 Silver",
    )
    _require_columns(
        fuel_price,
        set(CLEAN_FUEL_PRICE_SCHEMA.names),
        "연료비 Silver",
    )

    if _has_rows(
        trips.filter(F.date_format("pickup_datetime", "yyyy-MM") != year_month)
    ):
        raise ValueError(f"HVFHV Silver에 {year_month}가 아닌 운행이 섞였습니다")
    if _has_rows(driver_snapshot.filter(F.col("snapshot_month") != year_month)):
        raise ValueError(f"기사 차량 스냅샷에 {year_month}가 아닌 행이 섞였습니다")
    if _has_rows(fuel_price.filter(F.date_format("date", "yyyy-MM") != year_month)):
        raise ValueError(f"연료비 Silver에 {year_month}가 아닌 날짜가 섞였습니다")
    invalid_trip = (
        F.col("trip_miles").isNull()
        | (F.col("trip_miles") <= 0)
        | F.col("driver_pay").isNull()
        | (F.col("driver_pay") < 0)
    )
    if _has_rows(trips.filter(invalid_trip)):
        raise ValueError("HVFHV Silver의 거리 또는 기사 수익이 유효하지 않습니다")
    _validate_dimensions(driver_snapshot, inventory, fuel_price, year_month)

    snapshots = driver_snapshot.alias("snapshot")
    vehicles = inventory.alias("vehicle")
    profile_join = snapshots.join(
        F.broadcast(vehicles),
        (F.col("snapshot.vehicle_model_id") == F.col("vehicle.vehicle_model_id"))
        & F.col("snapshot.manufacturer").eqNullSafe(F.col("vehicle.manufacturer"))
        & F.col("snapshot.model_name").eqNullSafe(F.col("vehicle.model_name"))
        & F.col("snapshot.fuel_type").eqNullSafe(F.col("vehicle.fuel_type"))
        & F.col("snapshot.comfort_eligible").eqNullSafe(
            F.col("vehicle.comfort_eligible")
        )
        & F.col("snapshot.extra_comfort_eligible").eqNullSafe(
            F.col("vehicle.extra_comfort_eligible")
        ),
        "inner",
    )

    profiles = profile_join.select(
        F.col("snapshot.driver_id").alias("driver_id"),
        F.col("snapshot.taxi_id").alias("taxi_id"),
        F.col("snapshot.vehicle_model_id").alias("vehicle_model_id"),
        F.col("snapshot.manufacturer").alias("manufacturer"),
        F.col("snapshot.model_name").alias("model_name"),
        F.col("vehicle.model_year").alias("model_year"),
        F.col("snapshot.fuel_type").alias("fuel_type"),
        F.col("vehicle.fuel_efficiency").alias("fuel_efficiency"),
        F.col("snapshot.comfort_eligible").alias("comfort_eligible"),
        F.col("snapshot.extra_comfort_eligible").alias("extra_comfort_eligible"),
        F.col("snapshot.weekly_lease_fee").alias("weekly_lease_fee"),
    )

    trip_rows = trips.alias("trip")
    profile_rows = profiles.alias("profile")
    price_rows = fuel_price.alias("price")
    enriched = (
        trip_rows.join(
            F.broadcast(profile_rows),
            F.col("trip.taxi_id") == F.col("profile.taxi_id"),
            "inner",
        )
        .join(
            F.broadcast(price_rows),
            F.to_date(F.col("trip.pickup_datetime")) == F.col("price.date"),
            "inner",
        )
        .select(
            F.col("profile.driver_id").alias("driver_id"),
            F.col("trip.taxi_id").alias("taxi_id"),
            F.col("profile.vehicle_model_id").alias("vehicle_model_id"),
            F.col("profile.manufacturer").alias("manufacturer"),
            F.col("profile.model_name").alias("model_name"),
            F.col("profile.model_year").alias("model_year"),
            F.col("profile.fuel_type").alias("fuel_type"),
            F.col("profile.fuel_efficiency").alias("fuel_efficiency"),
            F.col("profile.comfort_eligible").alias("comfort_eligible"),
            F.col("profile.extra_comfort_eligible").alias("extra_comfort_eligible"),
            F.col("profile.weekly_lease_fee").alias("weekly_lease_fee"),
            F.col("trip.hvfhs_license_num").alias("hvfhs_license_num"),
            F.col("trip.estimated_service_tier").alias("estimated_service_tier"),
            F.col("trip.trip_miles").alias("trip_miles"),
            F.col("trip.driver_pay").alias("driver_pay"),
            F.coalesce(F.col("trip.tips"), F.lit(0.0)).alias("tips"),
            F.col("price.gas_price").alias("gas_price"),
            F.col("price.ev_price").alias("ev_price"),
        )
        .persist()
    )
    return enriched


def _with_tier_revenue_scenarios(enriched: DataFrame) -> DataFrame:
    """license·등급별 총수익/총거리 배수를 각 운행의 교체 시나리오에 붙입니다."""
    rates = (
        enriched.groupBy("hvfhs_license_num", "estimated_service_tier")
        .agg(
            F.sum("driver_pay").alias("_tier_driver_pay"),
            F.sum("trip_miles").alias("_tier_trip_miles"),
        )
        .withColumn(
            "_rate_per_mile",
            F.col("_tier_driver_pay") / F.col("_tier_trip_miles"),
        )
    )
    by_license = rates.groupBy("hvfhs_license_num").pivot(
        "estimated_service_tier",
        ["Standard", "Comfort", "Extra Comfort"],
    ).agg(F.first("_rate_per_mile"))
    multipliers = by_license.select(
        "hvfhs_license_num",
        F.coalesce(F.col("Comfort") / F.col("Standard"), F.lit(1.0)).alias(
            "_comfort_multiplier"
        ),
        F.coalesce(
            F.col("Extra Comfort") / F.col("Standard"), F.lit(1.0)
        ).alias("_extra_comfort_multiplier"),
    )

    rows = enriched.join(
        F.broadcast(multipliers), "hvfhs_license_num", "left"
    ).fillna(
        {
            "_comfort_multiplier": 1.0,
            "_extra_comfort_multiplier": 1.0,
        }
    )
    standard = F.col("estimated_service_tier") == "Standard"
    uber_standard = standard & (F.col("hvfhs_license_num") == "HV0003")
    lyft_standard = standard & (F.col("hvfhs_license_num") == "HV0005")
    return (
        rows.withColumn(
            "_driver_pay_if_comfort",
            F.when(
                uber_standard,
                F.col("driver_pay") * F.col("_comfort_multiplier"),
            ).otherwise(F.col("driver_pay")),
        )
        .withColumn(
            "_driver_pay_if_extra_comfort",
            F.when(
                lyft_standard,
                F.col("driver_pay") * F.col("_extra_comfort_multiplier"),
            ).otherwise(F.col("driver_pay")),
        )
        .withColumn(
            "_driver_pay_if_both",
            F.when(
                uber_standard,
                F.col("driver_pay") * F.col("_comfort_multiplier"),
            )
            .when(
                lyft_standard,
                F.col("driver_pay") * F.col("_extra_comfort_multiplier"),
            )
            .otherwise(F.col("driver_pay")),
        )
    )


def build_driver_monthly_aggregation(
    enriched: DataFrame, year_month: str
) -> DataFrame:
    """기사별 실제 운행·비용을 집계하고 추천 계산용 연료비 기준값을 보존합니다."""
    grouped = _with_tier_revenue_scenarios(enriched).groupBy(
        "driver_id",
        "taxi_id",
        "vehicle_model_id",
        "manufacturer",
        "model_name",
        "model_year",
        "fuel_type",
        "fuel_efficiency",
        "comfort_eligible",
        "extra_comfort_eligible",
        "weekly_lease_fee",
    ).agg(
        F.sum("trip_miles").alias("monthly_mileage"),
        F.sum("driver_pay").alias("monthly_driver_pay"),
        F.sum("_driver_pay_if_comfort").alias("_monthly_driver_pay_if_comfort"),
        F.sum("_driver_pay_if_extra_comfort").alias(
            "_monthly_driver_pay_if_extra_comfort"
        ),
        F.sum("_driver_pay_if_both").alias("_monthly_driver_pay_if_both"),
        F.sum("tips").alias("monthly_tips"),
        F.sum(F.col("trip_miles") * F.col("gas_price")).alias("_gas_price_miles"),
        F.sum(F.col("trip_miles") * F.col("ev_price")).alias("_ev_price_miles"),
    )
    current_fuel_cost = F.when(
        F.col("fuel_type") == "EV",
        F.col("_ev_price_miles")
        * F.lit(KWH_PER_GALLON_EQUIVALENT)
        / F.col("fuel_efficiency"),
    ).otherwise(F.col("_gas_price_miles") / F.col("fuel_efficiency"))

    return (
        grouped.withColumn("year_month", F.lit(year_month))
        .withColumn("monthly_fuel_cost", current_fuel_cost)
        .withColumn(
            "monthly_lease_fee", F.col("weekly_lease_fee") * F.lit(MONTHLY_WEEKS)
        )
        .withColumn(
            "monthly_net_profit",
            F.col("monthly_driver_pay")
            + F.col("monthly_tips")
            - F.col("monthly_fuel_cost")
            - F.col("monthly_lease_fee"),
        )
    )


def build_driver_monthly_profit(driver_metrics: DataFrame) -> DataFrame:
    """확정된 Gold 스키마의 기사 월 순수익 컬럼만 반환합니다."""
    return driver_metrics.select(*_columns(DriverMonthlyProfit))


def build_monthly_vehicle_recommendation(
    driver_metrics: DataFrame,
    inventory: DataFrame,
) -> DataFrame:
    """재고가 있는 후보 중 기사 예상 순수익이 가장 높은 차량 한 대를 고릅니다."""
    available = inventory.select(
        F.col("vehicle_model_id").alias("_candidate_vehicle_model_id"),
        F.col("manufacturer").alias("_candidate_manufacturer"),
        F.col("model_name").alias("_candidate_model_name"),
        F.col("model_year").alias("_candidate_model_year"),
        F.col("fuel_type").alias("_candidate_fuel_type"),
        F.col("fuel_efficiency").alias("_candidate_fuel_efficiency"),
        F.col("comfort_eligible").alias("_candidate_comfort_eligible"),
        F.col("extra_comfort_eligible").alias("_candidate_extra_comfort_eligible"),
        F.col("weekly_lease_fee").alias("_candidate_weekly_lease_fee"),
        F.col("stock").alias("_candidate_stock"),
    )
    if available.isEmpty():
        raise ValueError("추천할 수 있는 재고 차량이 없습니다")

    candidates = (
        driver_metrics.crossJoin(F.broadcast(available))
        .withColumn(
            "_is_current",
            F.col("vehicle_model_id") == F.col("_candidate_vehicle_model_id"),
        )
        .filter((F.col("_candidate_stock") > 0) | F.col("_is_current"))
    )
    expected_fuel_cost = F.when(
        F.col("_candidate_fuel_type") == "EV",
        F.col("_ev_price_miles")
        * F.lit(KWH_PER_GALLON_EQUIVALENT)
        / F.col("_candidate_fuel_efficiency"),
    ).otherwise(F.col("_gas_price_miles") / F.col("_candidate_fuel_efficiency"))
    expected_lease_fee = F.when(
        F.col("_is_current"), F.col("monthly_lease_fee")
    ).otherwise(F.col("_candidate_weekly_lease_fee") * F.lit(MONTHLY_WEEKS))
    gains_comfort = (
        ~F.col("comfort_eligible") & F.col("_candidate_comfort_eligible")
    )
    gains_extra_comfort = (
        ~F.col("extra_comfort_eligible")
        & F.col("_candidate_extra_comfort_eligible")
    )
    expected_driver_pay = (
        F.when(
            gains_comfort & gains_extra_comfort,
            F.col("_monthly_driver_pay_if_both"),
        )
        .when(gains_comfort, F.col("_monthly_driver_pay_if_comfort"))
        .when(
            gains_extra_comfort,
            F.col("_monthly_driver_pay_if_extra_comfort"),
        )
        .otherwise(F.col("monthly_driver_pay"))
    )
    candidates = (
        candidates.withColumn("expected_monthly_fuel_cost", expected_fuel_cost)
        .withColumn("recommended_monthly_lease_fee", expected_lease_fee)
        .withColumn(
            "expected_monthly_net_profit",
            expected_driver_pay
            + F.col("monthly_tips")
            - F.col("expected_monthly_fuel_cost")
            - F.col("recommended_monthly_lease_fee"),
        )
        .withColumn(
            "expected_net_profit_increase",
            F.col("expected_monthly_net_profit") - F.col("monthly_net_profit"),
        )
        .withColumn(
            "expected_revenue_increase",
            F.col("recommended_monthly_lease_fee") - F.col("monthly_lease_fee"),
        )
    )
    reasons = F.concat_ws(
        ", ",
        F.when(
            F.col("recommended_monthly_lease_fee") < F.col("monthly_lease_fee"),
            F.lit("렌트비 절감"),
        ),
        F.when(
            F.col("expected_monthly_fuel_cost") < F.col("monthly_fuel_cost"),
            F.lit("연료비 절감"),
        ),
        F.when(
            ~F.col("comfort_eligible") & F.col("_candidate_comfort_eligible"),
            F.lit("Comfort 등급 가능"),
        ),
        F.when(
            ~F.col("extra_comfort_eligible")
            & F.col("_candidate_extra_comfort_eligible"),
            F.lit("Extra Comfort 등급 가능"),
        ),
    )
    candidates = candidates.withColumn("_reasons", reasons).withColumn(
        "recommendation_reason",
        F.when(F.col("_is_current"), F.lit("현재 차량 유지"))
        .when(F.length("_reasons") > 0, F.col("_reasons"))
        .otherwise(F.lit("예상 순수익 개선")),
    )

    rank = Window.partitionBy("driver_id").orderBy(
        F.col("expected_monthly_net_profit").desc(),
        F.col("_is_current").desc(),
        F.col("_candidate_model_year").desc(),
        F.col("_candidate_vehicle_model_id").asc(),
    )
    best = candidates.withColumn("_rank", F.row_number().over(rank)).filter(
        F.col("_rank") == 1
    )
    return best.select(
        "driver_id",
        "year_month",
        F.col("_candidate_comfort_eligible").alias("comfort_eligible"),
        F.col("_candidate_extra_comfort_eligible").alias("extra_comfort_eligible"),
        F.col("_candidate_vehicle_model_id").alias("vehicle_model_id"),
        F.col("_candidate_manufacturer").alias("manufacturer"),
        F.col("_candidate_model_name").alias("model_name"),
        F.col("_candidate_model_year").alias("model_year"),
        "recommendation_reason",
        F.col("_candidate_fuel_efficiency").alias("fuel_efficiency"),
        "recommended_monthly_lease_fee",
        "expected_monthly_fuel_cost",
        "expected_monthly_net_profit",
        "expected_net_profit_increase",
        "expected_revenue_increase",
    ).select(*_columns(MonthlyVehicleRecommendation))


def build_monthly_report(
    recommendation: DataFrame,
    year_month: str,
    threshold_profit_increase: float,
) -> DataFrame:
    """기사·회사 기준을 함께 통과한 추천을 월 1행으로 요약합니다."""
    eligible = recommendation.filter(
        (F.col("expected_net_profit_increase") >= threshold_profit_increase)
        & (F.col("expected_revenue_increase") >= 0)
    )
    return (
        eligible.agg(
            F.count(F.lit(1)).alias("recommended_driver_count"),
            F.coalesce(F.avg("expected_net_profit_increase"), F.lit(0.0)).alias(
                "avg_net_profit_increase_per_driver"
            ),
            F.coalesce(F.avg("expected_revenue_increase"), F.lit(0.0)).alias(
                "avg_revenue_increase_per_driver"
            ),
            F.coalesce(F.sum("expected_revenue_increase"), F.lit(0.0)).alias(
                "total_revenue_increase"
            ),
        )
        .select(
            F.lit(year_month).alias("year_month"),
            F.lit(float(threshold_profit_increase)).alias(
                "threshold_profit_increase"
            ),
            "recommended_driver_count",
            "avg_net_profit_increase_per_driver",
            "avg_revenue_increase_per_driver",
            "total_revenue_increase",
        )
        .select(*_columns(MonthlyReport))
    )
