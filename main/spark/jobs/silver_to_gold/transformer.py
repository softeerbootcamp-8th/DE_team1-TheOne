"""원천 Silver 4종을 직접 결합해 월별 Gold 2종을 만듭니다."""

from calendar import monthrange
from dataclasses import fields
from datetime import datetime

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from schema.gold import (
    DriverMonthlyProfit,
    DriverCarSuggestion,
)
from schema.silver import (
    CLEAN_DRIVER_VEHICLE_MONTHLY_SNAPSHOT_SCHEMA,
    CLEAN_FUEL_PRICE_SCHEMA,
    CLEAN_LEASE_VEHICLE_INVENTORY_SCHEMA,
    CLEAN_MONTHLY_TAXI_TRIP_SCHEMA,
)
KWH_PER_GALLON_EQUIVALENT = 33.7
# 프리미엄 자격을 얻어도 Standard 수요가 모두 전환되지는 않습니다.
# 현재 사업 시나리오는 기존 Standard 운행 중 40%만 프리미엄으로 전환합니다.
PREMIUM_TIER_TRIP_SHARE = 0.4
# 추천 계산 로직이 바뀔 때만 사람이 올리는 알고리즘 코드 버전입니다.
# 적재 시점마다 바뀌는 DriverCarSuggestion.version과는 다른 축입니다.
RECOMMENDATION_ALGORITHM_VERSION_ID = 1


def _columns(model: type) -> list[str]:
    """`version`은 DB 적재 시점(기존 버전 + 1)에 결정되는 값이라 Spark 산출물에는
    포함하지 않습니다."""
    return [field.name for field in fields(model) if field.name != "version"]


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


def _require_all_join_keys_match(
    left: DataFrame,
    right: DataFrame,
    condition,
    relationship: str,
    sample_columns: list[str],
) -> None:
    """inner join 전에 미매칭 키를 찾아 조용한 행 유실을 막습니다."""
    unmatched = left.join(F.broadcast(right), condition, "left_anti")
    samples = [
        row.asDict(recursive=True)
        for row in unmatched.select(*sample_columns).limit(5).collect()
    ]
    if samples:
        raise ValueError(f"{relationship} 조인 키 미매칭: sample={samples}")


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
    if _has_rows(inventory.filter(F.col("stock").isNull() | (F.col("stock") < 0))):
        raise ValueError("보유 차량의 stock은 null이 아닌 0 이상의 정수여야 합니다")
    occupied = driver_snapshot.groupBy("vehicle_model_id").agg(
        F.count(F.lit(1)).alias("occupied_stock")
    )
    over_occupied = occupied.join(
        F.broadcast(inventory.select("vehicle_model_id", "stock")),
        "vehicle_model_id",
        "left",
    ).filter(F.col("stock").isNull() | (F.col("occupied_stock") > F.col("stock")))
    occupied_samples = [
        row.asDict(recursive=True) for row in over_occupied.limit(5).collect()
    ]
    if occupied_samples:
        raise ValueError(
            f"현재 운행 차량 수가 보유 재고를 초과합니다: sample={occupied_samples}"
        )

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
    # 경로에서 대상 월을 골랐더라도 파일 내용을 다시 제한해 잘못된 날짜가
    # 운행과 조인되지 않게 합니다. 이후 일수 검증이 누락·중복을 잡습니다.
    fuel_price = fuel_price.filter(F.date_format("date", "yyyy-MM") == year_month)
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
    profile_condition = (
        (F.col("snapshot.vehicle_model_id") == F.col("vehicle.vehicle_model_id"))
        & F.col("snapshot.manufacturer").eqNullSafe(F.col("vehicle.manufacturer"))
        & F.col("snapshot.model_name").eqNullSafe(F.col("vehicle.model_name"))
        & F.col("snapshot.fuel_type").eqNullSafe(F.col("vehicle.fuel_type"))
        & F.col("snapshot.comfort_eligible").eqNullSafe(
            F.col("vehicle.comfort_eligible")
        )
        & F.col("snapshot.extra_comfort_eligible").eqNullSafe(
            F.col("vehicle.extra_comfort_eligible")
        )
    )
    _require_all_join_keys_match(
        snapshots,
        vehicles,
        profile_condition,
        "기사 차량 스냅샷→보유 차량",
        ["driver_id", "taxi_id", "vehicle_model_id"],
    )
    profile_join = snapshots.join(
        F.broadcast(vehicles),
        profile_condition,
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
    trip_profile_condition = F.col("trip.taxi_id") == F.col("profile.taxi_id")
    _require_all_join_keys_match(
        trip_rows,
        profile_rows,
        trip_profile_condition,
        "HVFHV 운행→기사 차량 프로필",
        ["taxi_id"],
    )
    trip_price_condition = (
        F.to_date(F.col("trip.pickup_datetime")) == F.col("price.date")
    )
    _require_all_join_keys_match(
        trip_rows,
        price_rows,
        trip_price_condition,
        "HVFHV 운행→일별 연료비",
        ["taxi_id", "pickup_datetime"],
    )
    enriched = (
        trip_rows.join(
            F.broadcast(profile_rows),
            trip_profile_condition,
            "inner",
        )
        .join(
            F.broadcast(price_rows),
            trip_price_condition,
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


def _distance_band(trip_miles: Column) -> Column:
    """거리별 단가 차이를 보존하는 고정 운행거리 구간입니다."""
    return (
        F.when(trip_miles < 2, F.lit("0-2"))
        .when(trip_miles < 5, F.lit("2-5"))
        .when(trip_miles < 10, F.lit("5-10"))
        .when(trip_miles < 20, F.lit("10-20"))
        .otherwise(F.lit("20+"))
    )


def _with_tier_revenue_scenarios(enriched: DataFrame) -> DataFrame:
    """license·거리대·등급별 단가 배수를 각 운행의 교체 시나리오에 붙입니다."""
    banded = enriched.withColumn("_distance_band", _distance_band(F.col("trip_miles")))
    rates = (
        banded.groupBy(
            "hvfhs_license_num", "_distance_band", "estimated_service_tier"
        )
        .agg(
            F.sum("driver_pay").alias("_tier_driver_pay"),
            F.sum("trip_miles").alias("_tier_trip_miles"),
        )
        .withColumn(
            "_rate_per_mile",
            F.col("_tier_driver_pay") / F.col("_tier_trip_miles"),
        )
    )
    by_license = rates.groupBy("hvfhs_license_num", "_distance_band").pivot(
        "estimated_service_tier", ["Standard", "Comfort", "Extra Comfort"]
    ).agg(F.first("_rate_per_mile"))
    multipliers = by_license.select(
        "hvfhs_license_num",
        "_distance_band",
        (F.col("Comfort") / F.col("Standard")).alias("_comfort_multiplier"),
        (F.col("Extra Comfort") / F.col("Standard")).alias(
            "_extra_comfort_multiplier"
        ),
    )

    rows = banded.join(
        F.broadcast(multipliers),
        ["hvfhs_license_num", "_distance_band"],
        "left",
    )
    standard = F.col("estimated_service_tier") == "Standard"
    uber_standard = standard & (F.col("hvfhs_license_num") == "HV0003")
    lyft_standard = standard & (F.col("hvfhs_license_num") == "HV0005")
    missing = rows.filter(
        (uber_standard & F.col("_comfort_multiplier").isNull())
        | (lyft_standard & F.col("_extra_comfort_multiplier").isNull())
    )
    missing_samples = [
        row.asDict(recursive=True)
        for row in missing.select(
            "hvfhs_license_num", "_distance_band", "estimated_service_tier"
        )
        .distinct()
        .limit(5)
        .collect()
    ]
    if missing_samples:
        raise ValueError(f"거리대별 프리미엄 배수 결측: sample={missing_samples}")

    comfort_pay = F.col("driver_pay") * (
        F.lit(1.0)
        + F.lit(PREMIUM_TIER_TRIP_SHARE)
        * (F.col("_comfort_multiplier") - F.lit(1.0))
    )
    extra_comfort_pay = F.col("driver_pay") * (
        F.lit(1.0)
        + F.lit(PREMIUM_TIER_TRIP_SHARE)
        * (F.col("_extra_comfort_multiplier") - F.lit(1.0))
    )
    return (
        rows.withColumn(
            "_driver_pay_if_comfort",
            F.when(
                uber_standard,
                comfort_pay,
            ).otherwise(F.col("driver_pay")),
        )
        .withColumn(
            "_driver_pay_if_extra_comfort",
            F.when(
                lyft_standard,
                extra_comfort_pay,
            ).otherwise(F.col("driver_pay")),
        )
        .withColumn(
            "_driver_pay_if_both",
            F.when(
                uber_standard,
                comfort_pay,
            )
            .when(
                lyft_standard,
                extra_comfort_pay,
            )
            .otherwise(F.col("driver_pay")),
        )
    )


def build_driver_monthly_aggregation(
    enriched: DataFrame, year_month: str, service_area: str
) -> DataFrame:
    """기사별 실제 운행·비용을 집계하고 추천 계산용 연료비 기준값을 보존합니다."""
    days_in_month = monthrange(*map(int, year_month.split("-")))[1]
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
        # service_area 는 year_month 와 같은 축의 데이터 속성입니다 — version 과
        # 달리 적재 시점에 정해지는 값이 아니라 잡 실행 대상을 나타냅니다.
        .withColumn("service_area", F.lit(service_area))
        .withColumn("_lease_weeks_in_month", F.lit(days_in_month / 7.0))
        .withColumn("monthly_fuel_cost", current_fuel_cost)
        .withColumn(
            "monthly_lease_fee",
            F.col("weekly_lease_fee") * F.col("_lease_weeks_in_month"),
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


def _validate_candidate_grain(
    driver_profit: DataFrame,
    recommendation_candidates: DataFrame,
    inventory: DataFrame,
) -> None:
    """내부 후보가 실제 기사 N × 재고 모델 M 조합을 모두 보존하는지 검증합니다."""
    driver_count = driver_profit.select("driver_id").distinct().count()
    inventory_models = inventory.select("vehicle_model_id").distinct().count()
    candidate_stats = recommendation_candidates.agg(
        F.count(F.lit(1)).alias("rows"),
        F.countDistinct("driver_id").alias("drivers"),
        F.countDistinct("driver_id", "_candidate_vehicle_model_id").alias(
            "candidate_keys"
        ),
    ).first()
    expected_candidates = driver_count * inventory_models
    if (
        candidate_stats["rows"] != expected_candidates
        or candidate_stats["rows"] != candidate_stats["candidate_keys"]
        or candidate_stats["drivers"] != driver_count
    ):
        raise ValueError(
            "Gold 추천 후보 수 불일치: "
            f"drivers={driver_count} "
            f"vehicle_models={inventory_models} "
            f"expected={expected_candidates} actual={candidate_stats['rows']}"
        )


def _allocate_candidates_by_stock(candidates: DataFrame) -> DataFrame:
    """기사별 수익 순위대로 제안하고 남은 모델 재고 안에서 Spark로 배정합니다."""
    preference = Window.partitionBy("driver_id").orderBy(
        F.col("expected_monthly_net_profit").desc(),
        F.col("_is_current").desc(),
        F.col("_candidate_model_year").desc(),
        F.col("_candidate_vehicle_model_id").asc(),
    )
    ranked = candidates.withColumn(
        "_driver_rank", F.row_number().over(preference)
    ).persist()
    occupied_stock = (
        ranked.filter(F.col("_is_current"))
        .groupBy("_candidate_vehicle_model_id")
        .agg(F.count(F.lit(1)).alias("_occupied_stock"))
    )
    max_rank = ranked.agg(F.max("_driver_rank")).first()[0]
    assigned = None

    for driver_rank in range(1, max_rank + 1):
        proposals = ranked.filter(F.col("_driver_rank") == driver_rank)
        if assigned is not None:
            proposals = proposals.join(
                assigned.select("driver_id"), "driver_id", "left_anti"
            )

        keep_current = proposals.filter(F.col("_is_current"))
        changes = (
            proposals.filter(~F.col("_is_current"))
            .join(occupied_stock, "_candidate_vehicle_model_id", "left")
            .fillna({"_occupied_stock": 0})
        )
        if assigned is None:
            changes = changes.withColumn("_used_stock", F.lit(0))
        else:
            used_stock = (
                assigned.filter(~F.col("_is_current"))
                .groupBy("_candidate_vehicle_model_id")
                .agg(F.count(F.lit(1)).alias("_used_stock"))
            )
            changes = changes.join(
                used_stock, "_candidate_vehicle_model_id", "left"
            ).fillna({"_used_stock": 0})

        stock_priority = Window.partitionBy("_candidate_vehicle_model_id").orderBy(
            F.col("expected_net_profit_increase").desc(),
            F.col("expected_revenue_increase").desc(),
            F.col("driver_id").asc(),
        )
        changes = (
            changes.withColumn("_stock_rank", F.row_number().over(stock_priority))
            .filter(
                F.col("_stock_rank")
                <= F.col("_candidate_stock")
                - F.col("_occupied_stock")
                - F.col("_used_stock")
            )
            .drop("_occupied_stock", "_used_stock", "_stock_rank")
        )
        winners = keep_current.unionByName(changes)
        assigned = winners if assigned is None else assigned.unionByName(winners)
        assigned = assigned.coalesce(8).localCheckpoint(eager=False)

    ranked.unpersist()
    return assigned


def validate_gold_business_invariants(
    driver_profit: DataFrame,
    recommendation: DataFrame,
    driver_snapshot: DataFrame,
    inventory: DataFrame,
) -> None:
    """Gold 저장 전에 기사 보존과 모델별 재고 한도를 검증합니다."""
    counts = {}
    for name, frame in (
        ("driver_aggregation", driver_profit),
        ("driver_car_suggestion", recommendation),
        ("driver_snapshot", driver_snapshot),
    ):
        stats = frame.agg(
            F.count(F.lit(1)).alias("rows"),
            F.countDistinct("driver_id").alias("drivers"),
        ).first()
        counts[name] = stats["rows"]
        if stats["rows"] != stats["drivers"]:
            raise ValueError(f"{name}의 driver_id가 null이거나 중복입니다")

    if len(set(counts.values())) != 1:
        raise ValueError(f"Gold 기사 수 불일치: {counts}")

    assigned = recommendation.groupBy("vehicle_model_id").agg(
        F.count(F.lit(1)).alias("assigned")
    )
    overstocked = assigned.join(
        F.broadcast(inventory.select("vehicle_model_id", "stock")),
        "vehicle_model_id",
        "left",
    ).filter(F.col("stock").isNull() | (F.col("assigned") > F.col("stock")))
    samples = [
        row.asDict(recursive=True) for row in overstocked.limit(5).collect()
    ]
    if samples:
        raise ValueError(f"Gold 모델별 재고 초과: sample={samples}")

    negative_samples = [
        row.asDict(recursive=True)
        for row in recommendation.filter(F.col("expected_net_profit_increase") < 0)
        .select("driver_id", "vehicle_model_id", "expected_net_profit_increase")
        .limit(5)
        .collect()
    ]
    if negative_samples:
        raise ValueError(
            f"Gold 예상 순수익 증가액이 음수입니다: sample={negative_samples}"
        )


def build_monthly_vehicle_recommendation(
    driver_metrics: DataFrame,
    inventory: DataFrame,
) -> DataFrame:
    """동적 기사 N×모델 M 후보를 계산하고 재고 안에서 기사별 차량을 배정합니다."""
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

    candidates = driver_metrics.crossJoin(F.broadcast(available)).withColumn(
        "_is_current",
        F.col("vehicle_model_id") == F.col("_candidate_vehicle_model_id"),
    )
    expected_fuel_cost = F.when(
        F.col("_candidate_fuel_type") == "EV",
        F.col("_ev_price_miles")
        * F.lit(KWH_PER_GALLON_EQUIVALENT)
        / F.col("_candidate_fuel_efficiency"),
    ).otherwise(F.col("_gas_price_miles") / F.col("_candidate_fuel_efficiency"))
    expected_lease_fee = F.when(
        F.col("_is_current"), F.col("monthly_lease_fee")
    ).otherwise(
        F.col("_candidate_weekly_lease_fee")
        * F.col("_lease_weeks_in_month")
    )
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
    eligible_tiers = F.concat_ws(
        ", ",
        F.when(
            ~F.col("comfort_eligible") & F.col("_candidate_comfort_eligible"),
            F.lit("Comfort(Uber)"),
        ),
        F.when(
            ~F.col("extra_comfort_eligible")
            & F.col("_candidate_extra_comfort_eligible"),
            F.lit("Extra Comfort(Lyft)"),
        ),
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
            F.length(eligible_tiers) > 0,
            F.concat(eligible_tiers, F.lit(" 등급 가능")),
        ),
    )
    candidates = candidates.withColumn("_reasons", reasons).withColumn(
        "recommendation_reason",
        F.when(F.col("_is_current"), F.lit("현재 차량 유지"))
        .when(F.length("_reasons") > 0, F.col("_reasons"))
        .otherwise(F.lit("예상 순수익 개선")),
    )

    driver_profit = build_driver_monthly_profit(driver_metrics)
    _validate_candidate_grain(driver_profit, candidates, inventory)

    def recommendation_output(rows: DataFrame) -> DataFrame:
        return rows.select(
            "driver_id",
            "year_month",
            "service_area",
            F.col("_candidate_comfort_eligible").alias("comfort_eligible"),
            F.col("_candidate_extra_comfort_eligible").alias(
                "extra_comfort_eligible"
            ),
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
            F.lit(RECOMMENDATION_ALGORITHM_VERSION_ID).alias(
                "recommendation_algorithm_version_id"
            ),
        ).select(*_columns(DriverCarSuggestion))

    # 회사 매출에 기여 못 하는 차량 교체는 추천에서 제외한다(#955). 현재 차량은
    # 정의상 매출 증가가 0이라 이 조건에서 예외로 둬 최후의 보루로 남긴다.
    assignable = candidates.filter(
        F.col("_is_current")
        | (
            (F.col("_candidate_stock") > 0)
            & (F.col("expected_revenue_increase") > 0)
        )
    )
    return recommendation_output(_allocate_candidates_by_stock(assignable))
