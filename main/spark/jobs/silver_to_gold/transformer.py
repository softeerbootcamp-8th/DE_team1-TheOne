"""원천 Silver 4종을 결합해 월별 기사 운행 집계(driver_aggregation)를 만든다.

추천 차량 배정(driver_car_suggestion)은 recommendation_algorithm/ 패키지가 만들되,
이 모듈의 build_driver_monthly_profit()·_columns() 등 공통 조각을 가져다 쓴다.
"""

import hashlib
import json
import logging
from calendar import monthrange
from dataclasses import fields
from datetime import datetime
from math import isclose

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from schema.gold import DriverMonthlyProfit
from schema.silver import (
    CLEAN_DRIVER_VEHICLE_MONTHLY_SNAPSHOT_SCHEMA,
    CLEAN_FUEL_PRICE_SCHEMA,
    CLEAN_LEASE_VEHICLE_INVENTORY_SCHEMA,
    CLEAN_MONTHLY_TAXI_TRIP_SCHEMA,
)


logger = logging.getLogger(__name__)
KWH_PER_GALLON_EQUIVALENT = 33.7
# 프리미엄 자격을 얻어도 Standard 수요가 모두 전환되지는 않습니다.
# 현재 사업 시나리오는 기존 Standard 운행 중 30%만 프리미엄으로 전환합니다.
PREMIUM_TIER_TRIP_SHARE = 0.3
# 거리대 경계와 라벨. _distance_band 와 algorithm_constants_digest 가 같은 출처를
# 보게 해서 한쪽만 고치는 실수를 막습니다(#1088).
_DISTANCE_BAND_EDGES: tuple[tuple[float, str], ...] = (
    (2.0, "0-2"),
    (5.0, "2-5"),
    (10.0, "5-10"),
    (20.0, "10-20"),
)
_DISTANCE_BAND_FALLBACK = "20+"


def algorithm_constants_digest() -> str:
    """Gold 계산에 직접 들어가는 고정 상수들의 SHA-256.

    이 값들이 바뀌면 결과가 달라지지만 recommendation_algorithm_version_id 는
    사람이 올려야 해서 깜빡하면 fingerprint 가 불변으로 남아 예전 Gold 를
    재사용합니다(#1088). 상수 자체의 해시를 fingerprint 에 넣어 수동 규율
    의존을 없앱니다.
    """
    payload = {
        "kwh_per_gallon_equivalent": KWH_PER_GALLON_EQUIVALENT,
        "premium_tier_trip_share": PREMIUM_TIER_TRIP_SHARE,
        "distance_band_edges": [
            [edge, label] for edge, label in _DISTANCE_BAND_EDGES
        ],
        "distance_band_fallback": _DISTANCE_BAND_FALLBACK,
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True).encode("utf-8")
    ).hexdigest()


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
    column = F.lit(_DISTANCE_BAND_FALLBACK)
    for edge, label in reversed(_DISTANCE_BAND_EDGES):
        column = F.when(trip_miles < edge, label).otherwise(column)
    return column


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


# 합계는 부동소수점이라 정확히 같기를 요구하면 안 된다. Spark 는 집계 순서가
# 실행마다 달라질 수 있어서다. 실측(NYC 2026-01, 운행 67.5만건)에서는 차이가
# 0.00 이었지만, 그게 앞으로도 0.00 이라는 보장은 없다.
CONTROL_TOTAL_REL_TOL = 1e-9
CONTROL_TOTAL_ABS_TOL = 1e-6


def reconcile_gold_control_totals(
    trips: DataFrame, driver_metrics: DataFrame
) -> None:
    """Silver 운행 합계가 기사별 집계를 거쳐 보존됐는지 확인합니다.

    조인 키 누락은 `_require_all_join_keys_match` 가 이미 막지만, 그건 "짝이 있나"
    를 보는 것이고 여기서는 "합이 남았나"를 봅니다. 행 수가 맞아도 값이 밀리는
    사고는 합계로만 잡힙니다.

    Silver 운행에는 `driver_id` 가 없고 `taxi_id` 뿐입니다 — 기사는 스냅샷 조인으로
    붙습니다. 그래서 스냅샷이 없는 `taxi_id` 의 운행이 조인에서 빠지면 합이 줄고,
    그 순간 여기서 걸립니다.
    """
    silver = trips.agg(
        F.count(F.lit(1)).alias("trips"),
        F.sum("trip_miles").alias("mileage"),
        F.sum("driver_pay").alias("driver_pay"),
        F.sum(F.coalesce(F.col("tips"), F.lit(0.0))).alias("tips"),
    ).first()
    gold = driver_metrics.agg(
        F.sum("monthly_mileage").alias("mileage"),
        F.sum("monthly_driver_pay").alias("driver_pay"),
        F.sum("monthly_tips").alias("tips"),
    ).first()

    mismatched = []
    for name in ("mileage", "driver_pay", "tips"):
        before = float(silver[name] or 0.0)
        after = float(gold[name] or 0.0)
        if not isclose(
            before,
            after,
            rel_tol=CONTROL_TOTAL_REL_TOL,
            abs_tol=CONTROL_TOTAL_ABS_TOL,
        ):
            mismatched.append(f"{name}: silver={before!r} gold={after!r}")
    if mismatched:
        raise ValueError(
            "Gold 집계에서 운행 합계가 보존되지 않았습니다 — "
            + "; ".join(mismatched)
            + f" (Silver 운행 {int(silver['trips'] or 0)}건)"
        )
    logger.info(
        "control total 보존 확인: trips=%d mileage=%.2f driver_pay=%.2f tips=%.2f",
        int(silver["trips"] or 0),
        float(silver["mileage"] or 0.0),
        float(silver["driver_pay"] or 0.0),
        float(silver["tips"] or 0.0),
    )


def validate_gold_business_invariants(
    driver_profit: DataFrame,
    recommendation: DataFrame,
    driver_snapshot: DataFrame,
    inventory: DataFrame,
) -> None:
    """Gold 저장 전에 기사 보존과 모델별 재고 한도를 검증합니다.

    recommendation 은 (recommendation_algorithm_version_id, threshold) 조합마다
    독립적인 "이 알고리즘·임계값을 쓰면 이렇게 배정된다"는 가정의 리포트를 담습니다
    (#997) — 여러 조합이 동시에 실제 재고를 나눠 쓰는 게 아니라 서로 다른 시나리오라,
    기사 보존과 재고 한도는 조합별로 따로 확인합니다.
    """
    driver_stats = {}
    for name, frame in (
        ("driver_aggregation", driver_profit),
        ("driver_snapshot", driver_snapshot),
    ):
        stats = frame.agg(
            F.count(F.lit(1)).alias("rows"),
            F.countDistinct("driver_id").alias("drivers"),
        ).first()
        driver_stats[name] = stats["rows"]
        if stats["rows"] != stats["drivers"]:
            raise ValueError(f"{name}의 driver_id가 null이거나 중복입니다")

    if len(set(driver_stats.values())) != 1:
        raise ValueError(f"Gold 기사 수 불일치: {driver_stats}")
    driver_count = driver_stats["driver_aggregation"]

    combos = [
        (row["recommendation_algorithm_version_id"], row["threshold"])
        for row in recommendation.select(
            "recommendation_algorithm_version_id", "threshold"
        )
        .distinct()
        .collect()
    ]
    for algorithm_version_id, threshold in combos:
        group = recommendation.filter(
            (F.col("recommendation_algorithm_version_id") == algorithm_version_id)
            & (F.col("threshold") == threshold)
        )
        label = f"driver_car_suggestion(algorithm={algorithm_version_id}, threshold={threshold})"

        group_stats = group.agg(
            F.count(F.lit(1)).alias("rows"),
            F.countDistinct("driver_id").alias("drivers"),
        ).first()
        if group_stats["rows"] != group_stats["drivers"]:
            raise ValueError(f"{label}의 driver_id가 null이거나 중복입니다")
        if group_stats["rows"] != driver_count:
            raise ValueError(
                "Gold 기사 수 불일치: "
                f"driver_aggregation={driver_count} {label}={group_stats['rows']}"
            )

        assigned = group.groupBy("vehicle_model_id").agg(
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
            raise ValueError(f"{label} 모델별 재고 초과: sample={samples}")

        negative_samples = [
            row.asDict(recursive=True)
            for row in group.filter(F.col("expected_net_profit_increase") < 0)
            .select("driver_id", "vehicle_model_id", "expected_net_profit_increase")
            .limit(5)
            .collect()
        ]
        if negative_samples:
            raise ValueError(
                f"{label} 예상 순수익 증가액이 음수입니다: sample={negative_samples}"
            )
