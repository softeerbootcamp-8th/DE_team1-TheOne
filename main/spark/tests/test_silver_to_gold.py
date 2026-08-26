"""Silver → Gold 재고 배정 시나리오. 이슈 #927.

1. 실제 기사·차종 수로 후보를 계산하고 현재 보유 차량을 차감해 배정
2. 재고 0 후보는 건너뛰고 모든 기사는 한 개의 최종 차량을 받음
3. 프리미엄 배수는 5개 거리 구간별 실측값을 각 운행에 적용
4. Gold 파일은 집계와 최종 추천 2종만 함께 교체
5. Gold 실행은 Airflow run·Spark code·안정적 config hash를 계보에 기록
6. 수치 차원의 NaN·Infinity·null은 Gold 진입 전에 거부 (#1080)
"""

from calendar import monthrange
from datetime import date, datetime

import pytest
from pyspark.sql import functions as F

from main.spark.jobs.silver_to_gold import transformer as gold_transformer
from main.spark.jobs.silver_to_gold.recommendation_algorithm import ProfitFirstAlgorithm
from shared.spark.common.session import get_or_create_spark_session


@pytest.fixture(scope="module")
def spark():
    session = get_or_create_spark_session("test_silver_to_gold_stock")
    yield session
    session.stop()


def test_현재보유차량을_차감하고_재고0은_skip한다(spark):
    driver_metrics = spark.createDataFrame(
        [
            ("D1", "2026-01", "NYC", False, False, "T1", "A", "MAKE", "A", 2024, 30.0, 1000.0, 1000.0, 0.0, 100.0, 200.0, 700.0, 3000.0, 50.0, 1000.0, 1000.0, 1000.0, 4.0),
            ("D2", "2026-01", "NYC", False, False, "T2", "A", "MAKE", "A", 2024, 30.0, 1000.0, 1000.0, 0.0, 20.0, 200.0, 780.0, 600.0, 50.0, 1000.0, 1000.0, 1000.0, 4.0),
            ("D3", "2026-01", "NYC", False, False, "T3", "B", "MAKE", "B", 2024, 60.0, 1000.0, 1000.0, 0.0, 10.0, 200.0, 790.0, 600.0, 50.0, 1000.0, 1000.0, 1000.0, 4.0),
        ],
        [
            "driver_id", "year_month", "service_area", "comfort_eligible",
            "extra_comfort_eligible", "taxi_id", "vehicle_model_id", "manufacturer",
            "model_name", "model_year", "fuel_efficiency", "monthly_mileage",
            "monthly_driver_pay", "monthly_tips", "monthly_fuel_cost",
            "monthly_lease_fee", "monthly_net_profit", "_gas_price_miles", "_ev_price_miles",
            "_monthly_driver_pay_if_comfort", "_monthly_driver_pay_if_extra_comfort",
            "_monthly_driver_pay_if_both", "_lease_weeks_in_month",
        ],
    )
    inventory = spark.createDataFrame(
        [
            ("A", "MAKE", "A", 2024, "GAS", 30.0, False, False, 50.0, 2),
            ("B", "MAKE", "B", 2024, "GAS", 60.0, False, False, 55.0, 2),
            ("C", "MAKE", "C", 2025, "EV", 1000.0, True, True, 0.0, 0),
        ],
        [
            "vehicle_model_id", "manufacturer", "model_name", "model_year",
            "fuel_type", "fuel_efficiency", "comfort_eligible",
            "extra_comfort_eligible", "weekly_lease_fee", "stock",
        ],
    )

    recommendation = ProfitFirstAlgorithm().recommend(driver_metrics, inventory)
    assigned = {row.driver_id: row.vehicle_model_id for row in recommendation.collect()}

    assert assigned == {"D1": "B", "D2": "A", "D3": "B"}
    assert len(assigned) == 3
    assert "candidate_vehicle_model_id" not in recommendation.columns


def test_기존_보유자의_재고는_신규_스위처에게_뺏기지_않는다(spark):
    """말리부 재고가 D3가 지금 타는 딱 1대뿐이면, D1·D2가 말리부로 바꾸는 게 더
    이득이어도 그 1대를 가져갈 수 없고 D3는 자기 차를 그대로 유지해야 한다.
    `_allocate_candidates_by_stock`의 occupied_stock이 기존 보유자 몫을
    스위처 경쟁에서 먼저 빼두기 때문."""
    driver_metrics = spark.createDataFrame(
        [
            ("D1", "2026-01", "NYC", False, False, "T1", "A", "MAKE", "A", 2024, 30.0, 1000.0, 1000.0, 0.0, 100.0, 200.0, 700.0, 3000.0, 50.0, 1000.0, 1000.0, 1000.0, 4.0),
            ("D2", "2026-01", "NYC", False, False, "T2", "A", "MAKE", "A", 2024, 30.0, 1000.0, 1000.0, 0.0, 100.0, 200.0, 700.0, 3000.0, 50.0, 1000.0, 1000.0, 1000.0, 4.0),
            ("D3", "2026-01", "NYC", False, False, "T3", "M", "MAKE", "MALIBU", 2024, 30.0, 1000.0, 1000.0, 0.0, 100.0, 200.0, 700.0, 3000.0, 50.0, 1000.0, 1000.0, 1000.0, 4.0),
        ],
        [
            "driver_id", "year_month", "service_area", "comfort_eligible",
            "extra_comfort_eligible", "taxi_id", "vehicle_model_id", "manufacturer",
            "model_name", "model_year", "fuel_efficiency", "monthly_mileage",
            "monthly_driver_pay", "monthly_tips", "monthly_fuel_cost",
            "monthly_lease_fee", "monthly_net_profit", "_gas_price_miles", "_ev_price_miles",
            "_monthly_driver_pay_if_comfort", "_monthly_driver_pay_if_extra_comfort",
            "_monthly_driver_pay_if_both", "_lease_weeks_in_month",
        ],
    )
    inventory = spark.createDataFrame(
        [
            ("A", "MAKE", "A", 2024, "GAS", 30.0, False, False, 50.0, 5),
            # 말리부 재고 딱 1대 — 지금 D3가 타는 그 1대뿐, 여유 없음.
            ("M", "MAKE", "MALIBU", 2024, "GAS", 60.0, False, False, 50.0, 1),
        ],
        [
            "vehicle_model_id", "manufacturer", "model_name", "model_year",
            "fuel_type", "fuel_efficiency", "comfort_eligible",
            "extra_comfort_eligible", "weekly_lease_fee", "stock",
        ],
    )

    recommendation = ProfitFirstAlgorithm().recommend(driver_metrics, inventory)
    assigned = {row.driver_id: row.vehicle_model_id for row in recommendation.collect()}

    assert assigned == {"D1": "A", "D2": "A", "D3": "M"}


def test_매출_기여가_없는_차량은_순수익이_더_높아도_추천에서_제외한다(spark):
    """이슈 #955 — 회사 매출(리스료)이 늘지 않는 교체는 순수익이 더 좋아도 추천 안 함."""
    driver_metrics = spark.createDataFrame(
        [
            ("D1", "2026-01", "NYC", False, False, "T1", "A", "MAKE", "A", 2024, 30.0, 1000.0, 1000.0, 0.0, 100.0, 200.0, 700.0, 3000.0, 50.0, 1000.0, 1000.0, 1000.0, 4.0),
        ],
        [
            "driver_id", "year_month", "service_area", "comfort_eligible",
            "extra_comfort_eligible", "taxi_id", "vehicle_model_id", "manufacturer",
            "model_name", "model_year", "fuel_efficiency", "monthly_mileage",
            "monthly_driver_pay", "monthly_tips", "monthly_fuel_cost",
            "monthly_lease_fee", "monthly_net_profit", "_gas_price_miles", "_ev_price_miles",
            "_monthly_driver_pay_if_comfort", "_monthly_driver_pay_if_extra_comfort",
            "_monthly_driver_pay_if_both", "_lease_weeks_in_month",
        ],
    )
    inventory = spark.createDataFrame(
        [
            # B는 연비가 좋아 순수익은 A보다 높지만(700 -> 750), 리스료가 A와
            # 동일해 회사 매출 증가는 0이라 추천에서 제외되어야 한다.
            ("A", "MAKE", "A", 2024, "GAS", 30.0, False, False, 50.0, 2),
            ("B", "MAKE", "B", 2024, "GAS", 60.0, False, False, 50.0, 2),
        ],
        [
            "vehicle_model_id", "manufacturer", "model_name", "model_year",
            "fuel_type", "fuel_efficiency", "comfort_eligible",
            "extra_comfort_eligible", "weekly_lease_fee", "stock",
        ],
    )

    recommendation = ProfitFirstAlgorithm().recommend(driver_metrics, inventory)
    assigned = {row.driver_id: row.vehicle_model_id for row in recommendation.collect()}

    assert assigned == {"D1": "A"}


def test_v1과_v2를_함께_돌리면_job이_하는_것처럼_union하고_검증을_통과한다(spark):
    """job.py가 하는 것과 같은 조합 — ProfitFirstAlgorithm + RevenueFirstAlgorithm(threshold
    스윕)을 union한 결과가 validate_gold_business_invariants를 그대로 통과해야 한다."""
    from functools import reduce

    from pyspark.sql import DataFrame

    from main.spark.jobs.silver_to_gold.recommendation_algorithm import (
        RevenueFirstAlgorithm,
    )

    driver_metrics = spark.createDataFrame(
        [
            ("D1", "2026-01", "NYC", False, False, "T1", "A", "MAKE", "A", 2024, 30.0, 1000.0, 1000.0, 0.0, 100.0, 200.0, 700.0, 3000.0, 50.0, 1000.0, 1000.0, 1000.0, 4.0),
            ("D2", "2026-01", "NYC", False, False, "T2", "A", "MAKE", "A", 2024, 30.0, 1000.0, 1000.0, 0.0, 20.0, 200.0, 780.0, 600.0, 50.0, 1000.0, 1000.0, 1000.0, 4.0),
        ],
        [
            "driver_id", "year_month", "service_area", "comfort_eligible",
            "extra_comfort_eligible", "taxi_id", "vehicle_model_id", "manufacturer",
            "model_name", "model_year", "fuel_efficiency", "monthly_mileage",
            "monthly_driver_pay", "monthly_tips", "monthly_fuel_cost",
            "monthly_lease_fee", "monthly_net_profit", "_gas_price_miles", "_ev_price_miles",
            "_monthly_driver_pay_if_comfort", "_monthly_driver_pay_if_extra_comfort",
            "_monthly_driver_pay_if_both", "_lease_weeks_in_month",
        ],
    )
    inventory = spark.createDataFrame(
        [
            ("A", "MAKE", "A", 2024, "GAS", 30.0, False, False, 50.0, 2),
            ("B", "MAKE", "B", 2024, "GAS", 60.0, False, False, 50.0, 2),
        ],
        [
            "vehicle_model_id", "manufacturer", "model_name", "model_year",
            "fuel_type", "fuel_efficiency", "comfort_eligible",
            "extra_comfort_eligible", "weekly_lease_fee", "stock",
        ],
    )
    driver_snapshot = driver_metrics.select("driver_id", "taxi_id", "vehicle_model_id")
    driver_profit = gold_transformer.build_driver_monthly_profit(driver_metrics)

    thresholds = (50, 100)
    recommendation = reduce(
        DataFrame.unionByName,
        (
            algorithm.recommend(driver_metrics, inventory)
            for algorithm in (
                ProfitFirstAlgorithm(),
                RevenueFirstAlgorithm(thresholds=thresholds),
            )
        ),
    )

    gold_transformer.validate_gold_business_invariants(
        driver_profit, recommendation, driver_snapshot, inventory
    )

    rows = recommendation.collect()
    assert len(rows) == 2 * (1 + len(thresholds))  # 기사 2명 x (v1 1개 + v2 threshold 2개)
    assert {row["recommendation_algorithm_version_id"] for row in rows} == {1, 2}
    assert {row["threshold"] for row in rows} == {-1, 50, 100}


def test_현재보유량이_전체재고를_넘으면_실패한다(spark):
    snapshot = spark.createDataFrame(
        [("D1", "T1", "A", 400.0), ("D2", "T2", "A", 400.0)],
        ["driver_id", "taxi_id", "vehicle_model_id", "weekly_lease_fee"],
    )
    inventory = spark.createDataFrame(
        [("A", 30.0, 400.0, 1)],
        ["vehicle_model_id", "fuel_efficiency", "weekly_lease_fee", "stock"],
    )
    fuel_price = spark.createDataFrame(_full_month_fuel_price("2026-02"))

    with pytest.raises(ValueError, match="현재 운행 차량 수가 보유 재고를 초과"):
        gold_transformer._validate_dimensions(
            snapshot, inventory, fuel_price, "2026-02"
        )


def test_운행거리를_다섯개_구간으로_분류한다(spark):
    rows = spark.createDataFrame(
        [(0.1,), (1.99,), (2.0,), (4.99,), (5.0,), (9.99,), (10.0,), (19.99,), (20.0,)],
        ["trip_miles"],
    )

    bands = [
        row.distance_band
        for row in rows.select(
            gold_transformer._distance_band(F.col("trip_miles")).alias("distance_band")
        ).collect()
    ]

    assert bands == ["0-2", "0-2", "2-5", "2-5", "5-10", "5-10", "10-20", "10-20", "20+"]


def test_거리대별_실측_프리미엄_배수를_각_운행에_적용한다(spark):
    enriched = spark.createDataFrame(
        [
            ("short-standard", "HV0003", "Standard", 1.0, 5.0),
            ("short-comfort", "HV0003", "Comfort", 1.0, 10.0),
            ("long-standard", "HV0003", "Standard", 25.0, 50.0),
            ("long-comfort", "HV0003", "Comfort", 25.0, 75.0),
        ],
        ["row_id", "hvfhs_license_num", "estimated_service_tier", "trip_miles", "driver_pay"],
    )

    rows = {
        row.row_id: row._driver_pay_if_comfort
        for row in gold_transformer._with_tier_revenue_scenarios(enriched)
        .filter(F.col("estimated_service_tier") == "Standard")
        .select("row_id", "_driver_pay_if_comfort")
        .collect()
    }

    # 기대값을 상수(PREMIUM_TIER_TRIP_SHARE)로 직접 계산한다 — 값이 바뀔 때마다
    # 매번 다시 손으로 맞춰야 하는 하드코딩 숫자 대신, 배수 계산 로직 자체를 검증한다.
    share = gold_transformer.PREMIUM_TIER_TRIP_SHARE
    # short: 표준 5.0/mile, comfort 10.0/mile -> 배수 2.0
    assert rows["short-standard"] == pytest.approx(5.0 * (1 + share * 1.0))
    # long: 표준 2.0/mile, comfort 3.0/mile -> 배수 1.5
    assert rows["long-standard"] == pytest.approx(50.0 * (1 + share * 0.5))


@pytest.mark.parametrize(
    ("license_num", "premium_tier"),
    [("HV0003", "Comfort"), ("HV0005", "Extra Comfort")],
)
def test_Standard_운행_거리대에_프리미엄_표본이_없으면_실패한다(
    spark, license_num, premium_tier
):
    enriched = spark.createDataFrame(
        [
            (license_num, "Standard", 1.0, 5.0),
            (license_num, premium_tier, 25.0, 75.0),
        ],
        ["hvfhs_license_num", "estimated_service_tier", "trip_miles", "driver_pay"],
    )

    with pytest.raises(ValueError, match="프리미엄 배수.*결측"):
        gold_transformer._with_tier_revenue_scenarios(enriched)


# --- Gold 2종 적재 일관성 (#589, #927) ---------------------------------------
# 예전에는 `toPandas()` 와 CSV 쓰기가 한 루프에 섞여 최종 경로에 바로 썼습니다.
# 두 번째 산출물에서 죽으면 첫 파일만 이번 값으로 남는 문제를 막습니다.

def _frames(mark: str):
    import pandas as pd

    return {
        name: pd.DataFrame([{"year_month": "2026-01", "mark": mark}])
        for name in ("driver_aggregation", "driver_car_suggestion")
    }


def test_두_산출물이_지역경로에서_한꺼번에_교체된다(tmp_path):
    from main.spark.jobs.silver_to_gold.job import _write_all_csv

    written = _write_all_csv(_frames("first"), str(tmp_path), "2026-01", "NYC")

    assert set(written) == {"driver_aggregation", "driver_car_suggestion"}
    for path in written.values():
        assert "service_area=NYC" in str(path)
        assert path.read_text().count("first") == 1


def test_두_지역을_연달아_써도_서로의_CSV를_덮어쓰지_않는다(tmp_path):
    from main.spark.jobs.silver_to_gold.job import _write_all_csv

    nyc = _write_all_csv(_frames("nyc"), str(tmp_path), "2026-01", "NYC")
    tx = _write_all_csv(_frames("tx"), str(tmp_path), "2026-01", "TX")

    for dataset in nyc:
        assert nyc[dataset] != tx[dataset]
        assert nyc[dataset].read_text().count("nyc") == 1
        assert tx[dataset].read_text().count("tx") == 1


def test_쓰는_도중_실패하면_기존_산출물이_그대로_남는다(tmp_path, monkeypatch):
    """가장 현실적인 실패는 두 번째 산출물의 메모리 부족입니다."""
    import pandas as pd

    from main.spark.jobs.silver_to_gold import job

    job._write_all_csv(_frames("first"), str(tmp_path), "2026-01", "NYC")

    original = pd.DataFrame.to_csv
    calls = {"n": 0}

    def fail_on_second(self, path, *args, **kwargs):
        calls["n"] += 1
        if calls["n"] == 2:
            raise MemoryError("toPandas 상당 지점")
        return original(self, path, *args, **kwargs)

    monkeypatch.setattr(pd.DataFrame, "to_csv", fail_on_second)

    with pytest.raises(MemoryError):
        job._write_all_csv(_frames("second"), str(tmp_path), "2026-01", "NYC")

    # 둘 다 직전 실행 값이어야 합니다 — 하나라도 second 면 섞인 것입니다.
    for dataset in ("driver_aggregation", "driver_car_suggestion"):
        path = job._csv_path(str(tmp_path), dataset, "2026-01", "NYC")
        assert "second" not in path.read_text(), f"{dataset} 이 새 값으로 바뀌었습니다"


def test_실패해도_임시_파일을_남기지_않는다(tmp_path, monkeypatch):
    import pandas as pd

    from main.spark.jobs.silver_to_gold import job

    monkeypatch.setattr(
        pd.DataFrame, "to_csv", lambda self, path, *a, **k: (_ for _ in ()).throw(OSError("디스크"))
    )

    with pytest.raises(OSError):
        job._write_all_csv(_frames("x"), str(tmp_path), "2026-01", "NYC")

    assert not list(tmp_path.rglob("*.tmp"))


def test_Gold_job은_실행코드설정_식별자를_계보에_기록한다(monkeypatch):
    import pandas as pd

    from main.spark.jobs.silver_to_gold import job, postgres_loader

    class FakeFrame:
        def __init__(self, pandas_frame=None):
            self.pandas_frame = (
                pandas_frame if pandas_frame is not None else pd.DataFrame()
            )

        def persist(self):
            return self

        def unpersist(self):
            return None

        def toPandas(self):
            return self.pandas_frame.copy()

    class FakeReader:
        def parquet(self, path):
            return FakeFrame()

    class FakeSpark:
        read = FakeReader()

    aggregation = FakeFrame(pd.DataFrame([{"driver_id": "D1"}]))
    suggestion = FakeFrame(pd.DataFrame([
        {"driver_id": "D1", "recommendation_algorithm_version_id": 1, "threshold": -1},
        {"driver_id": "D1", "recommendation_algorithm_version_id": 2, "threshold": 100},
        {"driver_id": "D1", "recommendation_algorithm_version_id": 2, "threshold": 200},
    ]))

    class Profit:
        ALGORITHM_VERSION_ID = 1

        def recommend(self, driver_metrics, inventory):
            return suggestion

    class Revenue:
        ALGORITHM_VERSION_ID = 2

        def __init__(self, thresholds):
            self.thresholds = thresholds

        def recommend(self, driver_metrics, inventory):
            return suggestion

    captured = {}
    monkeypatch.setattr(job, "get_or_create_spark_session", lambda *a, **k: FakeSpark())
    monkeypatch.setattr(job, "enrich_trips_with_fuel_cost", lambda *a: FakeFrame())
    monkeypatch.setattr(job, "build_driver_monthly_aggregation", lambda *a: aggregation)
    monkeypatch.setattr(job, "reconcile_gold_control_totals", lambda *a: None)
    monkeypatch.setattr(job, "build_driver_monthly_profit", lambda frame: aggregation)
    monkeypatch.setattr(job, "ProfitFirstAlgorithm", Profit)
    monkeypatch.setattr(job, "RevenueFirstAlgorithm", Revenue)
    monkeypatch.setattr(job, "reduce", lambda function, frames: next(iter(frames)))
    monkeypatch.setattr(job, "validate_gold_business_invariants", lambda *a: None)
    monkeypatch.setattr(
        job,
        "write_gold_to_postgres",
        lambda frames, *args, **kwargs: captured.update(frames=frames) or {},
    )
    # 입력 내용 digest 는 파일을 실제로 읽습니다 — 경로별 고정값으로 대체(#1088).
    fake_paths = {
        "monthly_taxi_trip": "s3://lake/trips/v1.parquet",
        "driver_vehicle_monthly_snapshot": "s3://lake/drivers/v1.parquet",
        "lease_vehicle_inventory": "s3://lake/inventory/v1.parquet",
        "fuel_price": "s3://lake/fuel/v1.parquet",
    }
    fake_digests = {dataset: f"digest-{dataset}" for dataset in fake_paths}
    monkeypatch.setattr(
        job,
        "silver_input_digest",
        lambda path: fake_digests[
            next(key for key, value in fake_paths.items() if value == path)
        ],
    )

    paths = fake_paths
    job.main([
        "--env", "prod",
        "--gold_dsn", "postgresql://gold",
        "--year", "2026",
        "--month", "5",
        "--service_area", "NYC",
        "--monthly_taxi_trip_path", paths["monthly_taxi_trip"],
        "--driver_vehicle_monthly_snapshot_path", paths["driver_vehicle_monthly_snapshot"],
        "--lease_vehicle_inventory_path", paths["lease_vehicle_inventory"],
        "--fuel_price_path", paths["fuel_price"],
        "--thresholds", "[100, 200]",
        "--airflow_run_id", "scheduled__2026-05-01T00:00:00+00:00",
        "--code_sha", "abc1234",
    ])

    lineage = captured["frames"]["silver_lineage"].iloc[0]
    assert lineage["airflow_run_id"] == "scheduled__2026-05-01T00:00:00+00:00"
    assert lineage["code_sha"] == "abc1234"
    assert lineage["config_hash"] == postgres_loader.gold_config_hash(
        "NYC",
        "2026-05",
        lineage,
        [(1, -1), (2, 100), (2, 200)],
        input_digests=fake_digests,
        algorithm_constants_digest=gold_transformer.algorithm_constants_digest(),
    )
    assert len(lineage["config_hash"]) == 64


def _valid_gold_inputs(spark, year_month: str):
    """`enrich_trips_with_fuel_cost`의 다른 검증을 모두 통과하는 최소 입력 1행씩."""
    year, month = map(int, year_month.split("-"))
    trips = spark.createDataFrame([{
        "taxi_id": "T1",
        "hvfhs_license_num": "HV0003",
        "on_scene_datetime": datetime(year, month, 10, 7, 55),
        "pickup_datetime": datetime(year, month, 10, 8, 0),
        "dropoff_datetime": datetime(year, month, 10, 8, 30),
        "PULocationID": 1,
        "DOLocationID": 2,
        "pickup_zone": "Zone A",
        "dropoff_zone": "Zone B",
        "trip_miles": 5.0,
        "trip_time": 1800,
        "driver_pay": 20.0,
        "tips": 2.0,
        "estimated_service_tier": "Standard",
    }])
    driver_snapshot = spark.createDataFrame([{
        "snapshot_month": year_month,
        "driver_id": "D1",
        "taxi_id": "T1",
        "vehicle_model_id": "MODEL1",
        "manufacturer": "Kia",
        "model_name": "Forte",
        "fuel_type": "gasoline",
        "comfort_eligible": False,
        "extra_comfort_eligible": False,
        "weekly_lease_fee": 400.0,
        "join_date": date(year, 1, 1),
        "exit_date": date(2099, 12, 31),  # None이면 1행뿐인 DF에서 타입 추론이 안 됨
        "experience_years": 3,
        "vehicle_since": date(year, 1, 1),
        "snapshot_created_at": datetime(year, month, 1),
    }])
    inventory = spark.createDataFrame([{
        "vehicle_model_id": "MODEL1",
        "manufacturer": "Kia",
        "model_name": "Forte",
        "model_year": 2026,
        "fuel_type": "gasoline",
        "fuel_efficiency": 35.0,
        "comfort_eligible": False,
        "extra_comfort_eligible": False,
        "weekly_lease_fee": 400.0,
        "image_url": "http://example.com/forte.png",
        "stock": 3,
    }])
    return trips, driver_snapshot, inventory


def _full_month_fuel_price(year_month: str) -> list[dict]:
    """year_month 전체 날짜치 연료비 — `_validate_dimensions`의 일수 검증을 통과."""
    year, month = map(int, year_month.split("-"))
    days_in_month = monthrange(year, month)[1]
    return [
        {
            "date": date(year, month, day),
            "gas_price": 3.0 + day * 0.01,
            "ev_price": 0.2,
            "price_source": "eia",
            "bronze_collected_date": date(year, month, day),
            "ev_price_status": "Final",
        }
        for day in range(1, days_in_month + 1)
    ]


def test_연료비에_다른_달_데이터가_섞여도_대상_월만_걸러_정상_처리된다(spark):
    year_month = "2026-02"
    trips, driver_snapshot, inventory = _valid_gold_inputs(spark, year_month)
    # 연료비 Silver는 누적 파일이라 대상 월 외 데이터가 같이 들어올 수 있습니다.
    fuel_rows = (
        _full_month_fuel_price(year_month)
        + [{
            "date": date(2026, 1, 15),
            "gas_price": 999.0,
            "ev_price": 999.0,
            "price_source": "eia",
            "bronze_collected_date": date(2026, 1, 15),
            "ev_price_status": "Final",
        }]
    )
    fuel_price = spark.createDataFrame(fuel_rows)

    enriched = gold_transformer.enrich_trips_with_fuel_cost(
        trips, driver_snapshot, inventory, fuel_price, year_month
    )

    assert enriched.count() == 1
    assert enriched.first()["gas_price"] != 999.0


def test_연료비에_대상월_데이터가_전혀_없으면_명확히_실패한다(spark):
    year_month = "2026-02"
    trips, driver_snapshot, inventory = _valid_gold_inputs(spark, year_month)
    # 누적 파일이 아직 대상 월까지 안 쌓인 상황 — 앞뒤 달만 있음.
    fuel_rows = _full_month_fuel_price("2026-01") + _full_month_fuel_price("2026-03")
    fuel_price = spark.createDataFrame(fuel_rows)

    with pytest.raises(ValueError, match="연료비 Silver는"):
        gold_transformer.enrich_trips_with_fuel_cost(
            trips, driver_snapshot, inventory, fuel_price, year_month
        )


@pytest.mark.parametrize(
    ("broken", "message"),
    [
        ("trip_miles_nan", "거리 또는 기사 수익"),
        ("trip_driver_pay_nan", "거리 또는 기사 수익"),
        ("trip_tips_nan", "거리 또는 기사 수익"),
        ("inventory_fuel_efficiency_nan", "fuel_efficiency와 weekly_lease_fee"),
        ("inventory_lease_fee_inf", "fuel_efficiency와 weekly_lease_fee"),
        ("snapshot_lease_fee_nan", "스냅샷의 weekly_lease_fee"),
        ("gas_price_nan", "null이 아닌 유한한 값"),
        ("ev_price_null", "null이 아닌 유한한 값"),
    ],
)
def test_수치_차원에_비유한_값이_있으면_Gold_진입을_막는다(spark, broken, message):
    """NaN 은 null·음수·상한 비교를 전부 통과하므로 적재 전에 따로 거부한다 (#1080)."""
    year_month = "2026-02"
    trips, driver_snapshot, inventory = _valid_gold_inputs(spark, year_month)
    fuel_rows = _full_month_fuel_price(year_month)

    if broken == "trip_miles_nan":
        trips = trips.withColumn("trip_miles", F.lit(float("nan")))
    elif broken == "trip_driver_pay_nan":
        trips = trips.withColumn("driver_pay", F.lit(float("nan")))
    elif broken == "trip_tips_nan":
        trips = trips.withColumn("tips", F.lit(float("nan")))
    elif broken == "inventory_fuel_efficiency_nan":
        inventory = inventory.withColumn("fuel_efficiency", F.lit(float("nan")))
    elif broken == "inventory_lease_fee_inf":
        inventory = inventory.withColumn("weekly_lease_fee", F.lit(float("inf")))
    elif broken == "snapshot_lease_fee_nan":
        driver_snapshot = driver_snapshot.withColumn(
            "weekly_lease_fee", F.lit(float("nan"))
        )
    elif broken == "gas_price_nan":
        fuel_rows = [{**row, "gas_price": float("nan")} for row in fuel_rows]
    fuel_price = spark.createDataFrame(fuel_rows)
    if broken == "ev_price_null":
        # 전 행이 None 이면 타입 추론이 흔들리므로 double 로 명시해 채웁니다.
        fuel_price = fuel_price.withColumn("ev_price", F.lit(None).cast("double"))

    with pytest.raises(ValueError, match=message):
        gold_transformer.enrich_trips_with_fuel_cost(
            trips, driver_snapshot, inventory, fuel_price, year_month
        )
