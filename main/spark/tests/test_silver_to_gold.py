"""Silver → Gold 수익 시뮬레이션·재고 전달 시나리오. 이슈 #915.

1. 추천 후보는 실제 기사 수 × 실제 차량 모델 수를 보존
2. Silver 재고 업무 컬럼과 행은 Gold에서 그대로 보존
3. 프리미엄 배수는 5개 거리 구간별 실측값을 각 운행에 적용
4. Standard 운행 구간에 프리미엄 표본이 없으면 명시적으로 실패
5. 연료비에 대상 월 외 다른 월 데이터가 섞여 있어도 대상 월만 걸러 정상 처리
6. 연료비에 대상 월 데이터가 전혀 없으면 명확한 에러로 실패
"""

from calendar import monthrange
from datetime import date, datetime

import pytest
from pyspark.sql import functions as F

from main.spark.jobs.silver_to_gold import transformer as gold_transformer
from main.spark.jobs.silver_to_gold.transformer import (
    build_gold_lease_vehicle_inventory,
    build_monthly_vehicle_recommendation,
)
from shared.spark.common.session import get_or_create_spark_session


@pytest.fixture(scope="module")
def spark():
    session = get_or_create_spark_session("test_silver_to_gold_stock")
    yield session
    session.stop()


def test_추천후보는_실제기사수와_차량모델수의_곱이다(spark):
    driver_metrics = spark.createDataFrame(
        [
            ("D1", "2026-01", "NYC", False, False, "A", 1000.0, 10.0, 100.0, 200.0, 710.0, 100.0, 50.0, 1000.0, 1000.0, 1000.0, 4.0),
            ("D2", "2026-01", "NYC", False, False, "B", 900.0, 5.0, 90.0, 180.0, 635.0, 90.0, 45.0, 900.0, 900.0, 900.0, 4.0),
        ],
        [
            "driver_id", "year_month", "service_area", "comfort_eligible",
            "extra_comfort_eligible", "vehicle_model_id", "monthly_driver_pay",
            "monthly_tips", "monthly_fuel_cost", "monthly_lease_fee",
            "monthly_net_profit", "_gas_price_miles", "_ev_price_miles",
            "_monthly_driver_pay_if_comfort", "_monthly_driver_pay_if_extra_comfort",
            "_monthly_driver_pay_if_both", "_lease_weeks_in_month",
        ],
    )
    inventory = spark.createDataFrame(
        [
            ("A", "MAKE", "A", 2024, "GAS", 30.0, False, False, 50.0, 1),
            ("B", "MAKE", "B", 2024, "GAS", 25.0, False, False, 45.0, 1),
            ("C", "MAKE", "C", 2024, "EV", 100.0, True, True, 60.0, 0),
        ],
        [
            "vehicle_model_id", "manufacturer", "model_name", "model_year",
            "fuel_type", "fuel_efficiency", "comfort_eligible",
            "extra_comfort_eligible", "weekly_lease_fee", "stock",
        ],
    )

    candidates = build_monthly_vehicle_recommendation(driver_metrics, inventory)
    rows = candidates.collect()

    assert len(rows) == 2 * 3
    assert "candidate_stock" not in candidates.columns
    assert {(row.driver_id, row.candidate_vehicle_model_id) for row in rows} == {
        (driver_id, model_id)
        for driver_id in ("D1", "D2")
        for model_id in ("A", "B", "C")
    }


def test_시뮬레이션검증은_기사차량조합누락을_차단한다(spark):
    driver_profit = spark.createDataFrame([("D1",), ("D2",)], ["driver_id"])
    candidates = spark.createDataFrame(
        [("D1", "A"), ("D1", "B"), ("D2", "A")],
        ["driver_id", "candidate_vehicle_model_id"],
    )
    inventory = spark.createDataFrame([("A",), ("B",)], ["vehicle_model_id"])

    with pytest.raises(ValueError, match="추천 후보 수 불일치"):
        gold_transformer.validate_vehicle_profit_simulation(
            driver_profit, candidates, inventory
        )


def test_Silver_재고_업무컬럼과_행을_Gold에_그대로_전달한다(spark):
    silver_rows = [
        ("A", "MAKE", "MODEL", 2026, "GAS", 30.0, True, False, 500.0, "url", 3),
        ("B", "MAKE", "EV", 2025, "EV", 100.0, True, True, 600.0, "url2", 0),
    ]
    columns = [
        "vehicle_model_id", "manufacturer", "model_name", "model_year",
        "fuel_type", "fuel_efficiency", "comfort_eligible",
        "extra_comfort_eligible", "weekly_lease_fee", "image_url", "stock",
    ]
    inventory = spark.createDataFrame(silver_rows, columns)

    gold = build_gold_lease_vehicle_inventory(inventory, "2026-01", "NYC")

    assert [tuple(row[name] for name in columns) for row in gold.collect()] == silver_rows
    assert {row.year_month for row in gold.collect()} == {"2026-01"}
    assert {row.service_area for row in gold.collect()} == {"NYC"}


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

    assert rows["short-standard"] == pytest.approx(7.0)
    assert rows["long-standard"] == pytest.approx(60.0)


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


# --- Gold 3종 적재 일관성 (#589) ---------------------------------------------
# 예전에는 `toPandas()` 와 CSV 쓰기가 한 루프에 섞여 최종 경로에 바로 썼습니다.
# 두 번째 산출물에서 죽으면 첫 파일은 이번 값, 세 번째는 직전 실행 값이 남았고,
# 대시보드는 그 섞인 상태를 그대로 읽었습니다.

def _frames(mark: str):
    import pandas as pd

    return {
        name: pd.DataFrame([{"year_month": "2026-01", "mark": mark}])
        for name in (
            "driver_aggregation",
            "driver_vehicle_profit_simulation",
            "lease_vehicle_inventory",
        )
    }


def test_세_산출물이_지역경로에서_한꺼번에_교체된다(tmp_path):
    from main.spark.jobs.silver_to_gold.job import _write_all_csv

    written = _write_all_csv(_frames("first"), str(tmp_path), "2026-01", "NYC")

    assert set(written) == {
        "driver_aggregation",
        "driver_vehicle_profit_simulation",
        "lease_vehicle_inventory",
    }
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

    # 셋 다 직전 실행 값이어야 합니다 — 하나라도 second 면 섞인 것입니다.
    for dataset in (
        "driver_aggregation",
        "driver_vehicle_profit_simulation",
        "lease_vehicle_inventory",
    ):
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
