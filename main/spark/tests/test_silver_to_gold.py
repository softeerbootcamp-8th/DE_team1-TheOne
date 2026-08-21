"""Silver → Gold 차량 재고 배정 시나리오. 이슈 #561, #675.

1. 희소 차량은 예상 순이익 증가액이 큰 기사에게 우선 배정
2. 현재 차량 유지는 변경용 재고를 소비하지 않음
3. 재고에서 밀린 기사는 다음 순위 차량을 배정받음
4. 프리미엄 배수는 5개 거리 구간별 실측값을 각 운행에 적용
5. Standard 운행 구간에 프리미엄 표본이 없으면 명시적으로 실패
6. Gold 비즈니스 검증은 재고 초과·기사 누락·음수 수익 추천을 차단
7. 월간 리포트는 기사 순수익 기준과 회사 객단가 상승을 모두 만족한 기사만 집계
"""

import pytest
from pyspark.sql import functions as F

from main.spark.jobs.silver_to_gold import transformer as gold_transformer
from main.spark.jobs.silver_to_gold.transformer import (
    _allocate_candidates_by_stock,
    build_monthly_report,
)
from shared.spark.common.session import get_or_create_spark_session


@pytest.fixture(scope="module")
def spark():
    session = get_or_create_spark_session("test_silver_to_gold_stock")
    yield session
    session.stop()


def _candidate(
    driver_id: str,
    model_id: str,
    net_profit: float,
    stock: int,
    *,
    current: bool = False,
    revenue_increase: float = 0.0,
) -> dict:
    return {
        "driver_id": driver_id,
        "expected_monthly_net_profit": net_profit,
        "expected_net_profit_increase": net_profit - 100.0,
        "expected_revenue_increase": revenue_increase,
        "_is_current": current,
        "_candidate_model_year": 2026,
        "_candidate_vehicle_model_id": model_id,
        "_candidate_stock": stock,
    }


def test_재고_한대는_순이익_증가가_큰_기사에게_우선_배정한다(spark):
    candidates = spark.createDataFrame(
        [
            _candidate("high", "rare", 200.0, 1),
            _candidate("high", "high-current", 100.0, 1, current=True),
            _candidate("low", "rare", 150.0, 1, revenue_increase=20.0),
            _candidate("low", "low-current", 100.0, 1, current=True),
        ]
    )

    assigned = {
        row.driver_id: row._candidate_vehicle_model_id
        for row in _allocate_candidates_by_stock(candidates).collect()
    }

    assert assigned == {"high": "rare", "low": "low-current"}


def test_현재차량_유지자는_변경용_재고를_소비하지_않는다(spark):
    candidates = spark.createDataFrame(
        [
            _candidate("owner", "shared", 100.0, 1, current=True),
            _candidate("changer", "shared", 200.0, 1),
            _candidate("changer", "changer-current", 100.0, 1, current=True),
        ]
    )

    rows = _allocate_candidates_by_stock(candidates).collect()
    assigned = {row.driver_id: row._candidate_vehicle_model_id for row in rows}

    assert assigned == {"owner": "shared", "changer": "shared"}


def test_재고에서_밀린_기사는_차선_차량을_받는다(spark):
    candidates = spark.createDataFrame(
        [
            _candidate("high", "rare", 200.0, 1),
            _candidate("high", "high-current", 100.0, 1, current=True),
            _candidate("low", "rare", 180.0, 1),
            _candidate("low", "second", 160.0, 1),
            _candidate("low", "low-current", 100.0, 1, current=True),
        ]
    )

    rows = _allocate_candidates_by_stock(candidates).collect()
    assigned = {row.driver_id: row._candidate_vehicle_model_id for row in rows}

    assert assigned == {"high": "rare", "low": "second"}
    assert sum(row._candidate_vehicle_model_id == "rare" for row in rows) == 1


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


def _gold_frames(spark, *, stock=2, recommendation_ids=("D1", "D2"), profit_increase=10.0):
    driver_profit = spark.createDataFrame([("D1",), ("D2",)], ["driver_id"])
    recommendation = spark.createDataFrame(
        [
            (driver_id, "MODEL", profit_increase, "차량 변경")
            for driver_id in recommendation_ids
        ],
        [
            "driver_id",
            "vehicle_model_id",
            "expected_net_profit_increase",
            "recommendation_reason",
        ],
    )
    snapshot = spark.createDataFrame(
        [("D1", "CURRENT-D1"), ("D2", "CURRENT-D2")],
        ["driver_id", "vehicle_model_id"],
    )
    inventory = spark.createDataFrame([("MODEL", stock)], ["vehicle_model_id", "stock"])
    return driver_profit, recommendation, snapshot, inventory


def test_Gold_비즈니스검증은_모델별_재고초과를_차단한다(spark):
    frames = _gold_frames(spark, stock=1)

    with pytest.raises(ValueError, match="재고 초과"):
        gold_transformer.validate_gold_business_invariants(*frames)


def test_Gold_비즈니스검증은_현재차량_유지를_재고에서_제외한다(spark):
    driver_profit, recommendation, snapshot, inventory = _gold_frames(
        spark, stock=1
    )
    snapshot = snapshot.withColumn(
        "vehicle_model_id",
        F.when(F.col("driver_id") == "D1", F.lit("MODEL")).otherwise(
            F.col("vehicle_model_id")
        ),
    )

    gold_transformer.validate_gold_business_invariants(
        driver_profit, recommendation, snapshot, inventory
    )


def test_Gold_비즈니스검증은_기사누락을_차단한다(spark):
    frames = _gold_frames(spark, recommendation_ids=("D1",))

    with pytest.raises(ValueError, match="기사 수 불일치"):
        gold_transformer.validate_gold_business_invariants(*frames)


def test_Gold_비즈니스검증은_음수_예상순수익증가를_차단한다(spark):
    frames = _gold_frames(spark, profit_increase=-0.01)

    with pytest.raises(ValueError, match="예상 순수익 증가액이 음수"):
        gold_transformer.validate_gold_business_invariants(*frames)


def test_월간_리포트는_회사_객단가가_실제로_상승한_기사만_집계한다(spark):
    recommendation = spark.createDataFrame(
        [
            ("eligible", 600.0, 1.0),
            ("no_company_gain", 700.0, 0.0),
            ("low_profit", 599.0, 10.0),
            ("company_loss", 700.0, -1.0),
        ],
        ["driver_id", "expected_net_profit_increase", "expected_revenue_increase"],
    )

    report = build_monthly_report(recommendation, "2026-01", 600.0).first()

    assert report.recommended_driver_count == 1
    assert report.avg_net_profit_increase_per_driver == 600.0
    assert report.avg_revenue_increase_per_driver == 1.0
    assert report.total_revenue_increase == 1.0


# --- Gold 3종 적재 일관성 (#589) ---------------------------------------------
# 예전에는 `toPandas()` 와 CSV 쓰기가 한 루프에 섞여 최종 경로에 바로 썼습니다.
# 두 번째 산출물에서 죽으면 첫 파일은 이번 값, 세 번째는 직전 실행 값이 남았고,
# 대시보드는 그 섞인 상태를 그대로 읽었습니다.

def _frames(mark: str):
    import pandas as pd

    return {
        name: pd.DataFrame([{"year_month": "2026-01", "mark": mark}])
        for name in ("driver_aggregation", "driver_car_suggestion", "monthly_report")
    }


def test_세_산출물이_한꺼번에_교체된다(tmp_path):
    from main.spark.jobs.silver_to_gold.job import _write_all_csv

    written = _write_all_csv(_frames("first"), str(tmp_path), "2026-01")

    assert set(written) == {"driver_aggregation", "driver_car_suggestion", "monthly_report"}
    for path in written.values():
        assert path.read_text().count("first") == 1


def test_쓰는_도중_실패하면_기존_산출물이_그대로_남는다(tmp_path, monkeypatch):
    """가장 현실적인 실패는 두 번째 산출물의 메모리 부족입니다."""
    import pandas as pd

    from main.spark.jobs.silver_to_gold import job

    job._write_all_csv(_frames("first"), str(tmp_path), "2026-01")

    original = pd.DataFrame.to_csv
    calls = {"n": 0}

    def fail_on_second(self, path, *args, **kwargs):
        calls["n"] += 1
        if calls["n"] == 2:
            raise MemoryError("toPandas 상당 지점")
        return original(self, path, *args, **kwargs)

    monkeypatch.setattr(pd.DataFrame, "to_csv", fail_on_second)

    with pytest.raises(MemoryError):
        job._write_all_csv(_frames("second"), str(tmp_path), "2026-01")

    # 셋 다 직전 실행 값이어야 합니다 — 하나라도 second 면 섞인 것입니다.
    for dataset in ("driver_aggregation", "driver_car_suggestion", "monthly_report"):
        path = job._csv_path(str(tmp_path), dataset, "2026-01")
        assert "second" not in path.read_text(), f"{dataset} 이 새 값으로 바뀌었습니다"


def test_실패해도_임시_파일을_남기지_않는다(tmp_path, monkeypatch):
    import pandas as pd

    from main.spark.jobs.silver_to_gold import job

    monkeypatch.setattr(
        pd.DataFrame, "to_csv", lambda self, path, *a, **k: (_ for _ in ()).throw(OSError("디스크"))
    )

    with pytest.raises(OSError):
        job._write_all_csv(_frames("x"), str(tmp_path), "2026-01")

    assert not list(tmp_path.rglob("*.tmp"))
