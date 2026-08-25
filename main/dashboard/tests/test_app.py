"""Gold 차량 추천 대시보드 회귀 시나리오. 이슈 #562.

1. 월 파티션별 Gold CSV를 모두 로드 (`LocalCsvDataSource`, 이슈 #778로 이전)
2. 월간 리포트와 같은 순이익·회사 매출 기준을 통과한 기사만 표시
3. 기사·월 기준으로 현재 차량 비용과 최종 추천 차량 비용을 결합
"""

from pathlib import Path

import pandas as pd
import streamlit as st
from streamlit.testing.v1 import AppTest

from app import DEFAULT_THRESHOLD, recommendation_scope
from datasource import LocalCsvDataSource

APP_PATH = str(Path(__file__).resolve().parent.parent / "app.py")


def _write_partition(
    root: Path, dataset: str, service_area: str, year_month: str, rows: list[dict]
) -> None:
    partition = (
        root / dataset / f"service_area={service_area}" / f"year_month={year_month}"
    )
    partition.mkdir(parents=True)
    pd.DataFrame(rows).to_csv(partition / f"{dataset}.csv", index=False)


def _suggestion(
    driver_id: str, profit: float, revenue: float, service_area: str = "NYC"
) -> dict:
    return {
        "service_area": service_area,
        "driver_id": driver_id,
        "year_month": "2026-01",
        "recommendation_algorithm_version_id": 1,
        "threshold": -1,
        "manufacturer": "KIA",
        "model_name": "FORTE",
        "model_year": 2024,
        "recommendation_reason": "연료비 절감",
        "recommended_monthly_lease_fee": 2100.0,
        "expected_monthly_fuel_cost": 80.0,
        "expected_monthly_net_profit": 3000.0,
        "expected_net_profit_increase": profit,
        "expected_revenue_increase": revenue,
    }


def _aggregation(
    driver_id: str, year_month: str = "2026-01", service_area: str = "NYC"
) -> dict:
    return {
        "service_area": service_area,
        "driver_id": driver_id,
        "year_month": year_month,
        "manufacturer": "TOYOTA",
        "model_name": "CAMRY",
        "model_year": 2023,
        "monthly_lease_fee": 2200.0,
        "monthly_fuel_cost": 120.0,
        "monthly_net_profit": 2400.0,
        "monthly_mileage": 1000.0,
        "monthly_driver_pay": 4500.0,
        "monthly_tips": 220.0,
    }


def test_월별_Gold_파티션을_모두_읽는다(tmp_path):
    _write_partition(
        tmp_path,
        "driver_car_suggestion",
        "NYC",
        "2026-01",
        [_suggestion("D1", 700.0, 20.0)],
    )
    _write_partition(
        tmp_path,
        "driver_car_suggestion",
        "TX",
        "2026-02",
        [{**_suggestion("D2", 700.0, 20.0, "TX"), "year_month": "2026-02"}],
    )

    frame = LocalCsvDataSource(tmp_path).load("driver_car_suggestion")

    assert sorted(frame["year_month"]) == ["2026-01", "2026-02"]


def test_기본_하한값은_500달러다():
    assert DEFAULT_THRESHOLD == 500.0


def test_리포트_추천_기준을_통과한_기사만_표시한다():
    suggestion = pd.DataFrame(
        [
            _suggestion("eligible", 600.0, 1.0),
            _suggestion("no_company_gain", 700.0, 0.0),
            _suggestion("low_profit", 599.0, 10.0),
            _suggestion("company_loss", 700.0, -1.0),
        ]
    )
    aggregation = pd.DataFrame(
        [_aggregation(driver_id) for driver_id in suggestion["driver_id"]]
    )

    scope = recommendation_scope(
        suggestion, aggregation, "NYC", "2026-01", threshold=600.0
    )

    assert scope["driver_id"].tolist() == ["eligible"]


def test_기사와_월이_같은_현재_비용을_최종_추천에_붙인다():
    suggestion = pd.DataFrame([_suggestion("D1", 700.0, 20.0)])
    aggregation = pd.DataFrame(
        [
            _aggregation("D1", "2025-12"),
            _aggregation("D1", "2026-01"),
        ]
    )

    row = recommendation_scope(
        suggestion, aggregation, "NYC", "2026-01", threshold=600.0
    ).iloc[0]

    assert row["manufacturer"] == "KIA"
    assert row["current_manufacturer"] == "TOYOTA"
    assert row["recommended_monthly_lease_fee"] == 2100.0
    assert row["current_monthly_lease_fee"] == 2200.0
    assert row["expected_monthly_fuel_cost"] == 80.0
    assert row["current_monthly_fuel_cost"] == 120.0


def test_선택한_지역의_같은_기사만_현재차량과_결합한다():
    suggestion = pd.DataFrame([_suggestion("D1", 700.0, 20.0, "NYC")])
    aggregation = pd.DataFrame(
        [
            _aggregation("D1", service_area="NYC"),
            _aggregation("D1", service_area="TX"),
        ]
    )

    scope = recommendation_scope(
        suggestion, aggregation, "NYC", "2026-01", threshold=600.0
    )

    assert len(scope) == 1
    assert scope.iloc[0]["service_area"] == "NYC"


def test_render_전체_흐름이_gold_2종만으로_에러없이_동작한다(tmp_path, monkeypatch):
    """monthly_report 없이도 렌더가 끝까지 돈다 — 회귀 대상: #949."""
    monkeypatch.setenv("GOLD_DIR", str(tmp_path))
    monkeypatch.delenv("DASHBOARD_DATA_SOURCE", raising=False)

    _write_partition(
        tmp_path,
        "driver_car_suggestion",
        "NYC",
        "2026-01",
        [_suggestion("D1", 700.0, 20.0)],
    )
    _write_partition(
        tmp_path,
        "driver_aggregation",
        "NYC",
        "2026-01",
        [_aggregation("D1")],
    )

    # _data_source() 는 st.cache_resource라 GOLD_DIR이 달라도 프로세스 안에서
    # 첫 테스트가 만든 인스턴스를 계속 재사용한다 — 새 tmp_path를 실제로 읽게 비운다.
    st.cache_resource.clear()
    at = AppTest.from_file(APP_PATH)
    at.run()

    assert not at.exception
    assert not at.error


def test_알고리즘_버전별로_필터되고_설명과_silver_출처가_표시된다(tmp_path, monkeypatch):
    """이슈 #987 — 알고리즘 선택 박스 옆 설명, 맨 아래 접힌 Silver 출처."""
    monkeypatch.setenv("GOLD_DIR", str(tmp_path))
    monkeypatch.delenv("DASHBOARD_DATA_SOURCE", raising=False)

    _write_partition(
        tmp_path,
        "driver_car_suggestion",
        "NYC",
        "2026-01",
        [
            {**_suggestion("D1", 700.0, 20.0), "recommendation_algorithm_version_id": 1},
            {**_suggestion("D2", 900.0, 30.0), "recommendation_algorithm_version_id": 2},
        ],
    )
    _write_partition(
        tmp_path,
        "driver_aggregation",
        "NYC",
        "2026-01",
        [_aggregation("D1"), _aggregation("D2")],
    )
    _write_partition(
        tmp_path,
        "silver_lineage",
        "NYC",
        "2026-01",
        [{
            "service_area": "NYC",
            "year_month": "2026-01",
            "silver_monthly_taxi_trip_s3_link": "s3://bucket/silver/monthly_taxi_trip",
            "silver_driver_vehicle_monthly_snapshot_s3_link": "s3://bucket/silver/driver_vehicle_monthly_snapshot",
            "silver_lease_vehicle_inventory_s3_link": "s3://bucket/silver/lease_vehicle_inventory",
            "silver_gas_ev_price_s3_link": "s3://bucket/silver/gas_ev_price",
        }],
    )

    # load() 는 dataset 이름으로만 캐시 키를 잡고, _data_source() 는 인자 없이
    # 캐시되는 리소스라 GOLD_DIR이 달라도 이전 테스트 값이 샐 수 있어 둘 다 비운다.
    st.cache_data.clear()
    st.cache_resource.clear()
    at = AppTest.from_file(APP_PATH)
    at.run()

    assert not at.exception
    assert sorted(at.selectbox[0].options) == ["1", "2"]

    captions = [c.value for c in at.caption]
    assert any("s3://bucket/silver/monthly_taxi_trip" in c for c in captions)


def test_threshold가_sentinel뿐이면_콤보박스_대신_캡션만_보인다(tmp_path, monkeypatch):
    """이슈 #998 — threshold를 안 쓰는 알고리즘(v1)은 콤보박스를 안 띄운다."""
    monkeypatch.setenv("GOLD_DIR", str(tmp_path))
    monkeypatch.delenv("DASHBOARD_DATA_SOURCE", raising=False)

    _write_partition(
        tmp_path,
        "driver_car_suggestion",
        "NYC",
        "2026-01",
        [_suggestion("D1", 700.0, 20.0)],
    )
    _write_partition(
        tmp_path, "driver_aggregation", "NYC", "2026-01", [_aggregation("D1")]
    )

    st.cache_data.clear()
    st.cache_resource.clear()
    at = AppTest.from_file(APP_PATH)
    at.run()

    assert not at.exception
    # 알고리즘·지역·월 3개뿐 — threshold 콤보박스가 없다.
    assert len(at.selectbox) == 3
    captions = [c.value for c in at.caption]
    assert any("임계값을 쓰지 않습니다" in c for c in captions)


def test_threshold가_있으면_실제_값들로만_콤보박스를_구성한다(tmp_path, monkeypatch):
    """이슈 #998 — threshold를 쓰는 알고리즘(v2)은 실제 존재하는 값만 고를 수 있다."""
    monkeypatch.setenv("GOLD_DIR", str(tmp_path))
    monkeypatch.delenv("DASHBOARD_DATA_SOURCE", raising=False)

    _write_partition(
        tmp_path,
        "driver_car_suggestion",
        "NYC",
        "2026-01",
        [
            {**_suggestion("D1", 700.0, 20.0), "recommendation_algorithm_version_id": 2, "threshold": 100},
            {**_suggestion("D2", 900.0, 30.0), "recommendation_algorithm_version_id": 2, "threshold": 300},
        ],
    )
    _write_partition(
        tmp_path,
        "driver_aggregation",
        "NYC",
        "2026-01",
        [_aggregation("D1"), _aggregation("D2")],
    )

    st.cache_data.clear()
    st.cache_resource.clear()
    at = AppTest.from_file(APP_PATH)
    at.run()

    assert not at.exception
    # 알고리즘·threshold·지역·월 4개 — threshold 콤보박스가 실제 값(100, 300)만 담는다.
    assert len(at.selectbox) == 4
    assert sorted(at.selectbox[1].options) == ["$100", "$300"]
