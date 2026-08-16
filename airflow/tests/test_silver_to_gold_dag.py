"""Silver → Gold DAG 의 입력 해석과 산출물 검증 시나리오. 이슈 #413.

1. 대상 연월은 배정 파티션에서 고르되 기준일을 넘지 않음
2. 배정 파티션이 없으면 어느 DAG 를 돌려야 하는지 알려주며 실패
3. year/month 파라미터는 파티션 해석보다 우선
4. vehicle_master 는 대상 월 말일 이하 최신 파티션을 선택
5. 대상 월 이후에 수집된 vehicle_master 는 고르지 않음
5-1. 대상 월 이하가 하나도 없으면 가장 오래된 것으로 물러서되 경고를 남김
5-2. vehicle_master 파티션이 아예 없으면 실패
6. vehicle_master 도시가 여러 개면 조용히 고르지 않고 실패
7. 연료비 파일이 없으면 대상 월을 알려주며 실패
8. 정상 산출물 3종은 검증 통과
9. 산출물이 비었거나 필수 컬럼이 없거나 다른 연월이 섞이면 실패

Spark 잡은 부르지 않습니다. 경로 해석과 검증 함수만 파일을 실제로 놓고 확인합니다.
"""

from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

from dags import hvfhv_silver_to_gold_dag as dag_module


def _trip_partitions(root: Path, year_months: list[str]) -> Path:
    trips = root / "hvfhv_driver_trip"
    for year_month in year_months:
        (trips / f"year_month={year_month}").mkdir(parents=True)
    return trips


def _vehicle_master(root: Path, collected_dates: list[str], cities=("new-york",)) -> Path:
    dataset = root / "vehicle_master"
    for collected_date in collected_dates:
        for city in cities:
            partition = dataset / f"collected_date={collected_date}" / f"city={city}"
            partition.mkdir(parents=True)
            (partition / "vehicle_master.parquet").touch()
    return dataset


def _gas_ev_price(root: Path, months: list[str]) -> Path:
    dataset = root / "gas_ev_price"
    for month in months:
        partition = dataset / f"collected_month={month}"
        partition.mkdir(parents=True)
        (partition / "gas_ev_price.parquet").touch()
    return dataset


def _params(root: Path, **overrides) -> dict:
    params = {
        "trips_path": str(root / "hvfhv_driver_trip"),
        "vehicle_master_path": str(root / "vehicle_master"),
        "gas_ev_price_path": str(root / "gas_ev_price"),
        "year": None,
        "month": None,
    }
    params.update(overrides)
    return params


def _logical_date(year: int, month: int) -> datetime:
    return datetime(year, month, 13, tzinfo=timezone.utc)


def test_대상연월은_기준일_이하_최신_배정_파티션이다(tmp_path):
    trips = _trip_partitions(tmp_path, ["2026-03", "2026-05", "2026-09"])

    resolved = dag_module.resolve_target_year_month(
        _logical_date(2026, 6), _params(tmp_path), str(trips)
    )

    # 2026-09 는 기준일보다 미래라 제외 — 과거 날짜로 다시 돌려도 재현돼야 합니다.
    assert resolved == "2026-05"


def test_배정_파티션이_없으면_어느_DAG를_돌릴지_알려준다(tmp_path):
    trips = _trip_partitions(tmp_path, ["2026-09"])

    with pytest.raises(FileNotFoundError, match="hvfhv_driver_trip_silver_pipeline"):
        dag_module.resolve_target_year_month(
            _logical_date(2026, 6), _params(tmp_path), str(trips)
        )


def test_year_month_파라미터가_파티션_해석보다_우선한다(tmp_path):
    trips = _trip_partitions(tmp_path, ["2026-05"])

    resolved = dag_module.resolve_target_year_month(
        _logical_date(2026, 6), _params(tmp_path, year="2025", month="7"), str(trips)
    )

    assert resolved == "2025-07"


def test_vehicle_master는_대상월_말일_이하_최신을_고른다(tmp_path):
    dataset = _vehicle_master(tmp_path, ["2026-04-20", "2026-05-18", "2026-05-25"])

    resolved = dag_module.resolve_vehicle_master_file(dataset, date(2026, 5, 31))

    assert "collected_date=2026-05-25" in str(resolved)


def test_대상월_이후에_수집된_vehicle_master는_고르지_않는다(tmp_path):
    dataset = _vehicle_master(tmp_path, ["2026-05-18", "2026-08-15"])

    resolved = dag_module.resolve_vehicle_master_file(dataset, date(2026, 5, 31))

    # 8월 마스터를 5월 결과에 쓰면 그때 없던 차량이 섞입니다.
    assert "collected_date=2026-05-18" in str(resolved)


def test_대상월_이하가_하나도_없으면_가장_오래된것으로_물러서고_경고한다(tmp_path, caplog):
    """엄격히 막으면 만들 수 있는 달이 하나도 없습니다.

    마스터 수집은 2026-08 에 시작했고 TLC 는 두 달쯤 늦게 공개해서 "대상 월 >=
    마스터 수집일" 조합이 당분간 생기지 않습니다. 물러서되 흔적은 남겨야 합니다.
    """
    dataset = _vehicle_master(tmp_path, ["2026-08-15", "2026-09-20"])

    with caplog.at_level("WARNING"):
        resolved = dag_module.resolve_vehicle_master_file(dataset, date(2026, 5, 31))

    assert "collected_date=2026-08-15" in str(resolved)
    assert "그 시점에 없던 차량이 추천 후보에 섞일 수 있습니다" in caplog.text


def test_vehicle_master_파티션이_아예_없으면_실패한다(tmp_path):
    (tmp_path / "vehicle_master").mkdir()

    with pytest.raises(FileNotFoundError, match="vehicle_master_silver_pipeline"):
        dag_module.resolve_vehicle_master_file(tmp_path / "vehicle_master", date(2026, 5, 31))


def test_vehicle_master_도시가_여러개면_실패한다(tmp_path):
    dataset = _vehicle_master(tmp_path, ["2026-05-18"], cities=("new-york", "chicago"))

    with pytest.raises(ValueError, match="도시가 여러 개"):
        dag_module.resolve_vehicle_master_file(dataset, date(2026, 5, 31))


def test_연료비_파일이_없으면_대상월을_알려주며_실패한다(tmp_path):
    _trip_partitions(tmp_path, ["2026-05"])
    _vehicle_master(tmp_path, ["2026-05-18"])
    _gas_ev_price(tmp_path, ["2026-08"])

    with pytest.raises(FileNotFoundError, match="gas_ev_price_bronze_to_silver_pipeline"):
        dag_module.resolve_input_paths("2026-05", _params(tmp_path))


def test_입력_세개가_모두_있으면_경로를_확정한다(tmp_path):
    _trip_partitions(tmp_path, ["2026-05"])
    _vehicle_master(tmp_path, ["2026-05-18"])
    _gas_ev_price(tmp_path, ["2026-05"])

    resolved = dag_module.resolve_input_paths("2026-05", _params(tmp_path))

    assert resolved["year"] == "2026" and resolved["month"] == "5"
    assert resolved["trips_path"].endswith("year_month=2026-05")
    assert resolved["vehicle_master_path"].endswith("city=new-york/vehicle_master.parquet")
    assert resolved["gas_ev_price_path"].endswith("collected_month=2026-05/gas_ev_price.parquet")


def _write_gold(root: Path, year_month: str, **overrides) -> None:
    frames = {
        "driver_aggregation": pd.DataFrame([{
            "driver_id": "SD0001", "year_month": year_month,
            "monthly_net_profit": 100.0, "monthly_rental_fee": 400.0,
        }]),
        "driver_car_suggestion": pd.DataFrame([{
            "driver_id": "SD0001", "year_month": year_month,
            "recommended_make_key": "TOYOTA", "recommended_model_key": "CAMRY",
            "expected_net_profit_increase": 120.0, "recommendation_reason": "연비",
        }]),
        "monthly_report": pd.DataFrame([{
            "year_month": year_month, "threshold_profit_increase": 100.0,
            "recommended_driver_count": 1, "avg_net_profit_increase_per_driver": 120.0,
        }]),
    }
    frames.update(overrides)
    for dataset, frame in frames.items():
        path = root / dataset / f"year_month={year_month}" / f"{dataset}.csv"
        path.parent.mkdir(parents=True, exist_ok=True)
        frame.to_csv(path, index=False)


def test_정상_산출물_3종은_검증을_통과한다(tmp_path):
    _write_gold(tmp_path, "2026-05")

    dag_module.validate_gold_outputs(str(tmp_path), "2026-05")


@pytest.mark.parametrize(
    "violation, expected",
    [
        ("missing", "Gold 산출물이 없습니다"),
        ("empty", "비어 있습니다"),
        ("column", "필수 컬럼 누락"),
        ("year_month", "다른 연월이 섞였습니다"),
    ],
)
def test_산출물이_규칙을_어기면_실패한다(tmp_path, violation, expected):
    _write_gold(tmp_path, "2026-05")
    target = tmp_path / "monthly_report" / "year_month=2026-05" / "monthly_report.csv"

    if violation == "missing":
        target.unlink()
    elif violation == "empty":
        pd.read_csv(target).iloc[0:0].to_csv(target, index=False)
    elif violation == "column":
        pd.read_csv(target).drop(columns="recommended_driver_count").to_csv(target, index=False)
    else:
        frame = pd.read_csv(target)
        frame["year_month"] = "2026-04"
        frame.to_csv(target, index=False)

    with pytest.raises((FileNotFoundError, ValueError), match=expected):
        dag_module.validate_gold_outputs(str(tmp_path), "2026-05")
