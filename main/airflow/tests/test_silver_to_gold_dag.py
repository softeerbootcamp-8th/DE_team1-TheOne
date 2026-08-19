"""Silver → Gold DAG의 현재 4입력 계약과 산출물 검증 시나리오.

1. 대상 연월은 기준일 이하의 최신 HVFHV 파티션이며 수동 파라미터가 우선
2. 같은 월 Silver 4종이 모두 있어야 입력 경로 확정
3. Gold 3종이 비었거나 필수 컬럼이 없거나 다른 연월이면 실패
"""

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

from main.airflow.scripts.hvfhv_silver_to_gold import tasks as dag_module


def _params(root: Path, **overrides) -> dict:
    params = {
        "hvfhv_path": str(root / "hvfhv"),
        "driver_snapshot_path": str(root / "driver_vehicle_monthly_snapshot"),
        "inventory_path": str(root / "lease_vehicle_inventory"),
        "fuel_price_path": str(root / "gas_ev_price"),
        "year": None,
        "month": None,
    }
    params.update(overrides)
    return params


def _logical_date(year: int, month: int) -> datetime:
    return datetime(year, month, 13, tzinfo=timezone.utc)


def _write_inputs(root: Path, year_month: str) -> None:
    hvfhv = root / "hvfhv" / f"year_month={year_month}"
    hvfhv.mkdir(parents=True)
    (hvfhv / "part-00000.parquet").touch()

    files = {
        "driver_vehicle_monthly_snapshot": "driver_vehicle_monthly_snapshot.parquet",
        "lease_vehicle_inventory": "lease_vehicle_inventory.parquet",
        "gas_ev_price": "gas_ev_price.parquet",
    }
    for dataset, file_name in files.items():
        partition = root / dataset / f"year_month={year_month}"
        partition.mkdir(parents=True)
        (partition / file_name).touch()


def test_대상연월은_기준일_이하_최신_HVFHV_파티션이다(tmp_path):
    for year_month in ("2026-03", "2026-05", "2026-09"):
        (tmp_path / "hvfhv" / f"year_month={year_month}").mkdir(parents=True)

    resolved = dag_module.resolve_target_year_month(
        _logical_date(2026, 6), _params(tmp_path), str(tmp_path / "hvfhv")
    )

    assert resolved == "2026-05"


def test_year_month_파라미터가_파티션보다_우선한다(tmp_path):
    resolved = dag_module.resolve_target_year_month(
        _logical_date(2026, 6),
        _params(tmp_path, year="2025", month="7"),
        str(tmp_path / "hvfhv"),
    )

    assert resolved == "2025-07"


def test_Silver_4종이_있으면_같은_월_경로를_확정한다(tmp_path):
    _write_inputs(tmp_path, "2026-05")

    resolved = dag_module.resolve_input_paths("2026-05", _params(tmp_path))

    assert resolved["year"] == "2026" and resolved["month"] == "5"
    assert resolved["hvfhv_path"].endswith("hvfhv/year_month=2026-05")
    assert resolved["driver_snapshot_path"].endswith(
        "year_month=2026-05/driver_vehicle_monthly_snapshot.parquet"
    )
    assert resolved["inventory_path"].endswith(
        "year_month=2026-05/lease_vehicle_inventory.parquet"
    )
    assert resolved["fuel_price_path"].endswith(
        "year_month=2026-05/gas_ev_price.parquet"
    )


def test_Silver_입력이_빠지면_상류_DAG를_알려준다(tmp_path):
    _write_inputs(tmp_path, "2026-05")
    (tmp_path / "gas_ev_price/year_month=2026-05/gas_ev_price.parquet").unlink()

    with pytest.raises(FileNotFoundError, match="eia_fuel_price_silver_pipeline"):
        dag_module.resolve_input_paths("2026-05", _params(tmp_path))


def _write_gold(root: Path, year_month: str) -> None:
    frames = {
        "driver_aggregation": pd.DataFrame(
            [{"driver_id": "D1", "year_month": year_month, "monthly_net_profit": 100.0, "monthly_lease_fee": 400.0}]
        ),
        "driver_car_suggestion": pd.DataFrame(
            [{"driver_id": "D1", "year_month": year_month, "vehicle_model_id": "MODEL1", "manufacturer": "KIA", "model_name": "FORTE", "expected_net_profit_increase": 120.0, "recommendation_reason": "연료비 절감"}]
        ),
        "monthly_report": pd.DataFrame(
            [{"year_month": year_month, "threshold_profit_increase": 100.0, "recommended_driver_count": 1, "avg_net_profit_increase_per_driver": 120.0}]
        ),
    }
    for dataset, frame in frames.items():
        path = root / dataset / f"year_month={year_month}" / f"{dataset}.csv"
        path.parent.mkdir(parents=True)
        frame.to_csv(path, index=False)


def test_정상_Gold_3종은_검증을_통과한다(tmp_path):
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
def test_Gold_산출물이_계약을_어기면_실패한다(tmp_path, violation, expected):
    _write_gold(tmp_path, "2026-05")
    target = tmp_path / "monthly_report/year_month=2026-05/monthly_report.csv"

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
