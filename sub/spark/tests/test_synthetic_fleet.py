"""합성 차량 재고 생성 시나리오 (#962)."""

from __future__ import annotations

import pandas as pd

from sub.generators.synthetic_driver_state.fleet import build_fleet_stock


def _vehicle_master(prices: list[float], groups: list[str]) -> pd.DataFrame:
    return pd.DataFrame([
        {
            "vehicle_model_id": f"MAKE{i}|MODEL{i}|2024",
            "weekly_price_usd": price,
            "vehicle_group": groups[i],
        }
        for i, price in enumerate(prices)
    ])


def test_STANDARD_재고는_배정인원의_1_2배이고_저가차량이_더_많다():
    master = _vehicle_master([500.0, 600.0, 750.0], ["STANDARD"] * 3)
    stock = build_fleet_stock(master, driver_count=1_000)
    units = stock.set_index("vehicle_model_id")["unit_count"]

    assert units.sum() == 1_200
    assert units["MAKE0|MODEL0|2024"] > units["MAKE1|MODEL1|2024"]
    assert units["MAKE1|MODEL1|2024"] > units["MAKE2|MODEL2|2024"]


def test_BOTH_SINGLE_재고는_배정인원의_1_5배다():
    master = _vehicle_master(
        [600.0, 800.0, 550.0, 750.0, 500.0, 700.0],
        ["BOTH", "BOTH", "SINGLE", "SINGLE", "STANDARD", "STANDARD"],
    )

    stock = build_fleet_stock(master, driver_count=2_000)

    assert stock.groupby("vehicle_group")["unit_count"].sum().to_dict() == {
        "BOTH": 300,
        "SINGLE": 300,
        "STANDARD": 1_920,
    }
