"""합성 차량 배정 시나리오 (#962).

1. 무작위 배정은 가격을 바꿔도 결과가 같아 최저가를 우선하지 않음
2. 무작위 배정은 차종 수가 아니라 남은 개별 차량 수에 비례함
3. 기사 배정은 BOTH:SINGLE:STANDARD = 1:1:8이며 그룹별 재고를 남김
4. 같은 seed에서는 기사·재고 입력 순서와 무관하게 같은 차량을 배정함
"""

from __future__ import annotations

import pandas as pd

from sub.generators.synthetic_driver_state.assignment import assign_vehicles
from sub.generators.synthetic_driver_state.fleet import expand_fleet_units


def _vehicle_master(
    prices: list[float],
    groups: list[str] | None = None,
) -> pd.DataFrame:
    vehicle_groups = groups or ["STANDARD"] * len(prices)
    return pd.DataFrame([
        {
            "vehicle_model_id": f"MAKE{i}|MODEL{i}|2024",
            "weekly_price_usd": price,
            "vehicle_group": vehicle_groups[i],
        }
        for i, price in enumerate(prices)
    ])


def _drivers(count: int) -> pd.DataFrame:
    return pd.DataFrame({
        "driver_id": [f"DRIVER_{i:04d}" for i in range(count)],
        "tier_preference": [0.0] * count,
    })


def _units(
    model_counts: list[int],
    prices: list[float],
    groups: list[str] | None = None,
) -> pd.DataFrame:
    stock = _vehicle_master(prices, groups)
    stock["unit_count"] = model_counts
    return expand_fleet_units(stock)


def test_무작위배정은_가격을_바꿔도_결과가_같다():
    drivers = _drivers(60)
    original = _units([70, 20, 10], [500.0, 600.0, 750.0])
    price_swapped = original.copy()
    price_swapped["weekly_price_usd"] = price_swapped["weekly_price_usd"].map(
        {500.0: 750.0, 600.0: 600.0, 750.0: 500.0}
    )

    first = assign_vehicles(
        drivers, original,
        global_seed=42, target_month="2026-01", rationality=0.0,
    )
    second = assign_vehicles(
        drivers, price_swapped,
        global_seed=42, target_month="2026-01", rationality=0.0,
    )

    assert first == second


def test_무작위배정은_남은_개별차량수에_비례한다():
    assigned = assign_vehicles(
        _drivers(500), _units([900, 100], [500.0, 900.0]),
        global_seed=42, target_month="2026-01", rationality=0.0,
    )
    expensive_count = sum(taxi_id.startswith("MAKE1|") for taxi_id in assigned.values())

    assert 25 <= expensive_count <= 75


def test_기사배정은_BOTH_SINGLE_STANDARD_1_1_8이고_그룹별_재고를_남긴다():
    prices = [600.0, 800.0, 550.0, 750.0, 500.0, 700.0]
    groups = ["BOTH", "BOTH", "SINGLE", "SINGLE", "STANDARD", "STANDARD"]
    model_counts = [180, 120, 170, 130, 1_100, 820]
    master = _vehicle_master(prices, groups)
    units = _units(
        model_counts,
        prices,
        groups,
    )

    assigned = assign_vehicles(
        _drivers(2_000), units,
        global_seed=42, target_month="2026-01", rationality=0.0,
    )
    assigned_by_model = pd.Series(assigned).str.rsplit("#", n=1).str[0].value_counts()
    group_by_model = master.set_index("vehicle_model_id")["vehicle_group"]
    assigned_by_group = (
        assigned_by_model.rename_axis("vehicle_model_id").to_frame("assigned")
        .join(group_by_model)
        .groupby("vehicle_group")["assigned"]
        .sum()
        .to_dict()
    )

    assert assigned_by_group == {"BOTH": 200, "SINGLE": 200, "STANDARD": 1_600}
    initial_by_model = dict(zip(master["vehicle_model_id"], model_counts))
    for model_id, initial_stock in initial_by_model.items():
        remaining = initial_stock - int(assigned_by_model.get(model_id, 0))
        group = group_by_model.loc[model_id]
        lower, upper = (0.30, 0.35) if group in {"BOTH", "SINGLE"} else (0.15, 0.20)
        assert lower <= remaining / initial_stock <= upper


def test_같은_seed는_입력순서와_무관하게_같은차량을_배정한다():
    drivers = _drivers(60)
    units = _units([70, 20, 10], [500.0, 600.0, 750.0])

    first = assign_vehicles(
        drivers, units,
        global_seed=42, target_month="2026-01", rationality=0.0,
    )
    second = assign_vehicles(
        drivers.sample(frac=1.0, random_state=7),
        units.sample(frac=1.0, random_state=11),
        global_seed=42, target_month="2026-01", rationality=0.0,
    )

    assert first == second
