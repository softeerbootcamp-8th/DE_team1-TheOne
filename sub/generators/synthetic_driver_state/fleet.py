"""D5 재고 — 모델별 보유 대수와 개별 차량(taxi) 단위 확장.

`sub/prototype/synthesize.py::build_fleet_stock`/`expand_fleet_units` 를 그대로
옮겼습니다.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

# 리스팅은 대수를 주지 않아서 가정입니다. 그룹 안에서는 저렴한 차종에 더 많은
# 재고를 배분합니다. 재고 제약이 실제로 걸려야 D5 의
# "eligible pool → inventory constraint"가 장식이 아닙니다.
# ponytail: 총 재고 = 정원의 1.20배. 리스팅에 대수 컬럼이 생기면 그것을 쓰세요.
FLEET_OVERSUPPLY = 1.20

# 기사 배정 목표(BOTH 10%, SINGLE 10%, STANDARD 80%)를 먼저 계산한 뒤 그룹별
# 배율을 적용합니다. BOTH/SINGLE은 약 33.3%, STANDARD는 약 16.7%를 남깁니다.
VEHICLE_GROUP_SHARES = {"BOTH": 0.10, "SINGLE": 0.10, "STANDARD": 0.80}
VEHICLE_GROUP_OVERSUPPLY = {"BOTH": 1.50, "SINGLE": 1.50, "STANDARD": FLEET_OVERSUPPLY}


def apportion_counts(total: int, weights: dict[str, float]) -> dict[str, int]:
    """가중치 비율을 정수 합계 ``total``로 가장 큰 나머지 방식으로 배분합니다."""
    labels = sorted(weights)
    values = np.asarray([weights[label] for label in labels], dtype=float)
    values /= values.sum()
    raw = values * total
    counts = np.floor(raw).astype(int)
    missing = total - int(counts.sum())
    order = sorted(range(len(labels)), key=lambda i: (-(raw[i] - counts[i]), labels[i]))
    counts[order[:missing]] += 1
    return {label: int(counts[i]) for i, label in enumerate(labels)}


def build_fleet_stock(vehicle_master: pd.DataFrame, *, driver_count: int) -> pd.DataFrame:
    """Comfort 그룹은 정원의 1.5배, Standard는 1.2배 재고를 만듭니다."""
    stock = vehicle_master.copy()
    stock["unit_count"] = 0

    present_groups = sorted(stock["vehicle_group"].astype(str).unique())
    unknown = set(present_groups) - set(VEHICLE_GROUP_SHARES)
    if unknown:
        raise ValueError(f"알 수 없는 vehicle_group입니다: {sorted(unknown)}")
    group_driver_targets = apportion_counts(
        driver_count, {group: VEHICLE_GROUP_SHARES[group] for group in present_groups}
    )

    # 차종마다 최소 한 대는 있어야 모든 차종이 선택 후보에 남습니다. 소규모
    # fixture에서만 그룹 목표보다 이 최소 조건을 우선합니다.
    minimums = stock.groupby("vehicle_group").size().astype(int).to_dict()
    group_totals = {
        group: max(
            int(round(group_driver_targets[group] * VEHICLE_GROUP_OVERSUPPLY[group])),
            minimums[group],
        )
        for group in present_groups
    }

    for group in present_groups:
        indexes = stock.index[stock["vehicle_group"] == group].tolist()
        extra_total = group_totals[group] - len(indexes)
        model_weights = {
            str(stock.at[index, "vehicle_model_id"]): 1.0 / float(stock.at[index, "weekly_price_usd"])
            for index in indexes
        }
        extras = apportion_counts(extra_total, model_weights)
        for index in indexes:
            model_id = str(stock.at[index, "vehicle_model_id"])
            stock.at[index, "unit_count"] = 1 + extras[model_id]

    stock["unit_count"] = stock["unit_count"].astype(int)
    return stock


def expand_fleet_units(stock: pd.DataFrame) -> pd.DataFrame:
    """모델별 대수를 개별 차량(taxi) 한 대씩으로 펼칩니다.

    차량 개체가 있어야 제약 5(한 차량을 두 기사가 동시에 몰 수 없음)를 재고
    수준에서 지킬 수 있습니다. 모델 단위로만 두면 두 기사가 같은 차를 받습니다.
    """
    rows = []
    for record in stock.to_dict("records"):
        for serial in range(int(record["unit_count"])):
            unit = {k: v for k, v in record.items() if k != "unit_count"}
            unit["taxi_id"] = f"{record['vehicle_model_id']}#{serial:05d}"
            rows.append(unit)
    return pd.DataFrame(rows)
