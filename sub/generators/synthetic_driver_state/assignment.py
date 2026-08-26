"""D5 · D6 차량 배정.

순서를 지키는 것이 이 모듈의 전부입니다.

    vehicle-group quota → inventory reserve → random model within group

기본 설정은 비용 최적을 따르지 않고 남은 개별 차량 중 하나를 무작위로 고릅니다.
`rationality`는 과거 실험 재현을 위해 남겨 두되 기본값은 0.0입니다.

재고는 **모델 단위**로 세고, 기사는 BOTH:SINGLE:STANDARD=1:1:8로 나눕니다.
같은 그룹 안에서는 남은 배정 가능 대수에 비례해 무작위로 차종을 고릅니다.

`sub/prototype/assign.py` 를 그대로 옮겼습니다.
"""

from __future__ import annotations

from collections import deque

import numpy as np
import pandas as pd

from sub.generators.synthetic_driver_state.fleet import (
    VEHICLE_GROUP_SHARES,
    apportion_counts,
)
from sub.seeds import Stage, derive_entity_seed, derive_seed

def model_weekly_cost(models: pd.DataFrame) -> np.ndarray:
    """모델별 주당 총비용 = 렌트비.

    ★ 이 값은 메인 프로덕트의 추천이 **찾아내야 하는 정답**입니다 (D5). 배정이
      이것을 100% 따르면 추천의 효과가 0 으로 측정됩니다.

    연료비는 넣지 않습니다 — 기사 비용·기대수익 계산은 main(Gold)의 몫이고,
    거기서 이미 같은 연료비 Silver로 실제 순수익을 계산합니다. sub가 main
    소유 데이터셋을 직접 읽는 건 두 파이프라인의 경계(README 4-4) 위반입니다(#744).
    """
    return models["weekly_price_usd"].to_numpy(dtype=float)


def assign_vehicles(
    drivers: pd.DataFrame,
    available_units: pd.DataFrame,
    *,
    global_seed: int,
    target_month: str,
    rationality: float,
) -> dict[str, str]:
    """기사에게 차량 개체 한 대씩. 재고를 소진하며 결정적으로 배정합니다."""
    if drivers.empty or available_units.empty:
        return {}

    # 모델 단위 재고 + 개체 대기열. 대기열 순서가 결정적이어야 재현됩니다.
    models = (
        available_units.groupby("vehicle_model_id", as_index=False)
        .agg(
            weekly_price_usd=("weekly_price_usd", "first"),
            vehicle_group=("vehicle_group", "first"),
        )
        .sort_values("vehicle_model_id")
        .reset_index(drop=True)
    )
    queues = {
        model_id: deque(sorted(group["taxi_id"].astype(str)))
        for model_id, group in available_units.groupby("vehicle_model_id")
    }
    remaining = np.asarray([len(queues[m]) for m in models["vehicle_model_id"]], dtype=int)
    stage_seed = derive_seed(global_seed, Stage.VEHICLE_ASSIGNMENT, target_month)
    driver_ids = sorted(drivers["driver_id"].astype(str))
    present_groups = sorted(models["vehicle_group"].astype(str).unique())
    group_targets = apportion_counts(
        len(driver_ids), {group: VEHICLE_GROUP_SHARES[group] for group in present_groups}
    )
    group_labels = np.asarray([
        group
        for group in sorted(group_targets)
        for _ in range(group_targets[group])
    ], dtype=object)
    np.random.default_rng(stage_seed).shuffle(group_labels)
    driver_group = dict(zip(driver_ids, group_labels))

    # 그룹별 총 재고와 배정 목표의 차이가 곧 남길 재고입니다. 이를 모델의 초기
    # 재고 비율로 나누면 BOTH/SINGLE은 약 33.3%, STANDARD는 약 16.7%를 남깁니다.
    reserved = np.zeros(len(models), dtype=int)
    model_groups = models["vehicle_group"].to_numpy()
    for group in present_groups:
        indexes = np.flatnonzero(model_groups == group)
        reserve_total = max(0, int(remaining[indexes].sum()) - group_targets[group])
        reserve_by_model = apportion_counts(
            reserve_total,
            {
                str(models.at[index, "vehicle_model_id"]): float(remaining[index])
                for index in indexes
            },
        )
        for index in indexes:
            reserved[index] = reserve_by_model[str(models.at[index, "vehicle_model_id"])]

    assigned: dict[str, str] = {}
    # 기사 순서가 재고 경쟁 결과를 정하므로 driver_id 정렬로 고정합니다.
    for record in drivers.sort_values("driver_id").to_dict("records"):
        assignable = remaining > reserved
        if not assignable.any():
            break
        driver_id = record["driver_id"]
        rng = np.random.default_rng(derive_entity_seed(stage_seed, driver_id))

        # 1) 그룹 비율에 따라 먼저 정한 eligible pool. 그룹 안의 차종만 무작위입니다.
        target_group = driver_group[driver_id]
        eligible = assignable & (models["vehicle_group"].to_numpy() == target_group)
        if not eligible.any():
            continue

        # 2) inventory constraint 는 위 `assignable` 이 곧 그것입니다.
        # 3) weak P(class | profile)
        indexes = np.flatnonzero(eligible)
        if rng.random() < rationality:
            cost = model_weekly_cost(models.iloc[indexes])
            chosen = int(indexes[int(np.argmin(cost))])
        else:
            # 차종을 균등 추첨하면 재고가 적은 고가 차종도 초반에 같은 확률로 뽑혀
            # 조기 소진됩니다. 남은 대수를 가중치로 쓰면 개별 차량 한 대마다 선택
            # 확률이 같고, 가격은 선택 확률에 직접 개입하지 않습니다.
            assignable_count = remaining[indexes] - reserved[indexes]
            probability = assignable_count / assignable_count.sum()
            chosen = int(rng.choice(indexes, p=probability))

        model_id = str(models.at[chosen, "vehicle_model_id"])
        assigned[driver_id] = queues[model_id].popleft()
        remaining[chosen] -= 1
    return assigned
