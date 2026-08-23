"""D5 · D6 차량 배정.

순서를 지키는 것이 이 모듈의 전부입니다.

    eligible vehicle pool → inventory constraint → weak P(class | profile)

`rationality` 만큼만 비용 최적을 따릅니다. 1.0 으로 두면 배정이 정답을 그대로
써서 메인 프로덕트의 추천이 generator 규칙을 복원하는 자기충족이 됩니다 (D5).

재고는 **모델 단위**로 셉니다. 같은 모델의 개체는 비용이 동일해서 개체마다 비용을
다시 계산할 이유가 없습니다.

`sub/prototype/assign.py` 를 그대로 옮겼습니다.
"""

from __future__ import annotations

from collections import deque

import numpy as np
import pandas as pd

from sub.seeds import Stage, derive_entity_seed, derive_seed

# 프리미엄 선호가 이 값 이상인 기사는 자격 있는 차만 봅니다. 근거 없는 가정이고,
# 손잡이가 하나 더 늘어나는 걸 막으려고 중앙값으로 둡니다.
PREMIUM_SEEKER_THRESHOLD = 0.5


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
    is_premium = (models["vehicle_group"] != "STANDARD").to_numpy()

    stage_seed = derive_seed(global_seed, Stage.VEHICLE_ASSIGNMENT, target_month)
    assigned: dict[str, str] = {}
    # 기사 순서가 재고 경쟁 결과를 정하므로 driver_id 정렬로 고정합니다.
    for record in drivers.sort_values("driver_id").to_dict("records"):
        in_stock = remaining > 0
        if not in_stock.any():
            break
        driver_id = record["driver_id"]
        rng = np.random.default_rng(derive_entity_seed(stage_seed, driver_id))

        # 1) eligible pool
        eligible = in_stock
        if record["tier_preference"] >= PREMIUM_SEEKER_THRESHOLD and (in_stock & is_premium).any():
            eligible = in_stock & is_premium

        # 2) inventory constraint 는 위 `in_stock` 이 곧 그것입니다.
        # 3) weak P(class | profile)
        indexes = np.flatnonzero(eligible)
        if rng.random() < rationality:
            cost = model_weekly_cost(models.iloc[indexes])
            chosen = int(indexes[int(np.argmin(cost))])
        else:
            chosen = int(indexes[int(rng.integers(0, len(indexes)))])

        model_id = str(models.at[chosen, "vehicle_model_id"])
        assigned[driver_id] = queues[model_id].popleft()
        remaining[chosen] -= 1
    return assigned
