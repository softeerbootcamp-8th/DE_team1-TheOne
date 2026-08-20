"""D7 기사 성향 2층 구조.

(A) 기사 고유 기준값 — 가입 시 1회 결정, 영구 불변. `(global_seed, driver_id,
traits_pool_month)` 의 순수 함수이고 월을 시드에 넣지 않습니다.

(B) 월별 실현값 — 기준값 주변에서 변동. 월을 시드에 넣고, 자기상관은 시드가
아니라 전월 상태(`previous_noise`)로 연쇄합니다.

`sub/prototype/synthesize.py` 의 같은 이름 함수를 그대로 옮겼습니다.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from sub.config import GenerationConfig
from sub.seeds import Stage, derive_entity_seed, derive_seed

# 요일·시간대 비중, 거리 tertile 은 실측 상수입니다 (D13). 기존 Spark 경로가
# 소유하고 있어 거기서 가져옵니다 — 두 곳에 두면 갈립니다.
from sub.spark.jobs.driver_master.traits import (
    DISTANCE_LABELS,
    DISTANCE_MEDIUM_MAX_MI,
    DISTANCE_SHORT_MAX_MI,
    FIRST_NAMES,
    LAST_NAMES,
    TIME_BLOCK_WEIGHTS,
    WEEKDAY_WEIGHTS,
)

# 선호 시간대는 **연속** 블록입니다. 떨어진 두 블록을 주면 그 사이가 통째로
# 버려집니다 (기존 preference.py 주석).
#
# 개수는 4 입니다(= 12시간 창). 3 이었을 때 픽업 창의 최대가 정확히 9.00시간이
# 되고 그것을 넘는 기사-일이 0% 였습니다 — 하루 운행 예산을 8~12시간으로 줘도
# 창이 9시간이면 상한이 닿을 수 없는 장식입니다. 실측(2026-01, bucket 64)에서
# 기사-일의 67%가 이미 창의 8시간 이상을 쓰고 있었고, 후보 밀도를 3.2배 늘려도
# 운행시간은 8%만 올랐습니다. 그 천장이 이 상수였습니다.
PREFERRED_BLOCK_RUN = 4

# 가정 파라미터 (D13 — 아직 config 미이관, I6).
# 기존 traits.py 의 gamma(6.0, 1.2) 는 **하루** 근무시간(평균 7.2h)입니다. 주 단위
# 기준값으로 쓰려면 활동일수를 곱해야 해서, 여기서는 평균 5일을 곱해 둡니다.
BASE_WEEKLY_HOURS_SHAPE = 6.0
BASE_WEEKLY_HOURS_SCALE = 1.32
BASE_WEEKLY_DAYS = 5.0
# 목표 트립 수의 sanity 범위. **계산식이 정하고 이 값은 막기만 해야 합니다** —
# clip 이 자주 물면 목표가 계산이 아니라 이 상수로 정해지고 있다는 뜻입니다.
#
# 하한의 근거는 경제성입니다. 2026-01 실측에서 기사 수령액 중앙값이 트립당
# $15.29, 트립 거리 중앙값 2.78mi 이고 연비 30.5mpg·휘발유 $4.15/gal 이면
# 트립당 순수익이 $14.91 입니다. 렌트비($514~749/주)만 내려면 주 34~50건,
# 활동 5일 기준 **하루 7~10건**이 필요합니다. 그 아래는 차를 반납하는
# 기사이므로 명부에 남아 있을 수 없습니다.
#
# 상한은 물리적 한계입니다. 12시간 근무 상한에 트립 하나가 최소 승객시간 +
# 공차를 먹으므로 하루 40건이 사실상 천장입니다.
MIN_DAILY_TRIPS_RANGE = (7, 11)
MAX_DAILY_TRIPS_RANGE = (30, 41)
# 기사가 감수하는 공차 한도. 실측(2026-01, 4블록·bucket 20)에서 실제 공차는 평균
# 7.24분인데 한도 평균이 17.46분이었고, 그래도 `c4b_deadhead_over_limit` 이 8,853만
# 건으로 순차 탈락 1위였습니다. 한도를 올려서 늘어나는 것은 실제 공차 시간이
# 아니라 "이어 받을 수 있는 다음 트립의 범위"입니다.
DEADHEAD_RANGE = (15, 36)

# 하루 **운행시간**(승객 태운 시간 + 공차)의 기사별 하한·상한.
#
# 목표를 트립 수가 아니라 시간으로 잡습니다. 트립 수는 운행시간을 "트립 하나가
# 먹는 시간"으로 나눈 결과이고, 나누는 값(특히 공차 기대값)이 배정 로직에 딸려
# 움직입니다. 그래서 배정을 고칠 때마다 목표가 같이 흔들렸습니다. 시간은 배정
# 결과에서 그대로 측정되는 물리량이라 그 순환이 없습니다.
#
# 하한 4~8시간: 렌트비를 주 단위로 내는 기사가 차를 들고 나온 날 그 아래로
# 도는 것은 경제적으로 성립하지 않습니다. 상한 8~12시간: 12시간이 물리적
# 천장이고(휴식·유휴 제외), 기사마다 다릅니다.
MIN_DRIVE_MINUTES_RANGE = (240, 481)   # 4~8시간
MAX_DRIVE_MINUTES_RANGE = (480, 721)   # 8~12시간
BUFFER_SECONDS_RANGE = (60, 181)
ACTIVE_DAYS_CHOICES = ([3, 4, 5, 6, 7], [0.15, 0.20, 0.25, 0.25, 0.15])
# D7 의 (B) 클리핑 범위. 기준값의 30%~200% 밖으로는 나가지 않습니다.
REALIZATION_CLIP = (0.30, 2.00)


def distance_band(miles: float) -> str:
    if miles <= DISTANCE_SHORT_MAX_MI:
        return DISTANCE_LABELS[0]
    if miles <= DISTANCE_MEDIUM_MAX_MI:
        return DISTANCE_LABELS[1]
    return DISTANCE_LABELS[2]


def base_traits(
    driver_id: str,
    *,
    global_seed: int,
    traits_pool_month: str,
    trip_pool: dict[str, np.ndarray],
) -> dict:
    """`(global_seed, driver_id, traits_pool_month)` 의 순수 함수 (D7 A, D8).

    `traits_pool_month` 는 기사의 **가입 시점** 월입니다. 대상 월이 아닙니다 —
    2024-01 에 가입한 기사는 2026-08 을 처리할 때도 2024-01 풀에서 뽑은 성향을
    그대로 씁니다. 그래서 재생성 순서와 무관하게 같은 값이 나옵니다.
    """
    stage_seed = derive_seed(global_seed, Stage.DRIVER_TRAITS)
    rng = np.random.default_rng(derive_entity_seed(stage_seed, driver_id, traits_pool_month))

    base_weekly_hours = float(
        rng.gamma(BASE_WEEKLY_HOURS_SHAPE, BASE_WEEKLY_HOURS_SCALE) * BASE_WEEKLY_DAYS
    )
    volatility = float(rng.uniform(0.30, 0.40))
    active_days = int(rng.choice(ACTIVE_DAYS_CHOICES[0], p=ACTIVE_DAYS_CHOICES[1]))
    weekday_p = WEEKDAY_WEIGHTS / WEEKDAY_WEIGHTS.sum()
    active_weekdays = sorted(rng.choice(7, size=active_days, replace=False, p=weekday_p).tolist())

    # 실측 풀에서 뽑습니다. 풀은 가입 시점 월의 것이므로 재현됩니다 (D8).
    distance_pref_mi = float(rng.choice(trip_pool["trip_miles"]))
    avg_trip_duration_min = max(1.0, float(rng.choice(trip_pool["trip_time_min"])))

    time_weights = rng.dirichlet(alpha=8.0 * TIME_BLOCK_WEIGHTS)
    starts = range(len(time_weights) - PREFERRED_BLOCK_RUN + 1)
    best = max(starts, key=lambda s: time_weights[s:s + PREFERRED_BLOCK_RUN].sum())

    return {
        "driver_id": driver_id,
        "driver_name": f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}",
        "traits_pool_month": traits_pool_month,
        "base_weekly_hours": base_weekly_hours,
        "volatility": volatility,
        "active_weekdays": active_weekdays,
        "distance_pref_mi": distance_pref_mi,
        "avg_trip_duration_min": avg_trip_duration_min,
        "time_block_weights": time_weights.tolist(),
        "preferred_time_blocks": list(range(best, best + PREFERRED_BLOCK_RUN)),
        "preferred_distance_band": distance_band(distance_pref_mi),
        "airport_preference": float(rng.beta(2.0, 5.0)),
        "manhattan_preference": float(rng.beta(2.5, 2.5)),
        "tier_preference": float(rng.beta(5.0, 2.0)),
        "max_deadhead_minutes": int(rng.integers(*DEADHEAD_RANGE)),
        "buffer_seconds": int(rng.integers(*BUFFER_SECONDS_RANGE)),
        "min_daily_trips": int(rng.integers(*MIN_DAILY_TRIPS_RANGE)),
        "max_daily_trips": int(rng.integers(*MAX_DAILY_TRIPS_RANGE)),
        # ↓ 아래 두 개는 **맨 끝에** 둡니다. 앞에 끼우면 `rng.integers` 가 범위마다
        #   소비하는 난수량이 달라서 뒤쪽 draw 가 전부 밀리고, 요일·시간대·거리
        #   선호가 바뀌어 이전 실행과 비교가 불가능해집니다.
        "rest_frac": float(rng.uniform(0.05, 0.15)),
        "idle_frac": float(rng.uniform(0.15, 0.35)),
        # 하루 운행시간의 하한·상한. 위와 같은 이유로 **여기 맨 끝에** 붙입니다.
        "min_drive_minutes": int(rng.integers(*MIN_DRIVE_MINUTES_RANGE)),
        "max_drive_minutes": int(rng.integers(*MAX_DRIVE_MINUTES_RANGE)),
    }


def realize_month(
    traits: pd.DataFrame,
    previous_noise: pd.DataFrame | None,
    *,
    global_seed: int,
    target_month: str,
    config: GenerationConfig,
) -> tuple[pd.DataFrame, pd.DataFrame, float]:
    """기준값 주변의 그 달 실현값 (D7 B).

    `noise(m) = phi*noise(m-1) + shock + seasonal` 입니다. `noise(m-1)` 은 난수가
    아니라 **전월 상태를 읽는 것**입니다. 그래서 시드로 다루지 않고 `previous_noise`
    로 받습니다 — 시드로 만들면 백필로 과거 달만 다시 돌릴 때 연쇄가 끊깁니다.
    """
    phi = config.synthesize.noise_phi
    amplitude = config.synthesize.seasonal_amplitude
    volatility_scale = config.synthesize.traits_volatility

    # 계절 요인은 **전 기사 공통 방향**이라 기사 단위가 아니라 월 단위 단일 draw 입니다.
    seasonal_rng = np.random.default_rng(
        derive_seed(global_seed, Stage.SEASONAL_FACTOR, target_month)
    )
    seasonal = float(seasonal_rng.normal(0.0, amplitude))

    stage_seed = derive_seed(global_seed, Stage.MONTHLY_REALIZATION, target_month)
    prior = (
        dict(zip(previous_noise["driver_id"], previous_noise["noise"]))
        if previous_noise is not None and not previous_noise.empty
        else {}
    )

    noise_rows: list[dict] = []
    factors: list[float] = []
    clipped = 0
    for driver_id, volatility in zip(traits["driver_id"], traits["volatility"]):
        rng = np.random.default_rng(derive_entity_seed(stage_seed, driver_id))
        shock = float(rng.normal(0.0, volatility * volatility_scale))
        noise = phi * float(prior.get(driver_id, 0.0)) + shock + seasonal
        factor = 1.0 + noise
        low, high = REALIZATION_CLIP
        if not low <= factor <= high:
            clipped += 1
            factor = min(max(factor, low), high)
        noise_rows.append({"driver_id": driver_id, "noise": noise})
        factors.append(factor)

    realized = traits.copy()
    realized["realization_factor"] = factors
    realized["weekly_hours"] = realized["base_weekly_hours"] * realized["realization_factor"]
    # 근무 시간이 늘었으면 운행시간도 늘어야 합니다 (D7 "연동 필수").
    active_days = realized["active_weekdays"].map(len).clip(lower=1)
    daily_minutes = realized["weekly_hours"] * 60.0 / active_days

    # 하루 운행분 예산. 기사별 하한(4~8h)·상한(8~12h) 안으로 자릅니다.
    realized["target_drive_minutes"] = (
        daily_minutes.clip(realized["min_drive_minutes"], realized["max_drive_minutes"])
        .round()
        .astype(int)
    )
    # 하루의 길이(첫 픽업 ~ 막 하차). 운행분에 유휴(대기)를 더한 값입니다.
    realized["target_work_minutes"] = (
        realized["target_drive_minutes"] / (1.0 - realized["idle_frac"])
    ).round().astype(int)
    # `rest_frac`·`min_daily_trips`·`max_daily_trips` 는 이제 아무도 읽지
    # 않습니다. draw 를 지우면 난수 스트림이 밀려 기사 전원이 바뀌므로 그대로
    # 둡니다 (정리는 기사 신원을 새로 뽑아도 되는 회차에).
    clip_rate = clipped / max(1, len(traits))
    return realized, pd.DataFrame(noise_rows), clip_rate
