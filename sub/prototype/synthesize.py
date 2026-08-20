"""4A · synthesize — 합성이 개입하는 유일한 구간.

blue_print.md 의 D5·D6·D7·D8·D14·D15 가 전부 이 모듈에 들어 있습니다.

출력 두 개의 관계가 이 모듈의 핵심입니다.

  driver_vehicle_event    append only 원장. 진실은 여기에만 있습니다.
  driver_vehicle_current  이벤트를 접은 파생물. 언제든 재생해 복원합니다.

전월 상태를 읽는 것은 성능 최적화입니다(4.2). 이벤트 전체를 매달 재생해도 같은
결과가 나와야 하고, `fold_events` 가 그 재생 함수입니다.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from sub.config import GenerationConfig
from sub.prototype import paths
from sub.prototype.assign import assign_vehicles
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

EVENT_JOIN = "join"
EVENT_EXIT = "exit"
EVENT_VEHICLE_CHANGE = "vehicle_change"
EVENT_TYPES = (EVENT_JOIN, EVENT_EXIT, EVENT_VEHICLE_CHANGE)

# 재고. 리스팅은 대수를 주지 않아서 가정입니다 — 싼 차가 많다는 쪽으로 가중치를
# 줍니다. 재고 제약이 실제로 걸려야 D5 의 "eligible pool → inventory constraint"
# 가 장식이 아닙니다.
# ponytail: 총 재고 = 정원의 1.15배. 리스팅에 대수 컬럼이 생기면 그것을 쓰세요.
FLEET_OVERSUPPLY = 1.15

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


@dataclass(frozen=True)
class SynthesizeResult:
    events: pd.DataFrame            # 이 달에 발생한 이벤트만
    current: pd.DataFrame           # 전체 이벤트를 접은 현재 상태
    profiles: pd.DataFrame          # 배정이 쓰는 기사 선호 (월별 실현값 반영)
    noise_state: pd.DataFrame       # D7 (B) 자기상관 연쇄 — 시드가 아니라 상태
    clip_rate: float                # 클리핑 발생 빈도 (volatility 과다 신호)


# ---------------------------------------------------------------------------
# 재고
# ---------------------------------------------------------------------------


def build_fleet_stock(vehicle_master: pd.DataFrame, *, driver_count: int) -> pd.DataFrame:
    """모델별 보유 대수. 싼 차에 가중치를 줍니다."""
    weight = 1.0 / vehicle_master["weekly_price_usd"].to_numpy()
    share = weight / weight.sum()
    total = int(round(driver_count * FLEET_OVERSUPPLY))
    units = np.maximum(1, np.floor(share * total).astype(int))
    stock = vehicle_master.copy()
    stock["unit_count"] = units
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


# ---------------------------------------------------------------------------
# D7 (A) 기사 고유 기준값 — 월을 시드에 넣지 않는다
# ---------------------------------------------------------------------------


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
        "preferred_distance_band": _distance_band(distance_pref_mi),
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
        #
        #   기존 `driver_master/traits.py` 가 갖고 있던 값을 되살린 것입니다.
        #   이 프로토타입 초판에서 빠뜨렸고, 그 탓에 목표 트립 수가 공차·유휴를
        #   0 으로 놓고 계산돼 p95 가 하루 108건이라는 값이 나왔습니다.
        "rest_frac": float(rng.uniform(0.05, 0.15)),
        "idle_frac": float(rng.uniform(0.15, 0.35)),
        # 하루 운행시간의 하한·상한. 위와 같은 이유로 **여기 맨 끝에** 붙입니다.
        # 앞에 끼우면 뒤쪽 draw 가 밀려서 기사 전원이 다른 사람이 됩니다.
        "min_drive_minutes": int(rng.integers(*MIN_DRIVE_MINUTES_RANGE)),
        "max_drive_minutes": int(rng.integers(*MAX_DRIVE_MINUTES_RANGE)),
    }


def _distance_band(miles: float) -> str:
    if miles <= DISTANCE_SHORT_MAX_MI:
        return DISTANCE_LABELS[0]
    if miles <= DISTANCE_MEDIUM_MAX_MI:
        return DISTANCE_LABELS[1]
    return DISTANCE_LABELS[2]


# ---------------------------------------------------------------------------
# D7 (B) 월별 실현값 — 월을 시드에 넣고, 자기상관은 상태로 연쇄한다
# ---------------------------------------------------------------------------


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
    #
    # 예전에는 여기서 목표 **트립 수**를 계산했습니다. `근무분 × (1-휴식) ×
    # (1-유휴) ÷ (평균 트립시간 + 공차기대값)` 이었고, 공차 기대값을 실측에
    # 맞추려고 `max_deadhead_minutes / 3` 이라는 보정 상수를 들고 있었습니다.
    # 그 3 은 배정 결과에서 역산한 값이라 배정을 고치면 다시 재야 했습니다 —
    # 목표가 결과에 의존하는 순환입니다. 운행분을 직접 예산으로 주면 배정이
    # 그 예산을 실제 공차로 소모하므로 보정 상수가 필요 없습니다.
    realized["target_drive_minutes"] = (
        daily_minutes.clip(realized["min_drive_minutes"], realized["max_drive_minutes"])
        .round()
        .astype(int)
    )
    # 하루의 길이(첫 픽업 ~ 막 하차). 운행분에 유휴(대기)를 더한 값입니다.
    # 이 상한이 없으면 새벽 4시에 한 건, 밤 11시에 한 건을 받아도 운행분
    # 예산은 안 넘습니다 — 하루가 19시간이 됩니다.
    realized["target_work_minutes"] = (
        realized["target_drive_minutes"] / (1.0 - realized["idle_frac"])
    ).round().astype(int)
    # `rest_frac`·`min_daily_trips`·`max_daily_trips` 는 이제 아무도 읽지
    # 않습니다. draw 를 지우면 난수 스트림이 밀려 기사 전원이 바뀌므로 그대로
    # 둡니다 (정리는 기사 신원을 새로 뽑아도 되는 회차에).
    clip_rate = clipped / max(1, len(traits))
    return realized, pd.DataFrame(noise_rows), clip_rate


# ---------------------------------------------------------------------------
# 실측 연료비 (차량 배정은 sub/prototype/assign.py 가 소유)
# ---------------------------------------------------------------------------


def load_fuel_prices() -> dict:
    """실측 유가·전기요금. 합성이 아닙니다."""
    gas = paths.read_parquet_dir(
        paths.latest_partition(paths.DATA / "silver" / "gas_price", "collected_month")
    )
    ev = paths.read_parquet_dir(
        paths.latest_partition(paths.DATA / "silver" / "ev_charging_price", "collected_month")
    )
    return {
        "gallon_usd": float(gas["price_usd_per_gallon"].mean()),
        "kwh_usd": float(ev["average_price_usd_per_kwh"].mean()),
    }


# ---------------------------------------------------------------------------
# lifecycle + event sourcing
# ---------------------------------------------------------------------------


def fold_events(events: pd.DataFrame) -> pd.DataFrame:
    """append only 원장을 접어 `driver_vehicle_current` 를 만듭니다 (4.2).

    이 함수가 재생 함수입니다. 전월 상태 캐시 없이 이벤트 전체만으로 같은 결과가
    나와야 하고, 그래서 전월 상태가 성능 최적화일 뿐 구조적 의존이 아닙니다.

    D15: 유출 기사의 행을 삭제하지 않습니다. `exited_on` 만 채웁니다.
    """
    if events.empty:
        return pd.DataFrame(
            columns=[
                "driver_id", "taxi_id", "traits_pool_month",
                "joined_on", "exited_on", "vehicle_since",
            ]
        )
    ordered = events.sort_values(["driver_id", "event_ts", "event_type"], kind="stable")
    state: dict[str, dict] = {}
    for event in ordered.to_dict("records"):
        driver_id = event["driver_id"]
        kind = event["event_type"]
        if kind == EVENT_JOIN:
            state[driver_id] = {
                "driver_id": driver_id,
                "taxi_id": event["taxi_id"],
                "traits_pool_month": event["traits_pool_month"],
                "joined_on": event["event_ts"],
                "exited_on": None,
                "vehicle_since": event["event_ts"],
            }
        elif kind == EVENT_EXIT:
            if driver_id in state:
                state[driver_id]["exited_on"] = event["event_ts"]
        elif kind == EVENT_VEHICLE_CHANGE:
            if driver_id in state:
                state[driver_id]["taxi_id"] = event["taxi_id"]
                state[driver_id]["vehicle_since"] = event["event_ts"]
        else:
            raise ValueError(f"알 수 없는 이벤트 종류: {kind}")
    return pd.DataFrame(list(state.values())).sort_values("driver_id").reset_index(drop=True)


def _next_driver_ids(existing: set[str], target_month: str, count: int) -> list[str]:
    """신규 기사 ID. 월을 담아서 어느 달에 유입됐는지 ID 로 읽힙니다."""
    stamp = target_month.replace("-", "")
    ids: list[str] = []
    serial = 1
    while len(ids) < count:
        candidate = f"DRIVER_{stamp}_{serial:06d}"
        serial += 1
        if candidate not in existing:
            ids.append(candidate)
            existing.add(candidate)
    return ids


def synthesize_month(
    *,
    target_month: str,
    config: GenerationConfig,
    vehicle_master: pd.DataFrame,
    trip_pool: dict[str, np.ndarray],
    previous_current: pd.DataFrame | None,
    previous_events: pd.DataFrame | None,
    previous_noise: pd.DataFrame | None,
    fuel: dict,
) -> SynthesizeResult:
    """그 달의 합성. 전월 상태가 없으면 초기 스냅샷을 만듭니다."""
    global_seed = config.global_seed
    month_ts = pd.Timestamp(f"{target_month}-01")
    stock = build_fleet_stock(vehicle_master, driver_count=config.driver.initial_count)
    fleet_units = expand_fleet_units(stock)

    history = (
        previous_events.copy()
        if previous_events is not None and not previous_events.empty
        else pd.DataFrame(columns=["driver_id", "event_type", "event_ts", "taxi_id", "traits_pool_month"])
    )
    known_ids = set(history["driver_id"]) if not history.empty else set()
    new_events: list[dict] = []

    if previous_current is None or previous_current.empty:
        # --- 초기 스냅샷 (SNAPSHOT_INIT, 월을 시드에 넣지 않음) -----------------
        init_rng = np.random.default_rng(derive_seed(global_seed, Stage.SNAPSHOT_INIT))
        width = len(str(config.driver.initial_count - 1))
        joiners = [f"DRIVER_{i:0{width}d}" for i in range(config.driver.initial_count)]
        known_ids.update(joiners)
        # 가입일은 초기 픽스처의 기준일 하나로 둡니다. 리스 시작일을 과거로 흩는
        # 것은 원천의 현실성이지만, 프로토타입의 측정 대상(매칭률)을 바꾸지
        # 않으면서 상태 연쇄만 복잡하게 만듭니다.
        del init_rng
        joined_on = month_ts
        exiters: list[str] = []
        changers: list[str] = []
    else:
        # --- lifecycle (SNAPSHOT_EVOLVE, 월을 시드에 넣음) ---------------------
        active = previous_current[previous_current["exited_on"].isna()]
        if active.empty:
            raise ValueError(
                f"{target_month}: 전월 활성 기사가 0명입니다. exit_rate 를 확인하세요."
            )
        life_rng = np.random.default_rng(
            derive_seed(global_seed, Stage.LIFECYCLE, target_month)
        )
        pool_ids = np.asarray(sorted(active["driver_id"]), dtype=object)

        def monthly_count(rate: float) -> int:
            """비율 × 정원의 기대값을 확률적으로 반올림합니다.

            `round()` 로 자르면 `0.007 × 60 = 0.42` 가 0 이 되어 그 이벤트가
            **영원히 발생하지 않습니다.** 설정에 0 이 아닌 값을 넣었는데 결과가
            안 바뀌는 상태이고, D12 가 없애려는 것과 같은 종류의 침묵입니다.
            소수부를 확률로 처리하면 기대값이 보존됩니다.
            """
            expected = len(pool_ids) * rate
            whole = int(expected)
            return whole + int(life_rng.random() < (expected - whole))

        exit_count = monthly_count(config.driver.exit_rate)
        join_count = monthly_count(config.driver.join_rate)
        change_count = monthly_count(config.driver.vehicle_change_rate)
        exiters = list(life_rng.choice(pool_ids, size=min(exit_count, len(pool_ids)), replace=False))
        remaining = np.asarray([d for d in pool_ids if d not in set(exiters)], dtype=object)
        changers = list(
            life_rng.choice(remaining, size=min(change_count, len(remaining)), replace=False)
        )
        joiners = _next_driver_ids(known_ids, target_month, join_count)
        joined_on = month_ts

    # D14: 총원 하한만 검증합니다. 상한을 두면 몇 달 뒤 조용히 실패합니다.
    surviving = len(known_ids) - len(set(history[history["event_type"] == EVENT_EXIT]["driver_id"]) if not history.empty else set())
    if surviving < 1:
        raise ValueError(f"{target_month}: 활성 기사가 1명 미만입니다")

    # 신규 기사의 성향 — 가입 시점 월이 traits_pool_month 입니다 (D8).
    traits_rows = [
        base_traits(
            driver_id,
            global_seed=global_seed,
            traits_pool_month=target_month,
            trip_pool=trip_pool,
        )
        for driver_id in joiners
    ]
    # 기존 기사의 성향은 각자의 traits_pool_month 로 **재계산**합니다. 이전 parquet
    # 복사(승계)가 아닙니다 — 그것이 D8 이 없애려는 우연한 안정성입니다.
    if previous_current is not None and not previous_current.empty:
        for record in previous_current[previous_current["exited_on"].isna()].to_dict("records"):
            if record["driver_id"] in set(exiters):
                continue
            traits_rows.append(
                base_traits(
                    record["driver_id"],
                    global_seed=global_seed,
                    traits_pool_month=str(record["traits_pool_month"]),
                    trip_pool=trip_pool,
                )
            )
    traits = pd.DataFrame(traits_rows).drop_duplicates("driver_id").reset_index(drop=True)

    # 차량 배정 — 이미 차를 가진 기사의 차량은 재고에서 빼고 시작합니다.
    held = set()
    if previous_current is not None and not previous_current.empty:
        held = set(
            previous_current.loc[previous_current["exited_on"].isna(), "taxi_id"].astype(str)
        )
        held -= {
            str(previous_current.loc[previous_current["driver_id"] == d, "taxi_id"].iloc[0])
            for d in exiters
        }
        held -= {
            str(previous_current.loc[previous_current["driver_id"] == d, "taxi_id"].iloc[0])
            for d in changers
        }
    available = fleet_units[~fleet_units["taxi_id"].astype(str).isin(held)]
    needs_vehicle = traits[traits["driver_id"].isin(set(joiners) | set(changers))]
    assignment = assign_vehicles(
        needs_vehicle,
        available,
        global_seed=global_seed,
        target_month=target_month,
        rationality=config.synthesize.rationality,
        fuel=fuel,
    )
    for driver_id in joiners:
        taxi_id = assignment.get(driver_id)
        if taxi_id is None:
            # 재고 소진. 조용히 넘기지 않습니다 — 유입이 재고를 넘으면 그건 설정
            # 문제이고, 넘어가면 명부에만 있고 차가 없는 기사가 생깁니다.
            raise ValueError(
                f"{target_month}: 신규 기사 {driver_id} 에게 배정할 차량 재고가 없습니다. "
                f"FLEET_OVERSUPPLY({FLEET_OVERSUPPLY}) 또는 driver.join_rate 를 확인하세요."
            )
        new_events.append({
            "driver_id": driver_id, "event_type": EVENT_JOIN, "event_ts": joined_on,
            "taxi_id": taxi_id, "traits_pool_month": target_month,
        })
    for driver_id in exiters:
        new_events.append({
            "driver_id": driver_id, "event_type": EVENT_EXIT, "event_ts": month_ts,
            "taxi_id": None, "traits_pool_month": None,
        })
    for driver_id in changers:
        taxi_id = assignment.get(driver_id)
        if taxi_id is None:
            continue  # 교체할 재고가 없으면 그 달은 그대로 탑니다
        new_events.append({
            "driver_id": driver_id, "event_type": EVENT_VEHICLE_CHANGE, "event_ts": month_ts,
            "taxi_id": taxi_id, "traits_pool_month": None,
        })

    events = pd.DataFrame(new_events)
    all_events = pd.concat([history, events], ignore_index=True) if not events.empty else history
    current = fold_events(all_events)

    realized, noise_state, clip_rate = realize_month(
        traits, previous_noise,
        global_seed=global_seed, target_month=target_month, config=config,
    )
    active_current = current[current["exited_on"].isna()][["driver_id", "taxi_id"]]
    profiles = realized.merge(active_current, on="driver_id", how="inner")
    if profiles.empty:
        raise ValueError(f"{target_month}: 활성 기사에 대응하는 선호가 없습니다")
    return SynthesizeResult(
        events=events, current=current, profiles=profiles,
        noise_state=noise_state, clip_rate=clip_rate,
    )
