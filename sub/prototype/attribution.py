"""4B · attribution — 실 트립 + 합성 신원.

**여기서 트립을 만들지 않습니다.** curated 트립은 TLC 실데이터이고, 이 단계가
하는 일은 그 트립에 `driver_id` / `taxi_id` 를 붙이는 것뿐입니다 (D3).

blue_print.md 3.5 의 제약 6종을 전부 구현하고, **어떤 제약이 몇 건을 떨어냈는지**를
같이 돌려줍니다. 그 분해가 이 프로토타입의 측정 대상입니다 — 매칭률이 낮을 때
"기사가 부족한가, 프로필이 너무 좁은가, 등급이 안 맞는가"를 구분할 수 있어야 합니다.
"""

from __future__ import annotations

import hashlib
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass, field

import numpy as np
import pandas as pd

from sub.prototype import log
from sub.seeds import Stage, derive_seed

# 제약 이름. 보고서와 코드가 같은 이름을 써야 대조가 됩니다.
C1_ROSTER = "c1_roster_period"        # 트립 시각에 기사가 명부에 존재
C2_TIER = "c2_vehicle_tier"           # 배정 차량 등급 = 트립 서비스 등급
C3_WORK_MINUTES = "c3_daily_work_minutes"    # 하루 길이(첫 픽업~막 하차) 초과
C4_OVERLAP = "c4_driver_time_overlap"
# c4 를 셋으로 쪼갭니다. 하나로 뭉쳐 두면 "연결이 안 된다"까지만 알 수 있고,
# 공간(너무 먼 구역) 문제인지 시간(이미 지나간 트립) 문제인지 구분이 안 됩니다.
# 고칠 손잡이가 다릅니다 — 전자는 bucket_size·max_deadhead, 후자는 수요 밀도.
C4_NO_ROUTE = "c4a_zone_pair_unknown"    # 두 구역 사이 이동시간을 모름
C4_TOO_FAR = "c4b_deadhead_over_limit"   # 공차가 기사 한도를 넘음
C4_TOO_LATE = "c4c_not_free_in_time"     # 도착 전에 이미 출발한 트립
C5_VEHICLE_CONFLICT = "c5_vehicle_double_use"
C6_PROFILE = "c6_profile_fit"         # 근무 요일·시간대·거리·공차 한도
# 하루 운행분(승객 태운 시간 + 공차) 예산 초과. 예전에는 "일일 트립 상한"
# (`c3_daily_trip_cap`)이었는데, 트립 수 목표를 운행시간 예산으로 바꾸면서
# 이름과 단위가 같이 바뀌었습니다. 리포트를 옛 실행과 대조할 때 주의하세요.
C3_DRIVE_MINUTES = "c3_daily_drive_minutes"
C_NO_CANDIDATE = "no_bucket_candidate"  # 버킷에 후보 기사가 아예 없음

# 후보 테이블로 넘길 트립 컬럼. curated 는 19개인데 배정·제약·산출물이 읽는 건
# 아래뿐입니다. 나머지 요금 항목(base_passenger_fare·tolls·sales_tax 등)과 존
# 이름은 후보 행 수만큼 복제될 뿐 쓰이지 않습니다.
#
# `on_scene_datetime` 과 `tips` 는 배정이 보지 않는데도 실려 있습니다. 둘 다
# 산출물 계약이 그대로 요구합니다(schema/bronze.py) — 원본에 있는 값이라 배정을
# 거쳐도 떨어뜨리지 않고 실어 나릅니다.
TRIP_CANDIDATE_COLUMNS = [
    "trip_key", "on_scene_datetime", "pickup_datetime", "dropoff_datetime",
    "PULocationID", "DOLocationID", "trip_miles", "trip_time",
    "tips", "driver_pay",
    "platform_name", "estimated_service_tier",
    "service_date", "weekday", "time_block", "is_airport", "is_manhattan",
]


@dataclass
class AttributionResult:
    attributed: pd.DataFrame
    rejected: pd.DataFrame              # trip_key × 최종 탈락 사유
    reason_counts: dict = field(default_factory=dict)
    candidate_rows: int = 0
    survivor_rows: int = 0


def _tier_eligible(trips: pd.DataFrame) -> pd.Series:
    """제약 2. 트립 등급을 그 차량이 태울 자격이 있는가."""
    tier = trips["estimated_service_tier"]
    platform = trips["platform_name"]
    return (
        (tier == "Standard")
        | ((platform == "Uber") & (tier == "Comfort") & trips["uber_comfort_eligible"])
        | ((platform == "Lyft") & (tier == "Extra Comfort") & trips["lyft_extra_comfort_eligible"])
    )


def _profile_fit(frame: pd.DataFrame) -> pd.Series:
    """제약 6. 귀속이 기사 프로필을 따르는가.

    이게 없으면 4A 가 만든 프로필 파라미터가 장식으로 끝나고, "기사마다 최적
    차량이 다르다"는 프로젝트 전제가 성립하지 않습니다 (blue_print.md 3.5).

    요일·시간대를 **비트마스크 정수**로 받습니다. 예전에는 기사의 `active_weekdays`
    리스트를 후보 행마다 들고 다니며 `weekday in weekdays` 를 파이썬 루프로 돌았는데,
    그러면 (1) 기사 2,000행짜리 리스트가 후보 330만 행으로 복제되고
    (2) 그 검사가 벡터화되지 않습니다. 전체 달에서 워커가 메모리로 죽고 46분이
    걸린 원인이 이것이었습니다.
    """
    weekday_ok = (frame["_weekday_mask"].to_numpy() >> frame["weekday"].to_numpy()) & 1
    block_ok = (frame["_block_mask"].to_numpy() >> frame["time_block"].to_numpy()) & 1
    return pd.Series((weekday_ok & block_ok).astype(bool), index=frame.index)


def _preference_score(
    frame: pd.DataFrame, weights: dict[str, float], block_scores: np.ndarray
) -> pd.Series:
    """0~1 선호 점수. 가중치 합이 1.0 이라 범위가 유지됩니다.

    시간대 점수는 기사별 8블록 가중치를 최대값으로 정규화한 값입니다. 그 8개를
    후보 행마다 리스트로 들고 다니지 않고, `(기사 수, 8)` 행렬을 한 번 만들어
    `(기사 위치, 블록)` 으로 색인합니다.
    """
    time_score = block_scores[
        frame["_driver_pos"].to_numpy(), frame["time_block"].to_numpy()
    ]

    # 거리 점수: 선호 거리와 가까울수록 1.
    gap = (frame["trip_miles"] - frame["distance_pref_mi"]).abs()
    distance_score = 1.0 / (1.0 + gap / np.maximum(1.0, frame["distance_pref_mi"]))

    airport_score = np.where(frame["is_airport"], frame["airport_preference"], 1.0 - frame["airport_preference"])
    manhattan_score = np.where(frame["is_manhattan"], frame["manhattan_preference"], 1.0 - frame["manhattan_preference"])
    premium = frame["estimated_service_tier"] != "Standard"
    tier_score = np.where(premium, frame["tier_preference"], 1.0 - frame["tier_preference"])

    return pd.Series(
        weights["time"] * time_score
        + weights["distance"] * distance_score
        + weights["airport"] * airport_score
        + weights["manhattan"] * manhattan_score
        + weights["tier"] * tier_score,
        index=frame.index,
    )


@dataclass
class DriverSide:
    """버킷마다 다시 만들 필요가 없는 기사 쪽 사전 계산."""
    compact: pd.DataFrame
    block_scores: np.ndarray
    bucket_count: int


def prepare_drivers(
    profiles: pd.DataFrame, fleet_units: pd.DataFrame, *, bucket_size: int
) -> DriverSide:
    """기사 N명을 `bucket_size` 명씩 버킷에 넣고, 후보 테이블이 읽을 컬럼만 남깁니다.

    버킷 샤딩으로 후보 폭발을 막습니다. 트립도 같은 버킷 수로 해시해 **한 버킷에만**
    넣으므로, 트립 하나가 보는 기사가 `bucket_size` 명으로 제한됩니다 — 이게 매칭률의
    상한을 만드는 구조적 요인이라 지표에 함께 싣습니다.
    """
    drivers = profiles.sort_values("driver_id").reset_index(drop=True)
    bucket_count = max(1, len(drivers) // bucket_size)

    # 차량 자격을 붙입니다. 제약 2 가 이 두 컬럼만 봅니다 — 제원·가격은 배정에
    # 쓰이지 않으므로 후보 테이블로 끌고 오지 않습니다.
    vehicle = fleet_units[["taxi_id", "uber_comfort_eligible", "lyft_extra_comfort_eligible"]]
    drivers = drivers.merge(vehicle, on="taxi_id", how="left")
    if drivers["uber_comfort_eligible"].isna().any():
        raise ValueError("배정된 taxi_id 가 차량 대장에 없습니다 — 4A 재고 배정을 확인하세요")

    # --- 리스트 컬럼을 후보로 복제하지 않기 위한 사전 계산 -------------------
    # 기사 2,000행의 파이썬 리스트가 후보 330만 행으로 복제되면 그것만으로 수백
    # MB 이고, `in` 검사가 벡터화되지 않아 느립니다. 요일·시간대는 비트마스크
    # 정수로, 시간대 가중치는 (기사 수, 8) 행렬로 바꿔 위치로 색인합니다.
    weekday_mask = drivers["active_weekdays"].map(
        lambda days: int(sum(1 << int(d) for d in days))
    ).to_numpy()
    block_mask = drivers["preferred_time_blocks"].map(
        lambda blocks: int(sum(1 << int(b) for b in blocks))
    ).to_numpy()
    weights_matrix = np.asarray(
        [np.asarray(w, dtype=float) for w in drivers["time_block_weights"]], dtype=float
    )
    row_max = weights_matrix.max(axis=1, keepdims=True)
    block_scores = np.divide(
        weights_matrix, row_max, out=np.zeros_like(weights_matrix), where=row_max > 0
    )

    # 후보 테이블로 넘길 기사 컬럼. 배정과 제약이 실제로 읽는 것만 남깁니다 —
    # `driver_name`·`base_weekly_hours` 같은 값은 후보 행마다 복제될 이유가 없습니다.
    compact = pd.DataFrame({
        "_bucket": np.arange(len(drivers)) % bucket_count,
        "_driver_pos": np.arange(len(drivers)),
        "_weekday_mask": weekday_mask,
        "_block_mask": block_mask,
        "driver_id": drivers["driver_id"].to_numpy(),
        "taxi_id": drivers["taxi_id"].to_numpy(),
        "joined_on": pd.to_datetime(drivers["joined_on"]).to_numpy(),
        "exited_on": pd.to_datetime(drivers["exited_on"]).to_numpy(),
        "uber_comfort_eligible": drivers["uber_comfort_eligible"].to_numpy(),
        "lyft_extra_comfort_eligible": drivers["lyft_extra_comfort_eligible"].to_numpy(),
        "distance_pref_mi": drivers["distance_pref_mi"].to_numpy(),
        "airport_preference": drivers["airport_preference"].to_numpy(),
        "manhattan_preference": drivers["manhattan_preference"].to_numpy(),
        "tier_preference": drivers["tier_preference"].to_numpy(),
        "target_drive_minutes": drivers["target_drive_minutes"].to_numpy(),
        "target_work_minutes": drivers["target_work_minutes"].to_numpy(),
        "max_deadhead_minutes": drivers["max_deadhead_minutes"].to_numpy(),
        "buffer_seconds": drivers["buffer_seconds"].to_numpy(),
    })
    return DriverSide(compact=compact, block_scores=block_scores, bucket_count=bucket_count)


def trip_buckets(trip_keys, *, bucket_seed: int, bucket_count: int) -> np.ndarray:
    """결정적 해시. pandas 의 `hash` 는 실행마다 달라질 수 있어 쓰지 않습니다."""
    return np.fromiter(
        (
            int.from_bytes(hashlib.sha256(f"{bucket_seed}:{k}".encode()).digest()[:8], "big")
            % bucket_count
            for k in trip_keys
        ),
        dtype=np.int64,
        count=len(trip_keys),
    )


def candidates_for(
    keyed: pd.DataFrame,
    side: DriverSide,
    *,
    bucket_seed: int,
    score_weights: dict[str, float],
) -> tuple[pd.DataFrame, dict[str, int]]:
    """후보 생성 + 벡터화 가능한 제약(1·2·6) 적용.

    `keyed` 는 `_bucket` 이 이미 붙은 트립입니다. 한 버킷만 담겨 있어도 되고 하루
    전체가 담겨 있어도 되며, 어느 쪽이든 결과는 같습니다 — merge 가 `_bucket` 으로
    내부 조인하므로 담기지 않은 버킷은 애초에 행을 만들지 않습니다.
    """
    rejected: dict[str, int] = {}
    candidates = keyed.merge(side.compact, on="_bucket", how="inner")
    candidate_rows = len(candidates)
    trips_with_candidate = candidates["trip_key"].nunique()
    rejected[C_NO_CANDIDATE] = len(keyed) - trips_with_candidate

    # --- 제약 1: 트립 시각에 기사가 명부에 존재 ----------------------------
    roster_ok = (candidates["joined_on"] <= candidates["pickup_datetime"]) & (
        candidates["exited_on"].isna() | (candidates["pickup_datetime"] < candidates["exited_on"])
    )
    rejected[C1_ROSTER] = int((~roster_ok).sum())
    candidates = candidates[roster_ok]

    # --- 제약 2: 차량 등급 자격 -------------------------------------------
    tier_ok = _tier_eligible(candidates)
    rejected[C2_TIER] = int((~tier_ok).sum())
    candidates = candidates[tier_ok]

    # --- 제약 6: 프로필 적합 ----------------------------------------------
    profile_ok = _profile_fit(candidates)
    rejected[C6_PROFILE] = int((~profile_ok).sum())
    candidates = candidates[profile_ok].copy()

    if candidates.empty:
        return candidates, {**rejected, "_candidate_rows": candidate_rows}
    candidates["preference_score"] = _preference_score(
        candidates, score_weights, side.block_scores
    )
    # tie-break. 점수가 같을 때의 순서를 결정적으로 고정하는 것뿐이라, 버킷 샤딩과
    # 달리 배정 가능 집합을 바꾸지 않습니다.
    candidates["tie_break"] = [
        hashlib.sha256(f"{bucket_seed}:{t}:{d}".encode()).hexdigest()[:16]
        for t, d in zip(candidates["trip_key"], candidates["driver_id"])
    ]
    return candidates, {**rejected, "_candidate_rows": candidate_rows}


def build_candidates(
    trips: pd.DataFrame,
    profiles: pd.DataFrame,
    fleet_units: pd.DataFrame,
    *,
    global_seed: int,
    target_month: str,
    bucket_size: int,
    score_weights: dict[str, float],
) -> tuple[pd.DataFrame, dict[str, int]]:
    """받은 트립 전부의 후보를 **한 프레임으로** 만듭니다.

    `attribute_chunk` 가 버킷을 흘려 처리하므로 실행 경로에서는 쓰지 않습니다.
    남겨 두는 이유는 두 경로가 같은 결과를 낸다는 것을 테스트가 대조하기 때문입니다.
    """
    side = prepare_drivers(profiles, fleet_units, bucket_size=bucket_size)
    bucket_seed = derive_seed(global_seed, Stage.ALLOCATION_BUCKET, target_month)
    keyed = trips[TRIP_CANDIDATE_COLUMNS].copy()
    keyed["_bucket"] = trip_buckets(
        keyed["trip_key"], bucket_seed=bucket_seed, bucket_count=side.bucket_count
    )
    return candidates_for(keyed, side, bucket_seed=bucket_seed, score_weights=score_weights)


# 배정 결과 컬럼. 순서를 고정해 두는 이유는 산출물 계약이라서입니다 —
# `published.TRIP_SNAPSHOT_COLUMNS` 가 이 이름들을 읽습니다.
ASSIGNED_COLUMNS = [
    "trip_key", "driver_id", "taxi_id", "service_date", "trip_sequence",
    "on_scene_datetime", "pickup_datetime", "dropoff_datetime", "PULocationID", "DOLocationID",
    "trip_miles", "trip_time", "tips", "driver_pay",
    "platform_name", "estimated_service_tier", "preference_score", "deadhead_minutes",
]

_NS_PER_MINUTE = 60_000_000_000
_NS_PER_SECOND = 1_000_000_000


def allocate(
    candidates: pd.DataFrame,
    travel_minutes: dict[tuple[int, int], float],
) -> tuple[pd.DataFrame, dict[str, int]]:
    """제약 3·4·5 를 순차 상태로 지키며 greedy 배정.

    이 세 제약은 벡터화할 수 없습니다. "이 기사가 이 트립을 받을 수 있는가"가
    그 기사가 **이미 받은 트립들**에 의존하기 때문입니다. 그래서 (버킷, 서비스일)
    그룹 안에서 픽업 시각 순으로 훑습니다.

    벡터화가 안 되는 것과 **pandas 객체를 행마다 만드는 것**은 다른 문제입니다.
    예전에는 (버킷,서비스일) → trip_key 로 groupby 를 두 번 돌면서 트립마다
    `itertuples` 를 불렀는데, 트립 하나당 namedtuple 클래스 생성 + 컬럼별 `.iloc`
    가 붙습니다. 482k 트립 한 part 에서 그것만 494초 — 전체 실행의 93% 였습니다.
    지금은 정렬한 뒤 컬럼을 파이썬 리스트로 뽑아 한 번만 훑고, 그룹·트립 경계에서
    상태를 직접 리셋합니다. 정렬 순서가 같으므로 배정 결과는 동일합니다.

    제약 5(차량 중복 사용)는 기사:차량이 1:1 이라 제약 4 가 성립하면 따라옵니다.
    그래도 세어 둡니다 — 1:N 이 되는 순간 조용히 깨질 자리입니다.
    """
    counters = {
        C3_WORK_MINUTES: 0, C4_OVERLAP: 0, C5_VEHICLE_CONFLICT: 0, C3_DRIVE_MINUTES: 0,
        C4_NO_ROUTE: 0, C4_TOO_FAR: 0, C4_TOO_LATE: 0,
    }
    if candidates.empty:
        return pd.DataFrame(), counters

    order = candidates.sort_values(
        ["_bucket", "service_date", "pickup_datetime", "trip_key", "preference_score", "tie_break"],
        ascending=[True, True, True, True, False, True],
        kind="stable",
    )

    # `.tolist()` 로 파이썬 객체로 내려 둡니다. numpy 배열을 스칼라 색인하면
    # 행마다 np.int64 를 새로 만들고, 그 박싱이 리스트 색인보다 비쌉니다.
    def ints(column: str) -> list:
        return order[column].to_numpy().tolist()

    def times(column: str) -> list[int]:
        return order[column].to_numpy("datetime64[ns]").astype("int64").tolist()

    trip_keys = order["trip_key"].tolist()
    taxi_ids = order["taxi_id"].tolist()
    driver_pos = ints("_driver_pos")
    buckets = ints("_bucket")
    service_days = times("service_date")
    pickups = times("pickup_datetime")
    dropoffs = times("dropoff_datetime")
    pickup_zones = ints("PULocationID")
    dropoff_zones = ints("DOLocationID")
    drive_budgets = ints("target_drive_minutes")
    work_minute_caps = ints("target_work_minutes")
    trip_minutes = [seconds / 60.0 for seconds in ints("trip_time")]
    deadhead_caps = ints("max_deadhead_minutes")
    buffers = ints("buffer_seconds")

    picked: list[int] = []
    sequences: list[int] = []
    deadheads: list[float] = []

    # 기사 위치 -> (첫 픽업, 막 하차, 막 하차 구역, 트립 수, 누적 운행분)
    driver_state: dict[int, tuple[int, int, int, int, float]] = {}
    vehicle_busy: dict[str, int] = {}
    current_bucket = current_day = None
    current_trip = None
    trip_taken = False

    for row in range(len(trip_keys)):
        if buckets[row] != current_bucket or service_days[row] != current_day:
            current_bucket, current_day = buckets[row], service_days[row]
            driver_state = {}
            vehicle_busy = {}
            current_trip = None
        trip_key = trip_keys[row]
        if trip_key != current_trip:
            current_trip = trip_key
            trip_taken = False
        elif trip_taken:
            continue  # 이 트립은 이미 배정됐습니다 (예전의 `break`)

        position = driver_pos[row]
        previous = driver_state.get(position)
        pickup = pickups[row]
        dropoff = dropoffs[row]
        deadhead = 0.0
        sequence = 1
        first_pickup = pickup
        used = 0.0
        if previous is not None:
            first_pickup, previous_dropoff, previous_zone, count, used = previous
            zone = pickup_zones[row]
            if previous_zone != zone:
                pair = (previous_zone, zone)
                if pair not in travel_minutes:
                    # 이동시간을 모르는 구역쌍. 공차를 0으로 두면 물리적으로
                    # 불가능한 연결이 통과하므로 떨어냅니다.
                    counters[C4_OVERLAP] += 1
                    counters[C4_NO_ROUTE] += 1
                    continue
                deadhead = travel_minutes[pair]
            if deadhead > deadhead_caps[row]:
                counters[C4_OVERLAP] += 1
                counters[C4_TOO_FAR] += 1
                continue
            # `timedelta` 와 같은 마이크로초 반올림을 유지합니다 — 예전 경로와
            # 경계값에서 다른 판정이 나오지 않게.
            ready = (
                previous_dropoff
                + round(deadhead * 60_000_000) * 1_000
                + buffers[row] * _NS_PER_SECOND
            )
            if ready > pickup:
                counters[C4_OVERLAP] += 1
                counters[C4_TOO_LATE] += 1
                continue
            if (dropoff - first_pickup) / _NS_PER_MINUTE > work_minute_caps[row]:
                counters[C3_WORK_MINUTES] += 1
                continue
            sequence = count + 1
        # 하루 운행분 예산. 트립 하나가 먹는 것은 승객 태운 시간 + 그 트립까지의
        # 공차입니다. 첫 트립도 예산을 넘을 수 있어서(예산 4시간에 5시간 트립)
        # `previous` 유무와 무관하게 봅니다.
        drive = trip_minutes[row] + deadhead
        if used + drive > drive_budgets[row]:
            counters[C3_DRIVE_MINUTES] += 1
            continue
        taxi_id = taxi_ids[row]
        busy_until = vehicle_busy.get(taxi_id)
        if busy_until is not None and busy_until > pickup:
            counters[C5_VEHICLE_CONFLICT] += 1
            continue
        driver_state[position] = (
            first_pickup, dropoff, dropoff_zones[row], sequence, used + drive
        )
        vehicle_busy[taxi_id] = dropoff
        picked.append(row)
        sequences.append(sequence)
        deadheads.append(deadhead)
        trip_taken = True

    if not picked:
        return pd.DataFrame(), counters
    assigned = order.iloc[picked].reset_index(drop=True)
    assigned["trip_sequence"] = sequences
    assigned["deadhead_minutes"] = deadheads
    # 존 ID 는 parquet 에서 int32 로 올라옵니다. 산출물 스키마를 바꾸지 않으려고
    # 예전 경로(파이썬 int)와 같은 int64 로 맞춥니다.
    assigned["PULocationID"] = assigned["PULocationID"].astype("int64")
    assigned["DOLocationID"] = assigned["DOLocationID"].astype("int64")
    return assigned[ASSIGNED_COLUMNS], counters


def attribute_chunk(
    trips: pd.DataFrame,
    profiles: pd.DataFrame,
    fleet_units: pd.DataFrame,
    travel_minutes: dict[tuple[int, int], float],
    *,
    global_seed: int,
    target_month: str,
    bucket_size: int,
    score_weights: dict[str, float],
) -> tuple[pd.DataFrame, dict[str, int], int, int]:
    """받은 트립을 **버킷 하나씩** 후보 생성 → 배정하고, 배정된 행만 모읍니다.

    한 청크(하루)의 후보를 한 프레임에 담으면 `트립 수 × bucket_size` 행이 됩니다.
    72만 트립 × 5 = 360만 행이고, `bucket_size` 를 10 으로 올리면 720만 행이라
    워커가 메모리에서 죽습니다. `bucket_size` 를 올리는 것이 매칭률을 올리는
    손잡이인데, 그 손잡이가 메모리에 막혀 있던 셈입니다.

    쪼개도 결과가 같은 이유는 `allocate` 의 상태가 이미 (버킷, 서비스일) 단위이기
    때문입니다. 기사도 트립도 버킷 하나에만 속하므로 버킷 사이에 흐르는 상태가
    없습니다 — 청크 경계를 **알고리즘의 상태 경계**에 맞춘 것이지 파일 경계에
    맞춘 것이 아닙니다.

    남는 것은 배정된 행(하루 1.2만)과 사유 카운터뿐이라, 최대 사용량이
    `트립 수 × bucket_size` 에서 `버킷당 트립 수 × bucket_size` 로 내려갑니다.
    버킷이 400개면 400배입니다.
    """
    side = prepare_drivers(profiles, fleet_units, bucket_size=bucket_size)
    # `ALLOCATION_BUCKET` 은 그 달의 샤딩이라 월을 시드에 넣습니다.
    bucket_seed = derive_seed(global_seed, Stage.ALLOCATION_BUCKET, target_month)
    # 후보로 넘길 트립 컬럼을 추립니다. 존 이름과 요금 항목은 배정이 보지
    # 않습니다 — `on_scene_datetime` 과 `tips` 만 계약 때문에 남깁니다.
    keyed = trips[TRIP_CANDIDATE_COLUMNS].copy()
    keyed["_bucket"] = trip_buckets(
        keyed["trip_key"], bucket_seed=bucket_seed, bucket_count=side.bucket_count
    )

    frames: list[pd.DataFrame] = []
    counts: dict[str, int] = {}
    candidate_rows = survivor_rows = 0
    for _, group in keyed.groupby("_bucket", sort=True):
        candidates, pre_counts = candidates_for(
            group, side, bucket_seed=bucket_seed, score_weights=score_weights
        )
        assigned, post_counts = allocate(candidates, travel_minutes)
        candidate_rows += int(pre_counts.pop("_candidate_rows", 0))
        survivor_rows += len(candidates)
        for reason, value in {**pre_counts, **post_counts}.items():
            counts[reason] = counts.get(reason, 0) + value
        if not assigned.empty:
            frames.append(assigned)
        del candidates, assigned
    del keyed

    attributed = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    return attributed, counts, candidate_rows, survivor_rows


def _attribute_day(task) -> tuple[pd.DataFrame, dict[str, int], int, int]:
    """하루치 귀속. `ProcessPoolExecutor` 가 피클할 수 있어야 해서 모듈 최상위입니다.

    `day_trips` 가 DataFrame 이면 그대로, 경로면 워커 안에서 읽습니다. 경로로 주면
    하루치(약 67만 행)를 프로세스 경계로 피클하지 않아도 됩니다.
    """
    (
        day_trips, drivers, fleet_units, travel_minutes,
        global_seed, target_month, bucket_size, score_weights,
    ) = task
    if not isinstance(day_trips, pd.DataFrame):
        day_trips = pd.read_parquet(day_trips)
    return attribute_chunk(
        day_trips, drivers, fleet_units, travel_minutes,
        global_seed=global_seed, target_month=target_month,
        bucket_size=bucket_size, score_weights=score_weights,
    )


def attribute_month(
    trips: pd.DataFrame,
    profiles: pd.DataFrame,
    current: pd.DataFrame,
    fleet_units: pd.DataFrame,
    travel_minutes: dict[tuple[int, int], float],
    *,
    global_seed: int,
    target_month: str,
    bucket_size: int,
    score_weights: dict[str, float],
    jobs: int = 1,
) -> AttributionResult:
    """제약 6종을 통과한 트립에만 합성 신원을 붙입니다."""
    roster = current[["driver_id", "joined_on", "exited_on"]]
    enriched = profiles.merge(roster, on="driver_id", how="inner")

    # 청크를 두 겹으로 나눕니다.
    #
    #   바깥(여기)  서비스일   — 프로세스 병렬의 단위. 워커가 파일을 각자 읽습니다.
    #   안쪽        버킷       — `attribute_chunk` 안에서 순차. 메모리 최대치를 정합니다.
    #
    # 바깥만으로는 부족합니다. 하루도 72만 트립이라 후보가 `72만 × bucket_size`
    # 행이고, `bucket_size` 를 올리면 그대로 따라 커집니다. 안쪽 버킷 루프가
    # 그 최대치를 버킷 수(400)로 나눕니다.
    #
    # 두 겹 다 결과가 같은 이유는 같습니다 — `allocate` 의 상태가 (버킷, 서비스일)
    # 단위이고 기사도 트립도 버킷 하나에만 속하므로, 두 축 모두 완전히 독립입니다.
    if isinstance(trips, pd.DataFrame):
        days = sorted(trips["service_date"].unique())
        chunks = [group for _, group in trips.groupby("service_date", sort=True)]
    else:
        # `curated.DayPartitions.files` — 서비스일 -> 그 날 파일 디렉터리.
        # 프레임을 만들지 않고 경로만 넘겨 워커가 각자 읽습니다.
        days = sorted(trips)
        chunks = [trips[day] for day in days]
    labels = [str(pd.Timestamp(day).date()) for day in days]
    tasks = [
        (
            chunk, enriched, fleet_units, travel_minutes,
            global_seed, target_month, bucket_size, score_weights,
        )
        for chunk in chunks
    ]

    # 진행 로그. 하루가 몇 분씩 걸리는데 아무것도 찍지 않으면 죽은 것과 구분이
    # 안 됩니다. 결과는 **제출 순서로 되돌려** 담습니다 — 완료 순서로 담으면
    # 프로세스 스케줄링에 따라 concat 순서가 달라져 재현되지 않습니다.
    done = 0
    matched = 0
    results: list = [None] * len(tasks)

    def note(index: int, result) -> None:
        nonlocal done, matched
        done += 1
        matched += len(result[0])
        log(
            f"귀속 {done}/{len(tasks)}일 · {labels[index]} · "
            f"이 날 {len(result[0]):,}건 (누적 {matched:,}건)"
        )

    log(f"귀속 시작: 서비스일 {len(tasks)}일, 프로세스 {min(jobs, len(tasks))}개, 기사 {len(enriched):,}명")
    if jobs > 1 and len(tasks) > 1:
        with ProcessPoolExecutor(max_workers=min(jobs, len(tasks))) as pool:
            futures = {pool.submit(_attribute_day, task): index for index, task in enumerate(tasks)}
            for future in as_completed(futures):
                index = futures[future]
                results[index] = future.result()
                note(index, results[index])
    else:
        for index, task in enumerate(tasks):
            results[index] = _attribute_day(task)
            note(index, results[index])

    frames = [frame for frame, _, _, _ in results if not frame.empty]
    attributed = (
        pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    )
    reason_counts: dict[str, int] = {}
    candidate_rows = survivor_rows = 0
    for _, counts, cand, surv in results:
        for reason, value in counts.items():
            reason_counts[reason] = reason_counts.get(reason, 0) + value
        candidate_rows += cand
        survivor_rows += surv

    # 탈락 트립 목록은 프레임을 받았을 때만 냅니다. 전체 달 경로에서는 이 목록이
    # 1,700만 행이라 만드는 것 자체가 메모리를 먹고, 쓰는 곳도 없습니다 —
    # 탈락 **사유별 집계**(`reason_counts`)가 실제로 보는 값입니다.
    if isinstance(trips, pd.DataFrame):
        matched_keys = set(attributed["trip_key"]) if not attributed.empty else set()
        rejected = trips.loc[
            ~trips["trip_key"].isin(matched_keys),
            ["trip_key", "estimated_service_tier", "platform_name"],
        ].copy()
    else:
        rejected = pd.DataFrame(columns=["trip_key", "estimated_service_tier", "platform_name"])
    # 산출물의 무결성 — 여기서 안 잡으면 하류가 조용히 잘못된 값을 씁니다.
    if not attributed.empty:
        if attributed["trip_key"].duplicated().any():
            raise ValueError("한 트립이 두 번 배정됐습니다 (제약 위반)")
        overlap = attributed.sort_values(["driver_id", "pickup_datetime"])
        shifted = overlap.groupby("driver_id")["dropoff_datetime"].shift()
        if (overlap["pickup_datetime"] < shifted).any():
            raise ValueError("같은 기사의 트립이 시간상 겹칩니다 (제약 4 위반)")
    return AttributionResult(
        attributed=attributed,
        rejected=rejected,
        reason_counts=reason_counts,
        candidate_rows=candidate_rows,
        survivor_rows=survivor_rows,
    )
