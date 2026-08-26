"""기사 성향(trait) 샘플링.

`implementation_plan.md` §1 표를 그대로 코드로 옮깁니다. 트레잇 중 `distance_pref_i`,
`avg_trip_duration_i`는 실측 HVFHV 트립 데이터에서 부트스트랩 추출합니다 — 기사 간
분산이 실측 population 분산을 그대로 물려받게 하려는 목적입니다(`analysis.md` §3).

요일/시간대 가중치, 거리 버킷 임계값은 `analysis.md`에서 이미 계산해 둔 값을
상수로 고정합니다. 원본 bronze 데이터가 바뀌면 `analysis.md`를 다시 돌려 이 상수들도
갱신해야 합니다.
"""

import glob
import logging
from pathlib import Path

import numpy as np
import pandas as pd

from shared.common.s3_reader import (
    is_s3_uri,
    list_keys,
    parse_s3_uri,
    read_parquet_uri,
)

logger = logging.getLogger(__name__)

# =============================================================================
# analysis.md 실측 상수
# =============================================================================

# 요일 비중 (월=0 ~ 일=6), analysis.md §2
WEEKDAY_WEIGHTS = np.array([0.125, 0.130, 0.136, 0.143, 0.156, 0.167, 0.143])
WEEKDAY_LABELS = ["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"]

# 3시간 단위 시간대 비중, analysis.md §2 (hour-of-day 비중을 3시간 블록으로 합산)
TIME_BLOCK_WEIGHTS = np.array([0.081, 0.048, 0.120, 0.131, 0.136, 0.157, 0.171, 0.156])
TIME_BLOCK_LABELS = ["00-03", "03-06", "06-09", "09-12", "12-15", "15-18", "18-21", "21-24"]

# 거리 버킷 임계값(mile), analysis.md §2 trip_miles tertile (p33/p66)
DISTANCE_SHORT_MAX_MI = 1.93
DISTANCE_MEDIUM_MAX_MI = 4.75
DISTANCE_LABELS = ["SHORT", "MEDIUM", "LONG"]

# =============================================================================
# 기사이름용 영어 이름 풀 (신규 의존성 추가 안 하려고 하드코딩)
# =============================================================================

FIRST_NAMES = [
    "James", "Michael", "Robert", "John", "David", "William", "Richard", "Joseph",
    "Thomas", "Charles", "Daniel", "Matthew", "Anthony", "Mark", "Paul", "Steven",
    "Andrew", "Kenneth", "George", "Joshua", "Kevin", "Brian", "Edward", "Ronald",
    "Timothy", "Jason", "Jeffrey", "Ryan", "Jacob", "Gary",
    "Mary", "Patricia", "Jennifer", "Linda", "Elizabeth", "Barbara", "Susan", "Jessica",
    "Sarah", "Karen", "Nancy", "Lisa", "Margaret", "Betty", "Sandra", "Ashley",
    "Kimberly", "Emily", "Donna", "Michelle",
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
    "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson",
    "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker",
    "Young", "Allen", "King", "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores",
]


def discover_bootstrap_months(bronze_dir: str) -> list[str]:
    """`bronze_dir` 의 `year_month=` 파티션을 오름차순으로 모읍니다.

    ISO 연월이라 이름 정렬이 곧 시간 정렬입니다.
    """
    months = sorted(
        partition.name.removeprefix("year_month=")
        for partition in Path(bronze_dir).glob("year_month=*")
        if partition.is_dir()
    )
    if not months:
        raise FileNotFoundError(
            f"HVFHV Bronze 파티션이 없습니다: {bronze_dir}. "
            "monthly_taxi_trip_raw_to_silver DAG 를 먼저 돌리거나 --months 로 직접 지정하세요."
        )
    return months


def _latest_partition_file(bronze_dir: str, year_month: str) -> str | None:
    """`year_month=` 파티션의 가장 마지막 Parquet 하나. 없으면 None.

    S3 도 봅니다 — EMR 워커는 컨테이너 로컬 디스크를 못 봅니다. 파일 이름이 수집
    시각을 담아 정렬이 곧 시간 순이라 마지막 하나를 씁니다(원본 코드와 같은 규칙).
    """
    if is_s3_uri(bronze_dir):
        bucket, key_prefix = parse_s3_uri(bronze_dir.rstrip("/") + "/")
        prefix = f"{key_prefix}year_month={year_month}/"
        keys = sorted(k for k in list_keys(bucket, prefix) if k.endswith(".parquet"))
        return f"s3://{bucket}/{keys[-1]}" if keys else None

    files = sorted(glob.glob(str(Path(bronze_dir) / f"year_month={year_month}" / "*.parquet")))
    return files[-1] if files else None


# 부트스트랩 풀이 보는 컬럼 전부입니다. 원천에는 25개가 있고 나머지는 읽지 않습니다.
BOOTSTRAP_COLUMNS = ["trip_miles", "trip_time", "driver_pay"]


def load_bootstrap_pools(
    *,
    bronze_dir: str,
    sample_per_month: int,
    seed: int,
    # None 은 값이 아니라 "bronze_dir 에 있는 달 전부" 라는 뜻입니다 — 기본값 제거
    # 대상이 아닙니다. 근거는 아래 docstring.
    months: list[str] | None = None,
) -> dict[str, np.ndarray]:
    """실측 trip_miles / trip_time 부트스트랩 풀 로드.

    `analysis.md`와 동일한 유효성 필터(트립 시간/거리/기사 페이 범위)를 적용합니다.
    월별로 `sample_per_month`건만 무작위 추출해 메모리 부담을 줄입니다.

    `months` 를 비우면 `bronze_dir` 에 실제로 있는 달을 씁니다. 달 목록을 상수로
    두면 TLC 공개 지연(두 달쯤, 폭도 일정하지 않음) 탓에 그 상수를 만족하는
    사람이 없어 기본값으로는 아무도 못 돌립니다. 대신 어떤 달을 썼는지에 따라
    결과가 달라지므로, 고른 달을 로그로 남기고 재현이 필요하면 명시하세요.
    """
    if months is None:
        months = discover_bootstrap_months(bronze_dir)
        logger.info("부트스트랩 월 자동 선택: %s", ", ".join(months))

    rng = np.random.default_rng(seed)
    miles_chunks: list[np.ndarray] = []
    time_chunks: list[np.ndarray] = []

    for year_month in months:
        latest = _latest_partition_file(bronze_dir, year_month)
        if latest is None:
            continue

        # 컬럼을 읽은 뒤 자르면 안 됩니다 — 25개 전부 pandas 로 올라가고, 그중
        # 문자열 8개가 object dtype 으로 폭증해 드라이버가 OOM 으로 죽었습니다 (#894).
        df = read_parquet_uri(latest, columns=BOOTSTRAP_COLUMNS)
        valid = (
            df["trip_miles"].notna() & (df["trip_miles"] > 0) & (df["trip_miles"] <= 1000)
            & df["trip_time"].notna() & (df["trip_time"] > 0) & (df["trip_time"] <= 86400)
            & df["driver_pay"].notna() & (df["driver_pay"] >= 0) & (df["driver_pay"] <= 5000)
        )
        df = df.loc[valid]

        n = min(sample_per_month, len(df))
        idx = rng.choice(len(df), size=n, replace=False)
        miles_chunks.append(df["trip_miles"].to_numpy()[idx])
        time_chunks.append((df["trip_time"].to_numpy()[idx] / 60.0))  # 분 단위

    if not miles_chunks:
        raise FileNotFoundError(
            f"부트스트랩 풀을 만들 bronze 파티션을 찾지 못했습니다: {bronze_dir}, months={months}"
        )

    return {
        "trip_miles": np.concatenate(miles_chunks),
        "trip_time_min": np.concatenate(time_chunks),
    }


def sample_driver_traits(
    n_drivers: int,
    bootstrap_pools: dict[str, np.ndarray],
    today: np.datetime64,
    seed: int | None,
) -> pd.DataFrame:
    """기사 1만 명(기본값)의 성향 샘플링. `implementation_plan.md` §1."""
    rng = np.random.default_rng(seed)

    work_mean_h = rng.gamma(shape=6.0, scale=1.2, size=n_drivers)
    work_cv = rng.uniform(0.30, 0.40, size=n_drivers)

    active_days_count = rng.choice(
        [3, 4, 5, 6, 7], size=n_drivers, p=[0.15, 0.20, 0.25, 0.25, 0.15]
    )

    weekday_p = WEEKDAY_WEIGHTS / WEEKDAY_WEIGHTS.sum()
    active_weekdays = [
        sorted(rng.choice(7, size=int(k), replace=False, p=weekday_p).tolist())
        for k in active_days_count
    ]

    distance_pref_mi = rng.choice(bootstrap_pools["trip_miles"], size=n_drivers, replace=True)
    avg_trip_duration_min = rng.choice(bootstrap_pools["trip_time_min"], size=n_drivers, replace=True)

    time_pref = rng.dirichlet(alpha=8.0 * TIME_BLOCK_WEIGHTS, size=n_drivers)

    rest_frac = rng.uniform(0.05, 0.15, size=n_drivers)
    idle_frac = rng.uniform(0.15, 0.35, size=n_drivers)

    churn_flag = rng.binomial(1, 0.25, size=n_drivers).astype(bool)

    # 가입일: [오늘-1095일, 오늘-14일] — 최소 14일 tenure 보장
    tenure_floor_days = 14
    join_offset_days = rng.integers(tenure_floor_days, 1095, size=n_drivers)
    joined_at = today - join_offset_days.astype("timedelta64[D]")

    first_names = rng.choice(FIRST_NAMES, size=n_drivers)
    last_names = rng.choice(LAST_NAMES, size=n_drivers)
    driver_names = [f"{f} {l}" for f, l in zip(first_names, last_names)]

    return pd.DataFrame({
        "work_mean_h": work_mean_h,
        "work_cv": work_cv,
        "active_days_count": active_days_count,
        "active_weekdays": active_weekdays,
        "distance_pref_mi": distance_pref_mi,
        "avg_trip_duration_min": avg_trip_duration_min,
        "time_pref": list(time_pref),
        "rest_frac": rest_frac,
        "idle_frac": idle_frac,
        "churn_flag": churn_flag,
        "joined_at": joined_at,
        "driver_name": driver_names,
    })
