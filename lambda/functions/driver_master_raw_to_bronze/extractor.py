"""기사 마스터 테이블 월별 신규·탈퇴 배치 수집(extract).

`spark/jobs/driver_master`(이슈 #160)는 최초 1만 명 시드를 통계적 부트스트랩으로
1회 생성하는 용도이고, 이 모듈은 그 시드(또는 전월 스냅샷)를 이어받아 매달 몇 명이
빠지고(탈퇴) 몇 명이 새로 들어오는지(신규)만 가볍게 반영합니다. 신규 기사의 성향
필드는 spark 쪽 부트스트랩(HVFHV 실측 분포) 대신 관측된 값 범위 안에서 균등 난수로
생성합니다 — spark/ 는 별도 uv 프로젝트라 lambda/ 에서 직접 import 하지 않습니다.

기준 스냅샷 선택 순서:
1. 전월 파티션(`data/bronze/driver_master/year_month=YYYY-MM/`)에 parquet 이 있으면 그중 최신 파일
2. 없으면(최초 실행) 시드 CSV(`data/bronze/driver_master.csv`)
"""

import calendar
import csv
import logging
import random
import uuid
from datetime import datetime
from pathlib import Path

import pyarrow.parquet as pq
from pipeline_core.extractor import Extractor

logger = logging.getLogger(__name__)

DATASET = "driver_master"
DEFAULT_SEED_PATH = "data/bronze/driver_master.csv"

# 한 달에 최대로 빠지고/들어오는 기사 수. 사용자 지정값.
MAX_MONTHLY_CHURN = 30
MAX_MONTHLY_JOIN = 40

DISTANCE_LABELS = ["SHORT", "MEDIUM", "LONG"]
TIME_BLOCK_LABELS = ["00-03", "03-06", "06-09", "09-12", "12-15", "15-18", "18-21", "21-24"]
WEEKDAY_LABELS = ["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"]

# 신규 기사 이름용 하드코딩 풀 (신규 의존성 추가 안 하려고 spark/traits.py 와 별도로 둠)
FIRST_NAMES = [
    "James", "Michael", "Robert", "John", "David", "William", "Richard", "Joseph",
    "Mary", "Patricia", "Jennifer", "Linda", "Elizabeth", "Barbara", "Susan", "Jessica",
]
LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
]

# 시드 CSV(spark 쪽이 관측한 1만 명) 값 범위를 참고한, 신규 기사 성향 난수 범위.
IDLE_SECONDS_RANGE = (1_000.0, 20_000.0)
TRIP_COUNT_RANGE = (1, 60)
WORK_MINUTES_RANGE = (80.0, 1_300.0)
REST_MINUTES_RANGE = (5.0, 150.0)


def _load_seed_csv(path: str) -> list[dict]:
    with open(path, newline="", encoding="utf-8") as f:
        return [_parse_seed_row(row) for row in csv.DictReader(f)]


def _parse_seed_row(row: dict) -> dict:
    return {
        "driver_id": row["driver_id"],
        "driver_name": row["driver_name"],
        "primary_distance_bands": row["primary_distance_bands"],
        "primary_time_blocks": row["primary_time_blocks"],
        "active_weekdays": row["active_weekdays"],
        "max_idle_seconds": float(row["max_idle_seconds"]),
        "min_idle_seconds": float(row["min_idle_seconds"]),
        "max_trip_count": int(row["max_trip_count"]),
        "min_trip_count": int(row["min_trip_count"]),
        "min_work_minutes": float(row["min_work_minutes"]),
        "max_work_minutes": float(row["max_work_minutes"]),
        "max_rest_minutes": float(row["max_rest_minutes"]),
        "min_rest_minutes": float(row["min_rest_minutes"]),
        "churned_at": _parse_datetime(row["churned_at"]),
        "joined_at": _parse_datetime(row["joined_at"]),
    }


def _parse_datetime(value: str) -> datetime | None:
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")


def _prev_year_month(year: int, month: int) -> tuple[int, int]:
    return (year - 1, 12) if month == 1 else (year, month - 1)


def _random_date_in_month(year: int, month: int, days_in_month: int, rng: random.Random) -> datetime:
    return datetime(year, month, rng.randint(1, days_in_month))


def _random_labels(rng: random.Random, labels: list[str], k_min: int, k_max: int) -> str:
    k = rng.randint(k_min, min(k_max, len(labels)))
    chosen = rng.sample(labels, k)
    return "|".join(sorted(chosen, key=labels.index))


def _random_name(rng: random.Random) -> str:
    return f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}"


def _generate_new_driver(year: int, month: int, days_in_month: int, rng: random.Random) -> dict:
    min_idle = rng.uniform(*IDLE_SECONDS_RANGE)
    max_idle = rng.uniform(min_idle, IDLE_SECONDS_RANGE[1])
    min_trip = rng.randint(*TRIP_COUNT_RANGE)
    max_trip = rng.randint(min_trip, TRIP_COUNT_RANGE[1])
    min_work = rng.uniform(*WORK_MINUTES_RANGE)
    max_work = rng.uniform(min_work, WORK_MINUTES_RANGE[1])
    min_rest = rng.uniform(*REST_MINUTES_RANGE)
    max_rest = rng.uniform(min_rest, REST_MINUTES_RANGE[1])

    return {
        "driver_id": str(uuid.uuid4()),
        "driver_name": _random_name(rng),
        "primary_distance_bands": _random_labels(rng, DISTANCE_LABELS, 1, 3),
        "primary_time_blocks": _random_labels(rng, TIME_BLOCK_LABELS, 1, 4),
        "active_weekdays": _random_labels(rng, WEEKDAY_LABELS, 3, 7),
        "max_idle_seconds": max_idle,
        "min_idle_seconds": min_idle,
        "max_trip_count": max_trip,
        "min_trip_count": min_trip,
        "min_work_minutes": min_work,
        "max_work_minutes": max_work,
        "max_rest_minutes": max_rest,
        "min_rest_minutes": min_rest,
        "churned_at": None,
        "joined_at": _random_date_in_month(year, month, days_in_month, rng),
    }


class DriverMasterExtractor(Extractor):
    """전월 스냅샷(또는 시드)을 읽어 이번 달 신규·탈퇴를 반영한 기사 마스터 행 목록을 만듭니다."""

    name = "driver_master"

    def __init__(
        self,
        year: str | int,
        month: str | int,
        base_dir: str,
        seed_path: str = DEFAULT_SEED_PATH,
        rng: random.Random | None = None,
    ):
        self._year = int(year)
        self._month = int(month)
        self._base_dir = base_dir
        self._seed_path = seed_path
        self._rng = rng or random.Random()

    def _load_base_snapshot(self) -> list[dict]:
        prev_year, prev_month = _prev_year_month(self._year, self._month)
        prev_year_month = f"{prev_year:04d}-{prev_month:02d}"
        partition_dir = Path(self._base_dir) / DATASET / f"year_month={prev_year_month}"
        files = sorted(partition_dir.glob("*.parquet")) if partition_dir.exists() else []
        if files:
            return pq.ParquetFile(files[-1]).read().to_pylist()

        dataset_dir = Path(self._base_dir) / DATASET
        # 시드 폴백은 driver_master 이력이 전혀 없는 최초 실행에서만 허용합니다.
        # 다른 달 파티션이 이미 있는데 바로 전월만 없으면(건너뛴 달) 시드로 조용히
        # 리셋되지 않도록 명시적으로 실패시킵니다 — 순차 실행을 강제합니다.
        if dataset_dir.exists() and any(dataset_dir.glob("year_month=*/*.parquet")):
            raise Exception(
                f"전월({prev_year_month}) 파티션이 없습니다. "
                "driver_master는 순차 실행이 필요합니다 — 건너뛴 달을 먼저 채우세요."
            )
        return _load_seed_csv(self._seed_path)

    def extract(self) -> list[dict]:
        rows = self._load_base_snapshot()
        days_in_month = calendar.monthrange(self._year, self._month)[1]

        active_idx = [i for i, row in enumerate(rows) if row["churned_at"] is None]
        n_churn = min(self._rng.randint(0, MAX_MONTHLY_CHURN), len(active_idx))
        for i in self._rng.sample(active_idx, n_churn):
            rows[i]["churned_at"] = _random_date_in_month(self._year, self._month, days_in_month, self._rng)

        n_join = self._rng.randint(0, MAX_MONTHLY_JOIN)
        rows.extend(
            _generate_new_driver(self._year, self._month, days_in_month, self._rng) for _ in range(n_join)
        )

        logger.info(
            "driver_master_extract done base=%d churned=%d joined=%d total=%d",
            len(rows) - n_join,
            n_churn,
            n_join,
            len(rows),
        )
        return rows
