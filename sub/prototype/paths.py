"""프로토타입이 읽고 쓰는 경로. 설정이 아니라 환경입니다 (blue_print.md D13)."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "data"

# --- 입력: 이미 저장소에 있는 실데이터 -------------------------------------
# blue_print.md 는 이 계층을 raw / curated 로 부르지만(D1), 저장소의 물리 경로는
# 아직 bronze / silver 입니다. 이름 이관은 작업 9 이고 이 프로토타입 범위가
# 아닙니다 — 여기서는 경로만 한 곳에 모아 두고 의미 이름으로 부릅니다.
CURATED_TRIP_DIR = DATA / "silver" / "hvfhv"                       # curated_hvfhv_trip
CURATED_TRAVEL_TIMES = DATA / "silver" / "taxi_zone_travel_times"  # 구역쌍 이동시간
CURATED_CATALOG_DIR = DATA / "silver" / "vehicle_catalog"          # 렌탈사 리스팅
CURATED_UBER_DIR = DATA / "silver" / "uber_eligible_vehicles"
CURATED_LYFT_DIR = DATA / "silver" / "lyft_eligible_vehicles"
CURATED_SPECS_DIR = DATA / "silver" / "fueleconomy_vehicle_specs"
ZONE_LOOKUP = DATA / "bronze" / "taxi_zone_lookup.csv"

# --- 입력: silver 가 버린 컬럼만 원천에서 직접 봅니다 ----------------------
# 정제 산출물을 대체하는 것이 아니라, 산출물 계약이 요구하는데 silver 화이트리스트
# (`schema/silver/vehicle_catalog.py`)에서 빠진 컬럼만 가져옵니다 — `image_url`.
# (`on_scene_datetime` 은 `schema/silver/hvfhv.py` 가 그대로 실어 오므로 여기서
# 원천을 따로 볼 필요가 없습니다.)
RAW_CATALOG_DIR = DATA / "bronze" / "vehicle_catalog"

# --- 출력: 프로토타입 전용 트리 -------------------------------------------
# 기존 `data/source/**` 를 건드리지 않습니다. 프로토타입이 실패해도 기존 로컬
# 산출물이 남아 있어야 비교 대상이 됩니다.
PROTOTYPE = DATA / "prototype"
STATE_DIR = PROTOTYPE / "state"                 # 4.3 체크포인트
EVENT_DIR = PROTOTYPE / "driver_vehicle_event"  # append only 원장
PUBLISHED_DIR = PROTOTYPE / "published"         # 5 · data contract
METRICS_DIR = PROTOTYPE / "metrics"


def latest_partition(root: Path, prefix: str) -> Path:
    """`prefix=값` 파티션 중 이름이 가장 큰 것. ISO 표기라 이름 정렬이 시간 정렬입니다."""
    candidates = sorted(p for p in root.glob(f"{prefix}=*") if p.is_dir())
    if not candidates:
        raise FileNotFoundError(
            f"{prefix} 파티션이 없습니다: {root}\n"
            "해당 수집 DAG 를 먼저 돌리거나 data/ 를 확인하세요."
        )
    return candidates[-1]


def read_parquet_dir(path: Path):
    """디렉터리든 단일 파일이든 하나의 DataFrame 으로 읽습니다."""
    import pandas as pd

    if path.is_file():
        return pd.read_parquet(path)
    files = sorted(path.rglob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"Parquet 이 없습니다: {path}")
    return pd.concat((pd.read_parquet(f) for f in files), ignore_index=True)
