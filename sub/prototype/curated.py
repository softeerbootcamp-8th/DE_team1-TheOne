"""3 · curated — 검증된 실데이터.

blue_print.md 1.3 의 "무엇이 진짜인가"에 해당하는 전부가 이 모듈을 통과합니다.
합성은 한 줄도 없습니다. 여기서 나온 차량 대장은 D2 에 따라 합성을 거치지 않고
`published_vehicle_master` 로 직행합니다.
"""

from __future__ import annotations

import glob
import shutil
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from sub.prototype import log, paths

# Uber / Lyft 의 제품명 중 이 프로젝트가 다루는 등급만. HVFHV 트립의
# `estimated_service_tier` 와 맞춰야 하고, 그 컬럼에는 Standard / Comfort /
# Extra Comfort 만 나옵니다(실측 확인).
UBER_COMFORT_PRODUCT = "Comfort"
LYFT_EXTRA_COMFORT_PRODUCT = "Extra Comfort"

# 차량 대장에 실을 연식. 렌탈사 리스팅은 연식을 주지 않아서 한 해로 고정합니다 —
# 근거 없는 가정이지만 config 로 빼지 않았습니다(D13: 이관은 단계적).
FLEET_MODEL_YEAR = 2024

# 배정이 실제로 보는 컬럼만. silver 는 26개인데 여기 19개만 씁니다 — 전체 달을
# 읽을 때 안 쓰는 컬럼 하나가 2,090만 행이면 수백 MB 입니다.
TRIP_COLUMNS = [
    "trip_key", "on_scene_datetime", "pickup_datetime", "dropoff_datetime",
    "PULocationID", "DOLocationID", "trip_miles", "trip_time",
    "base_passenger_fare", "tolls", "sales_tax", "congestion_surcharge",
    "airport_fee", "tips", "driver_pay",
    "platform_name", "estimated_service_tier",
    "pickup_borough", "pickup_zone", "dropoff_borough",
]


@dataclass(frozen=True)
class Curated:
    """4A·4B 가 소비하는 검증된 실데이터 묶음."""

    vehicle_master: pd.DataFrame  # 차량 대장 (D2 — 끝까지 실데이터)
    trips: pd.DataFrame           # curated_hvfhv_trip
    travel_minutes: dict[tuple[int, int], float]
    trip_pool: dict[str, np.ndarray]  # 부트스트랩 풀 (D8 — 그 달의 실측)


# atv_type -> 연료 구분. 에너지 단가를 휘발유 $/gal 로 곱할지 전기 $/kWh 로 곱할지가
# 여기서 갈립니다. 디젤·CNG 는 단가를 수집하지 않아 GAS 로 둡니다.
# `sub/aws_lambda/functions/vehicle_master_silver/transformer.py::_fuel_type` 와 같은
# 규칙입니다 — 그 모듈은 `pipeline_core` 와 상대 임포트를 끌고 와서 여기서 못 씁니다.
def _fuel_type(atv_types: pd.Series) -> str:
    """후보 트림의 연료가 갈리면 MIXED. 임의로 하나를 고르지 않습니다."""
    kinds = set()
    for value in atv_types:
        text = str(value or "").strip().casefold()
        if text == "ev":
            kinds.add("EV")
        elif "plug-in" in text:
            kinds.add("PHEV")
        elif "hybrid" in text:
            kinds.add("HYBRID")
        else:
            kinds.add("GAS")
    if not kinds:
        return "GAS"
    return kinds.pop() if len(kinds) == 1 else "MIXED"


def _listing_images() -> pd.DataFrame:
    """리스팅 이미지 URL. silver 카탈로그가 링크를 버려서 원천을 봅니다.

    조인 키는 표기 원문을 대문자화한 것입니다 — silver 의 `make_key`/`model_key` 와
    같은 정규화이고, 실측으로 silver 키 전부가 이 방식으로 맞습니다.
    """
    raw = paths.read_parquet_dir(
        paths.latest_partition(paths.RAW_CATALOG_DIR, "collected_date")
    )
    images = pd.DataFrame({
        "make_key": raw["make"].str.upper().str.strip(),
        "model_key": raw["model"].str.upper().str.strip(),
        "image_url": raw["image_url"],
    })
    return images.drop_duplicates(subset=["make_key", "model_key"], keep="first")


def _resolve_specs(fleet: pd.DataFrame, trims: pd.DataFrame) -> pd.DataFrame:
    """리스팅 모델명 ↔ 제원 트림명 엔티티 해소 (blue_print.md 3.3).

    `base_model_key` 를 그대로 쓸 수 없습니다. fueleconomy 는 `OUTLANDER SPORT 2WD`
    의 base 를 `OUTLANDER` 로 접어서, 리스팅의 `OUTLANDER`(주 549불)와
    `OUTLANDER SPORT`(554불)가 같은 연비를 받게 됩니다. 두 차는 다른 차입니다.

    규칙: 제원의 `model_key` 가 리스팅 모델명으로 시작하면 같은 차로 본다. 단
    한 트림이 여러 리스팅 모델명에 걸리면 **가장 긴 쪽**이 이깁니다 —
    `OUTLANDER SPORT 4WD` 는 `OUTLANDER` 가 아니라 `OUTLANDER SPORT` 입니다.
    """
    rows: list[dict] = []
    for make, models in fleet.groupby("make_key")["model_key"]:
        # 긴 이름부터 보면서 이미 더 긴 이름에 잡힌 트림은 건너뜁니다.
        for model in sorted(models.unique(), key=len, reverse=True):
            same_make = trims[trims["make_key"] == make]
            matched = same_make[
                (same_make["model_key"] == model)
                | same_make["model_key"].str.startswith(f"{model} ")
            ]
            longer = [m for m in models.unique() if len(m) > len(model) and m.startswith(model)]
            for other in longer:
                matched = matched[
                    ~(
                        (matched["model_key"] == other)
                        | matched["model_key"].str.startswith(f"{other} ")
                    )
                ]
            if matched.empty:
                continue
            rows.append({
                "make_key": make,
                "model_key": model,
                # 트림 평균. 리스팅이 트림을 특정하지 않으므로 대장은 그 모델의
                # 평균 제원을 싣습니다 — 트림별 구분은 리스팅 데이터가 트림을
                # 줄 때 넣을 일입니다.
                "combined_mpg": float(matched["combined_mpg"].mean()),
                "combined_kwh_per_100mi": float(matched["combined_kwh_per_100mi"].mean()),
                "range_miles": float(matched["range_miles"].mean()),
                "fuel_type": _fuel_type(matched["atv_type"]),
                "spec_trim_count": int(len(matched)),
            })
    return pd.DataFrame(rows)


def build_vehicle_master(*, model_year: int = FLEET_MODEL_YEAR) -> pd.DataFrame:
    """렌탈사 리스팅 × eligible list × 제원을 정합해 차량 대장을 확정합니다.

    엔티티 해소가 여기서 일어납니다. eligible list 는 크롤링 산출물이라 `make_key`
    가 `3112` 같은 내부 코드로 들어온 행이 섞여 있습니다(uber 29,825행 중 대부분).
    리스팅에 있는 (make, model) 로 내부 조인해 그 쓰레기를 자연스럽게 떨어냅니다 —
    "리스팅에 없는 차는 우리 대장에 없다"가 정합 기준입니다.
    """
    catalog = paths.read_parquet_dir(
        paths.latest_partition(paths.CURATED_CATALOG_DIR, "collected_date")
    )[["make_key", "model_key", "weekly_price_usd"]].drop_duplicates()
    uber = paths.read_parquet_dir(
        paths.latest_partition(paths.CURATED_UBER_DIR, "collected_date")
    )
    lyft = paths.read_parquet_dir(
        paths.latest_partition(paths.CURATED_LYFT_DIR, "collected_date")
    )
    specs = paths.read_parquet_dir(
        paths.latest_partition(paths.CURATED_SPECS_DIR, "collected_date")
    )

    key = ["make_key", "model_key"]
    fleet = catalog.copy()
    fleet["model_year"] = model_year

    def eligible(frame: pd.DataFrame, product: str) -> set[tuple[str, str]]:
        hit = frame[(frame["product"] == product) & (frame["min_year"] <= model_year)]
        return set(map(tuple, hit[key].drop_duplicates().to_numpy()))

    uber_comfort = eligible(uber, UBER_COMFORT_PRODUCT)
    lyft_extra = eligible(lyft, LYFT_EXTRA_COMFORT_PRODUCT)
    identity = list(map(tuple, fleet[key].to_numpy()))
    fleet["uber_comfort_eligible"] = [i in uber_comfort for i in identity]
    fleet["lyft_extra_comfort_eligible"] = [i in lyft_extra for i in identity]

    trims = specs[specs["year"] == model_year]
    if trims.empty:
        raise ValueError(f"제원에 {model_year}년식이 없습니다. 수집 DAG 를 확인하세요.")
    fleet = fleet.merge(_resolve_specs(fleet, trims), on=key, how="left")
    unmatched = fleet["combined_mpg"].isna()
    if unmatched.any():
        raise ValueError(
            "제원을 붙이지 못한 리스팅 차량이 있습니다: "
            f"{fleet.loc[unmatched, key].to_dict('records')}. "
            f"{model_year}년식 제원에 해당 모델이 없거나 표기가 다릅니다."
        )

    # 서비스 등급 자격 조합. `vehicle_group` 은 기존 Spark 경로와 같은 이름·의미로
    # 둡니다 — 배정 로직이 이 라벨로 재고를 셉니다.
    count = fleet["uber_comfort_eligible"].astype(int) + fleet["lyft_extra_comfort_eligible"].astype(int)
    fleet["vehicle_group"] = np.select(
        [count == 2, count == 1], ["BOTH", "SINGLE"], default="STANDARD"
    )
    fleet["vehicle_model_id"] = fleet["make_key"] + "|" + fleet["model_key"] + "|" + fleet["model_year"].astype(str)
    fleet = fleet.merge(_listing_images(), on=key, how="left")
    if fleet["image_url"].isna().any():
        raise ValueError(
            "리스팅 이미지를 붙이지 못한 차량이 있습니다: "
            f"{fleet.loc[fleet['image_url'].isna(), key].to_dict('records')}. "
            "원천 카탈로그의 표기와 silver 정규화 키가 갈렸습니다."
        )
    return fleet.sort_values("vehicle_model_id").reset_index(drop=True)


def load_curated_trips(target_month: str, *, part_limit: int | None) -> pd.DataFrame:
    """그 달의 curated 트립. `part_limit` 은 프로토타입용 표본 크기입니다.

    part 파일 하나가 약 49만 행입니다. 기사 2,000명의 월 수용량(약 30만 트립)과
    같은 자릿수라 매칭률을 의미 있게 재려면 part 1~2개가 적당합니다. 전체
    2,090만 행을 넣으면 매칭률은 정의상 2% 근처가 되고, 그 숫자는 배정 품질이
    아니라 기사 수 부족만 말해 줍니다.
    """
    partition = paths.CURATED_TRIP_DIR / f"year_month={target_month}"
    files = sorted(glob.glob(str(partition / "*.parquet")))
    if not files:
        raise FileNotFoundError(
            f"curated 트립 파티션이 없습니다: {partition}\n"
            "sub 가 자체 수집한 원천을 먼저 정제하세요:\n"
            f"  python -m sub.prototype.hvfhv_curate --target_month {target_month}"
        )
    if part_limit is not None:
        files = files[:part_limit]
    trips = pd.concat(
        (pd.read_parquet(f, columns=TRIP_COLUMNS) for f in files), ignore_index=True
    )
    trips = prepare_trips(trips, target_month)
    if trips.empty:
        raise ValueError(f"{target_month} 안에 드는 트립이 없습니다")
    if trips["trip_key"].duplicated().any():
        raise ValueError("curated 트립의 trip_key 가 중복입니다")
    return trips.sort_values(["pickup_datetime", "trip_key"]).reset_index(drop=True)


def trip_part_files(target_month: str, *, part_limit: int | None) -> list[str]:
    partition = paths.CURATED_TRIP_DIR / f"year_month={target_month}"
    files = sorted(glob.glob(str(partition / "*.parquet")))
    if not files:
        raise FileNotFoundError(
            f"curated 트립 파티션이 없습니다: {partition}\n"
            "sub 가 자체 수집한 원천을 먼저 정제하세요:\n"
            f"  python -m sub.prototype.hvfhv_curate --target_month {target_month}"
        )
    return files if part_limit is None else files[:part_limit]


def prepare_trips(trips: pd.DataFrame, target_month: str) -> pd.DataFrame:
    """월 경계로 자르고 배정이 쓰는 파생 컬럼을 붙입니다.

    월 경계로 자르는 이유: silver 파티션이 UTC 로 잘려 있어 전월 말·당월 말 행이
    섞여 들어옵니다. 배정은 서비스일 단위라 대상 월 밖의 날짜가 오면 그 날짜의
    기사 상태가 없습니다.
    """
    month_start = pd.Timestamp(f"{target_month}-01")
    month_end = month_start + pd.offsets.MonthBegin(1)
    inside = (trips["pickup_datetime"] >= month_start) & (trips["pickup_datetime"] < month_end)
    trips = trips.loc[inside].copy()
    if trips.empty:
        return trips
    trips["service_date"] = trips["pickup_datetime"].dt.normalize()
    trips["weekday"] = trips["pickup_datetime"].dt.dayofweek
    trips["time_block"] = trips["pickup_datetime"].dt.hour // 3
    trips["is_airport"] = (trips["airport_fee"].fillna(0) > 0).to_numpy()
    trips["is_manhattan"] = (trips["pickup_borough"] == "Manhattan").to_numpy()
    trips["PULocationID"] = trips["PULocationID"].astype(int)
    trips["DOLocationID"] = trips["DOLocationID"].astype(int)
    return trips


# 계약이 요구하는 `hvfhs_license_num`. silver 는 `platform_name` 만 남기므로
# `hvfhv_clean_transformer.py:170` 의 매핑을 되돌립니다.
LICENSE_BY_PLATFORM = {
    "Juno": "HV0002", "Uber": "HV0003", "Via": "HV0004", "Lyft": "HV0005",
}

def load_zone_names() -> pd.Series:
    """LocationID -> 구역 이름. 계약이 ID 와 이름을 둘 다 요구합니다."""
    lookup = pd.read_csv(paths.ZONE_LOOKUP)
    return lookup.set_index("LocationID")["Zone"]


def load_travel_minutes() -> dict[tuple[int, int], float]:
    """구역쌍 이동시간. 공차(deadhead) 판정의 유일한 근거입니다."""
    frame = paths.read_parquet_dir(paths.CURATED_TRAVEL_TIMES)
    return {
        (int(a), int(b)): float(c)
        for a, b, c in zip(
            frame["from_location_id"], frame["to_location_id"], frame["travel_minutes"]
        )
    }


def build_trip_pool(trips: pd.DataFrame, *, sample_size: int, seed: int) -> dict[str, np.ndarray]:
    """그 달의 실측 부트스트랩 풀 (D8 — 시점 정합).

    기사 성향의 거리·시간 분산이 실측 population 분산을 그대로 물려받게 하려는
    목적입니다. 전 기간 통합 풀을 쓰지 않습니다.
    """
    valid = trips[
        trips["trip_miles"].between(0.01, 1000, inclusive="both")
        & trips["trip_time"].between(1, 86400, inclusive="both")
    ]
    if valid.empty:
        raise ValueError("부트스트랩 풀을 만들 유효 트립이 없습니다")
    rng = np.random.default_rng(seed)
    take = min(sample_size, len(valid))
    index = rng.choice(len(valid), size=take, replace=False)
    return {
        "trip_miles": valid["trip_miles"].to_numpy()[index],
        "trip_time_min": valid["trip_time"].to_numpy()[index] / 60.0,
    }


def build_curated(
    target_month: str, *, part_limit: int | None, pool_sample: int, pool_seed: int
) -> Curated:
    trips = load_curated_trips(target_month, part_limit=part_limit)
    return Curated(
        vehicle_master=build_vehicle_master(),
        trips=trips,
        travel_minutes=load_travel_minutes(),
        trip_pool=build_trip_pool(trips, sample_size=pool_sample, seed=pool_seed),
    )


# ---------------------------------------------------------------------------
# 대용량 경로 — 서비스일로 셔플한 뒤 하루씩 흘려보냅니다
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DayPartitions:
    """서비스일별 임시 파일 + 스트리밍 중에 모은 월 단위 집계."""

    files: dict[pd.Timestamp, Path]       # 서비스일 -> 그 날 트립 파일
    trip_count: int                       # 그 달 총 트립 수
    tier_counts: dict[tuple[str, str], int]  # (플랫폼, 등급) -> 제공 트립 수
    work_dir: Path

    def __iter__(self):
        """하루치 DataFrame 을 날짜순으로 하나씩 내놓습니다.

        `yield` 가 여기 있는 이유가 이 모듈의 핵심입니다. 배정 상태가
        (기사, 서비스일) 단위라 청크는 **하루가 통째로** 들어와야 하고, 그래서
        part 파일 단위로 그냥 흘리면 안 됩니다 — 하루가 46조각으로 잘려
        `target_daily_trips` 를 46번 새로 받고 시간 겹침도 못 잡습니다.
        위 `shuffle_by_service_date` 가 그 재배치를 먼저 해 둡니다.
        """
        for day in sorted(self.files):
            yield day, pd.read_parquet(self.files[day])

    def cleanup(self) -> None:
        shutil.rmtree(self.work_dir, ignore_errors=True)


def shuffle_by_service_date(
    target_month: str, *, part_limit: int | None, work_dir: Path
) -> DayPartitions:
    """part 파일을 한 번씩만 읽어 서비스일별 파일로 흩뿌립니다.

    Spark 의 셔플과 같은 일을 로컬 디스크로 합니다. I/O 는 2배가 되지만 메모리가
    **월 전체 → part 파일 하나**로 떨어집니다. 2,090만 행을 한 프레임에 올리면
    `trip_key`(64자 문자열) 하나만 2.1GB 이고, 마지막 정렬이 그것을 통째로 복사해
    16~20GB 를 씁니다. 그래서 SIGKILL 이 납니다.
    """
    work_dir.mkdir(parents=True, exist_ok=True)
    writers: dict[pd.Timestamp, Path] = {}
    tier_counts: dict[tuple[str, str], int] = {}
    total = 0
    part_files = trip_part_files(target_month, part_limit=part_limit)
    log(f"셔플: part {len(part_files)}개를 서비스일별 파일로 -> {work_dir}")
    for index, path in enumerate(part_files):
        part = prepare_trips(pd.read_parquet(path, columns=TRIP_COLUMNS), target_month)
        if part.empty:
            log(f"셔플: part {index + 1}/{len(part_files)} 대상 월 밖 (건너뜀)")
            continue
        total += len(part)
        for (platform, tier), count in part.groupby(
            ["platform_name", "estimated_service_tier"]
        ).size().items():
            tier_counts[(platform, tier)] = tier_counts.get((platform, tier), 0) + int(count)
        for day, group in part.groupby("service_date", sort=False):
            day_dir = work_dir / f"service_date={day.date().isoformat()}"
            day_dir.mkdir(exist_ok=True)
            # part 마다 다른 파일명으로 떨궈 두고 아래에서 한 번에 읽습니다.
            group.reset_index(drop=True).to_parquet(day_dir / f"part-{index:05d}.parquet", index=False)
            writers[day] = day_dir
        # 전수 중복 검사는 2,090만 개 문자열을 set 에 올리는 일이라 여기서 못 합니다.
        # 대신 part 안에서만 봅니다 — silver 가 trip_key 를 유일하게 만들고
        # (`hvfhv_raw_to_silver`), 그 계약이 깨지면 아래 배정에서 중복 배정으로 잡힙니다.
        if part["trip_key"].duplicated().any():
            raise ValueError(f"curated 트립의 trip_key 가 중복입니다: {path}")
        log(
            f"셔플: part {index + 1}/{len(part_files)} · {len(part):,}행 -> "
            f"{part['service_date'].nunique()}일 · 누적 {total:,}행"
        )
        del part

    if not writers:
        raise ValueError(f"{target_month} 안에 드는 트립이 없습니다")
    log(f"셔플 완료: {total:,}행 / 서비스일 {len(writers)}일")
    return DayPartitions(
        files={day: directory for day, directory in writers.items()},
        trip_count=total,
        tier_counts=tier_counts,
        work_dir=work_dir,
    )


def build_trip_pool_streaming(
    target_month: str, *, part_limit: int | None, sample_size: int, seed: int
) -> dict[str, np.ndarray]:
    """부트스트랩 풀을 두 컬럼만 읽어 만듭니다 (D8).

    풀에 필요한 건 `trip_miles` / `trip_time` 뿐입니다. 두 컬럼이면 2,090만 행도
    334MB 라 한 번에 올려도 됩니다 — 19개 컬럼을 다 읽어야 할 이유가 없습니다.
    """
    files = trip_part_files(target_month, part_limit=part_limit)
    log(f"부트스트랩 풀: part {len(files)}개에서 2개 컬럼만 읽는 중")
    frames = [pd.read_parquet(path, columns=["trip_miles", "trip_time"]) for path in files]
    pool = build_trip_pool(
        pd.concat(frames, ignore_index=True), sample_size=sample_size, seed=seed
    )
    log(f"부트스트랩 풀: {len(pool['trip_miles']):,}개 표본 (D8 — 그 달 실측)")
    return pool
