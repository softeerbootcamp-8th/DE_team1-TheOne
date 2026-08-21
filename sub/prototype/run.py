"""프로토타입 한 달 실행 + 월별 상태 체크포인트 (blue_print.md 4.3).

    python -m sub.prototype.run --target_month 2026-01 --part_limit 1

`--target_month` 를 여러 번 주면 순차 재생성입니다. 각 달의 상태 파티션이
체크포인트라서, 중간 달부터 다시 돌릴 수 있습니다 — 별도 메커니즘을 만들지
않습니다.
"""

from __future__ import annotations

import argparse
import json
import shutil
from dataclasses import replace
from pathlib import Path

import pandas as pd

from sub.config import DEFAULT_CONFIG_PATH, load_config
from sub.prototype import (
    attribution,
    curated,
    log,
    metrics,
    paths,
    published,
    synthesize,
)
from sub.run_context import RunContext
from sub.seeds import Stage, derive_seed

STATE_FILES = {
    "current": "driver_vehicle_current.parquet",
    "events": "driver_vehicle_event_all.parquet",
    "noise": "realization_noise.parquet",
}


def _state_dir(month: str) -> Path:
    return paths.STATE_DIR / f"snapshot_month={month}"


def _previous_month(month: str) -> str:
    stamp = pd.Timestamp(f"{month}-01") - pd.offsets.MonthBegin(1)
    return stamp.strftime("%Y-%m")


def read_state(month: str, run: RunContext) -> dict | None:
    """전월 체크포인트. `run_id` 가 다르면 조용히 재사용하지 않습니다 (4.3)."""
    directory = _state_dir(month)
    marker = directory / "_run.json"
    if not directory.is_dir():
        return None
    if not marker.is_file():
        raise ValueError(
            f"계보 표시가 없는 상태 파티션입니다: {directory}\n"
            f"어느 설정으로 만들었는지 알 수 없어 이어받을 수 없습니다. 지우고 다시 만드세요:\n"
            f"  rm -rf {directory}"
        )
    recorded = json.loads(marker.read_text(encoding="utf-8"))
    if recorded.get("config_hash") != run.config_hash:
        raise ValueError(
            f"전월 상태의 설정이 다릅니다: 기존 config_hash={recorded.get('config_hash')!r}, "
            f"요청={run.config_hash!r}.\n"
            f"config 를 바꾸면 그 달 이후 파티션이 전부 무효입니다 (blue_print.md 4.3). "
            f"초기 달부터 다시 돌리세요:\n  rm -rf {paths.STATE_DIR}"
        )
    return {
        name: pd.read_parquet(directory / filename)
        for name, filename in STATE_FILES.items()
        if (directory / filename).is_file()
    }


def write_state(month: str, run: RunContext, *, current, events_all, noise) -> Path:
    directory = _state_dir(month)
    directory.mkdir(parents=True, exist_ok=True)
    current.to_parquet(directory / STATE_FILES["current"], index=False)
    events_all.to_parquet(directory / STATE_FILES["events"], index=False)
    noise.to_parquet(directory / STATE_FILES["noise"], index=False)
    (directory / "_run.json").write_text(
        json.dumps(
            {"run_id": run.run_id, "config_hash": run.config_hash, "created_at": run.created_at},
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return directory


def run_month(
    target_month: str,
    *,
    config,
    part_limit: int | None,
    trip_sample: int | None = None,
    metrics_only: bool = False,
    label: str | None = None,
    jobs: int = 1,
) -> dict:
    run = RunContext.create(target_month, config)
    print(f"\n=== {target_month} · run_id={run.run_id} ===", flush=True)
    log(
        f"설정: config_hash={run.config_hash} seed={config.global_seed} "
        f"bucket_size={config.allocation.bucket_size} 기사정원={config.driver.initial_count:,} "
        f"jobs={jobs}{' metrics_only' if metrics_only else ''}"
    )

    # 3 · curated -----------------------------------------------------------
    log("curated: 차량 대장 정합 중 (리스팅 × eligible × 제원)")
    pool_seed = derive_seed(config.global_seed, Stage.MONTHLY_SAMPLE, target_month)
    vehicle_master = curated.build_vehicle_master()
    log(f"curated: 차량 대장 {len(vehicle_master)}종 확정")
    travel_minutes = curated.load_travel_minutes()
    log(f"curated: 구역쌍 이동시간 {len(travel_minutes):,}쌍")

    # 트립을 어떻게 들고 있을지가 여기서 갈립니다.
    #
    #   표본 경로   (part_limit 지정)  프레임 하나로 올린다. 지표 계산이 쉽다.
    #   전체 달 경로 (part_limit=0)     서비스일별 파일로 셔플하고 하루씩 흘린다.
    #
    # 2,090만 행을 프레임 하나로 올리면 `trip_key`(64자 문자열)만 2.1GB 이고
    # 마지막 정렬이 그것을 통째로 복사해 16~20GB 를 씁니다. SIGKILL 이 납니다.
    # 산출물 계보에 실을 입력 범위. `config_hash` 에 안 들어가는 값이라
    # manifest 에 따로 적고 재사용 판정에 씁니다.
    input_scope = (
        f"part_limit=all"
        if part_limit is None
        else f"part_limit={part_limit},trip_sample={trip_sample or 0}"
    )
    day_partitions = None
    if part_limit is None:
        work_dir = paths.PROTOTYPE / "_shuffle" / target_month
        shutil.rmtree(work_dir, ignore_errors=True)
        day_partitions = curated.shuffle_by_service_date(
            target_month, part_limit=None, work_dir=work_dir
        )
        trip_pool = curated.build_trip_pool_streaming(
            target_month, part_limit=None,
            sample_size=config.bootstrap.sample_per_month, seed=pool_seed,
        )
        trips_for_attribution = day_partitions.files
        trip_count = day_partitions.trip_count
        service_dates = sorted(day_partitions.files)
        tier_counts = day_partitions.tier_counts
        log(f"curated: 트립 {trip_count:,}행 / 서비스일 {len(service_dates)}일")
    else:
        trips = curated.load_curated_trips(target_month, part_limit=part_limit)
        # 풀을 **먼저** 만듭니다. 부트스트랩 풀은 그 달 실측 분포를 물려받아야
        # 하는데(D8), 감도 측정용 축소 표본에서 뽑으면 기사 성향이 표본 크기에
        # 딸려 움직여서 `--trip_sample` 을 바꿀 때마다 다른 기사가 됩니다.
        trip_pool = curated.build_trip_pool(
            trips, sample_size=config.bootstrap.sample_per_month, seed=pool_seed
        )
        if trip_sample and trip_sample < len(trips):
            # 픽업 시각 정렬 뒤 균등 간격으로 뽑아 월 전체를 덮습니다 — head 로
            # 자르면 월초 며칠만 남아 요일 분포가 망가집니다.
            step = len(trips) // trip_sample
            trips = trips.iloc[::step].head(trip_sample).reset_index(drop=True)
        trips_for_attribution = trips
        trip_count = len(trips)
        service_dates = sorted(trips["service_date"].unique())
        tier_counts = trips.groupby(["platform_name", "estimated_service_tier"]).size().to_dict()
        log(f"curated: 트립 {trip_count:,}행 / 서비스일 {len(service_dates)}일")

    # 4A · synthesize -------------------------------------------------------
    previous = read_state(_previous_month(target_month), run)
    log(
        "synthesize: 전월 상태 "
        + (
            f"이어받음 (기사 {len(previous['current']):,}명)"
            if previous and "current" in previous
            else f"없음 -> {_previous_month(target_month)} 초기 스냅샷을 만듭니다"
        )
    )
    result = synthesize.synthesize_month(
        target_month=target_month,
        config=config,
        vehicle_master=vehicle_master,
        trip_pool=trip_pool,
        previous_current=previous.get("current") if previous else None,
        previous_events=previous.get("events") if previous else None,
        previous_noise=previous.get("noise") if previous else None,
        fuel=synthesize.load_fuel_prices(),
    )
    active = int(result.current["exited_on"].isna().sum())
    budget = result.profiles["target_drive_minutes"]
    log(
        f"synthesize: 이벤트 {len(result.events):,}건 신규, 활성 기사 {active:,}명, "
        f"클리핑 {result.clip_rate * 100:.2f}%"
    )
    log(
        f"synthesize: 하루 운행 예산 평균 {budget.mean() / 60:.2f}h "
        f"(하한 평균 {result.profiles['min_drive_minutes'].mean() / 60:.2f}h, "
        f"상한 평균 {result.profiles['max_drive_minutes'].mean() / 60:.2f}h)"
    )

    # 이벤트 원장은 append only. 그 달 파티션에 그 달 이벤트만 씁니다 (불변).
    if not result.events.empty:
        event_dir = paths.EVENT_DIR / f"snapshot_month={target_month}"
        event_dir.mkdir(parents=True, exist_ok=True)
        result.events.to_parquet(event_dir / "events.parquet", index=False)
        log(f"이벤트 원장: {len(result.events):,}건 -> {event_dir}")

    fleet_units = synthesize.expand_fleet_units(
        synthesize.build_fleet_stock(vehicle_master, driver_count=config.driver.initial_count)
    )
    log(f"재고: 차량 {len(fleet_units):,}대 ({len(vehicle_master)}종)")

    # 4B · attribution ------------------------------------------------------
    attributed = attribution.attribute_month(
        trips_for_attribution, result.profiles, result.current, fleet_units, travel_minutes,
        global_seed=config.global_seed,
        target_month=target_month,
        bucket_size=config.allocation.bucket_size,
        score_weights=config.allocation.score_weights,
        jobs=jobs,
    )
    log(
        f"귀속 완료: {len(attributed.attributed):,}건 배정 "
        f"(후보 {attributed.candidate_rows:,}행 -> 벡터 제약 통과 {attributed.survivor_rows:,}행)"
    )

    # 5 · published ---------------------------------------------------------
    if metrics_only:
        log("published: 건너뜀 (--metrics_only)")
        release = None
    else:
        release = published.publish(
            attributed=attributed.attributed,
            current=result.current,
            fleet_units=fleet_units,
            vehicle_master=vehicle_master,
            output_dir=paths.PUBLISHED_DIR,
            run=run,
            input_scope=input_scope,
        )
        log(f"published: {release}")

    # 상태 체크포인트 -------------------------------------------------------
    events_all = (
        pd.concat([previous["events"], result.events], ignore_index=True)
        if previous and "events" in previous and not result.events.empty
        else (result.events if not previous else previous["events"])
    )
    if not metrics_only:
        state_dir = write_state(
            target_month, run,
            current=result.current, events_all=events_all, noise=result.noise_state,
        )
        log(f"상태 체크포인트: 기사 {len(result.current):,}명 / 이벤트 {len(events_all):,}건 -> {state_dir}")

    report = metrics.matching_report(
        target_month=target_month,
        run_id=run.run_id,
        trip_count=trip_count,
        service_dates=service_dates,
        tier_counts=tier_counts,
        profiles=result.profiles,
        attribution=attributed,
        clip_rate=result.clip_rate,
        bucket_size=config.allocation.bucket_size,
    )
    if day_partitions is not None:
        day_partitions.cleanup()
        log("셔플 임시 파일 삭제")
    report["trips_sampled"] = trip_sample
    report["input_scope"] = input_scope
    metrics.write_report(
        report, paths.METRICS_DIR / f"{target_month}{'_' + label if label else ''}.json"
    )
    log("리포트 작성 완료")
    print(metrics.format_report(report), flush=True)
    return report


def main(argv: list[str] | None = None) -> list[dict]:
    parser = argparse.ArgumentParser(description="blue_print.md 파이프라인 프로토타입")
    parser.add_argument("--target_month", action="append", required=True, help="YYYY-MM. 여러 번 주면 순차 실행")
    parser.add_argument("--config", default=None, help=f"비우면 {DEFAULT_CONFIG_PATH}")
    parser.add_argument("--seed", type=int, default=None, help="비우면 config 의 global_seed")
    parser.add_argument(
        "--part_limit", type=int, default=1,
        help="읽을 curated 트립 part 파일 수. 0 이면 전체 (기본 1 = 약 49만 트립)",
    )
    parser.add_argument("--bucket_size", type=int, default=None, help="비우면 config 의 allocation.bucket_size")
    parser.add_argument(
        "--trip_sample", type=int, default=None,
        help="감도 측정용 축소 표본 크기. 비우면 part 파일 전체",
    )
    parser.add_argument(
        "--metrics_only", action="store_true",
        help="published·상태 파티션을 쓰지 않고 매칭 리포트만 냅니다 (감도 스윕용)",
    )
    parser.add_argument("--label", default=None, help="리포트 파일명 접미사")
    parser.add_argument(
        "--jobs", type=int, default=1,
        help=(
            "귀속을 몇 개 프로세스로 나눌지. 서비스일 단위로 쪼개므로 하루보다 "
            "많이 쪼갤 수는 없습니다. 전체 달(2,090만 트립)이면 6~8 을 권합니다"
        ),
    )
    args = parser.parse_args(argv)

    config = load_config(args.config)
    if args.seed is not None:
        config = replace(config, global_seed=args.seed)
    if args.bucket_size is not None:
        config = replace(config, allocation=replace(config.allocation, bucket_size=args.bucket_size))

    part_limit = None if args.part_limit == 0 else args.part_limit
    return [
        run_month(
            month, config=config, part_limit=part_limit,
            trip_sample=args.trip_sample, metrics_only=args.metrics_only,
            label=args.label, jobs=args.jobs,
        )
        for month in args.target_month
    ]


if __name__ == "__main__":
    main()
