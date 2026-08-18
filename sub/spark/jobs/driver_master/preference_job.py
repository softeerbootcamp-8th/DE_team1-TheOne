"""가상 기사 선호 마스터 생성 CLI.

`job.py` 와 같은 이유로 Spark 세션을 쓰지 않습니다 — `load_bootstrap_pools` 가 Bronze
parquet 을 pandas 로 직접 읽습니다.

사용 예:
    cd main/spark && PYTHONPATH=../.. uv run --frozen python -m sub.spark.jobs.driver_master.preference_job
"""

import argparse
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

from sub.spark.jobs.driver_master.preference import build_driver_preferences, write_driver_preferences
from sub.spark.jobs.driver_master.traits import load_bootstrap_pools
from sub.scripts.synthetic_company_snapshot.snapshot import build_driver_ids


def main(args_list: list[str] | None = None) -> Path:
    parser = argparse.ArgumentParser(description="가상 기사 선호 마스터 생성")
    # 실행 위치가 spark/ 라서 저장소 루트가 한 단계 위입니다 (generate.py 와 같은 규칙).
    parser.add_argument("--output_path", default="../data/bronze/driver_preferences.parquet")
    parser.add_argument("--bronze_dir", default="../data/bronze/hvfhv")
    parser.add_argument("--sample_per_month", type=int, default=200_000)
    parser.add_argument(
        "--months", nargs="+", default=None,
        help="부트스트랩에 쓸 year_month 목록 (예: 2026-01). 비우면 Bronze 에 있는 달 전부",
    )
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--as_of_date", default=None,
        help="선호 고정 기준일 (YYYY-MM-DD). 비우면 실행 시각의 UTC 날짜 사용",
    )
    args = parser.parse_args(args_list)

    as_of_date = (
        np.datetime64(args.as_of_date) if args.as_of_date
        else np.datetime64(datetime.now(timezone.utc).date())
    )
    bootstrap_pools = load_bootstrap_pools(
        bronze_dir=args.bronze_dir, months=args.months,
        sample_per_month=args.sample_per_month, seed=args.seed,
    )
    preferences = build_driver_preferences(
        build_driver_ids(), bootstrap_pools, as_of_date=as_of_date, seed=args.seed,
    )
    path = write_driver_preferences(preferences, args.output_path)
    print(f"가상 기사 선호 마스터 생성 완료: {path} ({len(preferences)}행)")
    return path


if __name__ == "__main__":
    main()
