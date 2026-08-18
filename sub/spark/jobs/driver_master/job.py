"""기사 마스터 테이블 생성 CLI. `implementation_plan.md` §4.

Spark 세션은 쓰지 않습니다 (numpy/scipy/pandas만 사용) — `spark/` uv 환경의 scipy를
재사용하려고 이 프로젝트 안에 둡니다.

사용 예 (spark/jobs/bronze_to_silver/hvfhv/job.py와 동일한 PYTHONPATH 패턴):
    cd main/spark && PYTHONPATH=../.. uv run --frozen python -m sub.spark.jobs.driver_master.job
"""

import argparse
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

from sub.spark.jobs.driver_master.aggregate import build_driver_master_table
from sub.spark.jobs.driver_master.traits import load_bootstrap_pools, sample_driver_traits

# 리스트 필드(primary_distance_bands/primary_time_blocks/active_weekdays)를 CSV 한 셀에
# 담을 때 쓰는 구분자. 콤마는 CSV 필드 구분자와 겹쳐서 못 씁니다.
LIST_FIELD_SEP = "|"
LIST_FIELDS = ["primary_distance_bands", "primary_time_blocks", "active_weekdays"]


def main(args_list: list[str] | None = None) -> Path:
    parser = argparse.ArgumentParser(description="기사 마스터 테이블 생성")
    parser.add_argument("--n_drivers", type=int, default=10_000)
    # 실행 위치가 spark/ 라서 저장소 루트가 한 단계 위입니다 (preference_job.py 와 같은 규칙).
    parser.add_argument("--output_path", default="../data/bronze/driver_master.csv")
    parser.add_argument("--bronze_dir", default="../data/bronze/hvfhv")
    parser.add_argument("--sample_per_month", type=int, default=200_000)
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument(
        "--today", default=None,
        help="시뮬레이션 기준일 (YYYY-MM-DD). 비우면 실행 시각의 UTC 날짜 사용",
    )
    args = parser.parse_args(args_list)

    today = (
        np.datetime64(args.today) if args.today
        else np.datetime64(datetime.now(timezone.utc).date())
    )

    bootstrap_pools = load_bootstrap_pools(
        bronze_dir=args.bronze_dir, sample_per_month=args.sample_per_month, seed=args.seed or 42,
    )
    traits_df = sample_driver_traits(
        n_drivers=args.n_drivers, bootstrap_pools=bootstrap_pools, today=today, seed=args.seed,
    )
    driver_master_df = build_driver_master_table(traits_df, today=today, seed=args.seed)

    for field in LIST_FIELDS:
        driver_master_df[field] = driver_master_df[field].apply(LIST_FIELD_SEP.join)

    output_path = Path(args.output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    driver_master_df.to_csv(output_path, index=False)
    print(f"기사 마스터 테이블 생성 완료: {output_path} ({len(driver_master_df)}행)")
    return output_path


if __name__ == "__main__":
    main()
