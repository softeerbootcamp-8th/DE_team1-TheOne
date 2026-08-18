"""가상 회사 고객·택시·리스 원천 스냅샷 생성 CLI.

실행: ``cd main/spark && PYTHONPATH=../.. uv run --frozen python -m sub.scripts.synthetic_company_snapshot.generate``
"""

import argparse
from datetime import date
from pathlib import Path

import pandas as pd

from sub.scripts.synthetic_company_snapshot.snapshot import (
    DEFAULT_LEASE_START_MIN,
    DEFAULT_SNAPSHOT_DATE,
    build_company_snapshot,
    build_driver_ids,
    build_vehicle_pool,
    evolve_company_snapshot,
    read_snapshot,
    write_snapshot,
)

# 실행 위치가 아니라 이 파일 위치로 저장소 루트를 확정합니다. Makefile은
# main/spark에서 실행하므로 상대경로를 쓰면 main/data를 보게 됩니다.
PROJECT_ROOT = Path(__file__).resolve().parents[3]

# vehicle_master 는 네 개 Silver 를 조인해 만드는 파생 Silver 라 Bronze 가 없습니다
# (lambda/functions/common/vehicle_master_layout.py 참고). 경로에 생성일이 들어가서
# 고정값으로 두면 다음 수집일에 낡으므로, 데이터셋 디렉터리에서 최신을 골라 씁니다.
_VEHICLE_MASTER_DIR = PROJECT_ROOT / "data" / "silver" / "vehicle_master"
_VEHICLE_MASTER_FILE = "vehicle_master.parquet"


def resolve_vehicle_master_path(dataset_dir: str | Path) -> Path:
    """가장 최신 `collected_date=` 파티션의 vehicle_master 를 고릅니다.

    ISO 날짜라 이름 정렬이 곧 시간 정렬입니다. 도시가 여러 개면 어느 쪽을 쓸지
    정할 근거가 없으므로 조용히 고르지 않고 실패시킵니다.
    """
    partitions = sorted(Path(dataset_dir).glob("collected_date=*"))
    if not partitions:
        raise FileNotFoundError(
            f"vehicle_master Silver 가 없습니다: {dataset_dir}. "
            "vehicle_master_silver DAG 를 먼저 돌리거나 --vehicle_master_path 로 직접 지정하세요."
        )

    latest = partitions[-1]
    files = sorted(latest.glob(f"city=*/{_VEHICLE_MASTER_FILE}"))
    if not files:
        raise FileNotFoundError(f"도시 파티션이 비어 있습니다: {latest}")
    if len(files) > 1:
        raise ValueError(
            f"도시가 여러 개라 하나를 고를 수 없습니다. --vehicle_master_path 로 직접 지정하세요: "
            f"{[str(f) for f in files]}"
        )
    return files[0]


def main(args_list: list[str] | None = None):
    parser = argparse.ArgumentParser(description="합성 회사 원천 DB 스냅샷 생성")
    parser.add_argument(
        "--previous_snapshot_dir",
        default=None,
        help="지정하면 초기 생성 대신 이 파티션을 기준으로 월별 스냅샷 갱신",
    )
    parser.add_argument(
        "--vehicle_master_path",
        default=None,
        help=f"비우면 {_VEHICLE_MASTER_DIR} 의 최신 collected_date 파티션을 씁니다",
    )
    parser.add_argument("--output_dir", default="../data/source/company")
    # 기본값과 그 이유는 `snapshot.py` 가 소유합니다 — 두 곳에 적어두면 갈립니다.
    parser.add_argument("--snapshot_date", default=DEFAULT_SNAPSHOT_DATE.isoformat())
    parser.add_argument("--lease_start_min", default=DEFAULT_LEASE_START_MIN.isoformat())
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--model_year", type=int, default=2023)
    parser.add_argument("--change_rate", type=float, default=None)
    args = parser.parse_args(args_list)

    snapshot_date = date.fromisoformat(args.snapshot_date)
    vehicle_master_path = (
        Path(args.vehicle_master_path) if args.vehicle_master_path
        else resolve_vehicle_master_path(_VEHICLE_MASTER_DIR)
    )
    vehicle_pool = build_vehicle_pool(
        pd.read_parquet(vehicle_master_path),
        model_year=args.model_year,
    )
    if args.previous_snapshot_dir:
        tables = evolve_company_snapshot(
            read_snapshot(args.previous_snapshot_dir),
            vehicle_pool,
            seed=args.seed,
            snapshot_date=snapshot_date,
            change_rate=args.change_rate,
        )
    else:
        tables = build_company_snapshot(
            build_driver_ids(),
            vehicle_pool,
            seed=args.seed,
            snapshot_date=snapshot_date,
            lease_start_min=date.fromisoformat(args.lease_start_min),
        )
    paths = write_snapshot(tables, args.output_dir, snapshot_date)
    print(f"합성 회사 원천 스냅샷 생성 완료: {', '.join(map(str, paths))}")
    return paths


if __name__ == "__main__":
    main()
