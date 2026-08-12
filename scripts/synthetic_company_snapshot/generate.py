"""가상 회사 고객·택시·리스 원천 스냅샷 생성 CLI.

실행: ``cd spark && PYTHONPATH=.. uv run --frozen python ../scripts/synthetic_company_snapshot/generate.py``
"""

import argparse
from datetime import date

import pandas as pd

from scripts.synthetic_company_snapshot.snapshot import (
    build_company_snapshot,
    build_vehicle_pool,
    driver_ids_from_mapping,
    write_snapshot,
)


def main(args_list: list[str] | None = None):
    parser = argparse.ArgumentParser(description="합성 회사 원천 DB 스냅샷 생성")
    parser.add_argument("--mapping_path", default="../data/bronze/driver_trip_mapping.parquet")
    parser.add_argument(
        "--vehicle_catalog_path",
        default="../data/silver/vehicle_catalog/collected_date=2026-08-12/vendor=fasttrack/vehicle_catalog.parquet",
    )
    parser.add_argument(
        "--uber_eligibility_path",
        default="../data/silver/uber_eligible_vehicles/collected_date=2026-08-12/city=new-york/uber_eligible_vehicles.parquet",
    )
    parser.add_argument(
        "--lyft_eligibility_path",
        default="../data/silver/lyft_eligible_vehicles/collected_date=2026-08-12/city=new-york/lyft_eligible_vehicles.parquet",
    )
    parser.add_argument("--output_dir", default="../data/source/company")
    parser.add_argument("--snapshot_date", default="2026-08-12")
    parser.add_argument("--lease_start_min", default="2023-01-01")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--model_year", type=int, default=2023)
    args = parser.parse_args(args_list)

    snapshot_date = date.fromisoformat(args.snapshot_date)
    driver_ids = driver_ids_from_mapping(pd.read_parquet(args.mapping_path))
    vehicle_pool = build_vehicle_pool(
        pd.read_parquet(args.vehicle_catalog_path),
        pd.read_parquet(args.uber_eligibility_path),
        pd.read_parquet(args.lyft_eligibility_path),
        model_year=args.model_year,
    )
    tables = build_company_snapshot(
        driver_ids,
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
