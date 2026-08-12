"""회사 원천 DB 스냅샷 Raw → Bronze 적재 시나리오.

1. PK/FK 위반 → 명시적 실패
2. 파일 누락 또는 0행 → 전체 실패
3. 요청일과 데이터 snapshot_date 불일치 → 실패
"""

from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from functions.driver_master_raw_to_bronze.source_snapshot import CompanySnapshotExtractor

SNAPSHOT_DATE = "2026-08-12"


def _tables(snapshot_date: date = date(2026, 8, 12)) -> dict[str, pa.Table]:
    return {
        "customer": pa.Table.from_pylist([
            {"customer_id": "c1", "synthetic_driver_id": "DRIVER_000001", "snapshot_date": snapshot_date},
            {"customer_id": "c2", "synthetic_driver_id": "DRIVER_000002", "snapshot_date": snapshot_date},
        ]),
        "taxi": pa.Table.from_pylist([
            {
                "taxi_id": "t1", "make_key": "KIA", "model_key": "SPORTAGE",
                "model_year": 2023, "weekly_price_usd": 574.0,
                "uber_comfort_eligible": True, "lyft_extra_comfort_eligible": True,
                "vehicle_group": "BOTH", "snapshot_date": snapshot_date,
            },
            {
                "taxi_id": "t2", "make_key": "KIA", "model_key": "FORTE",
                "model_year": 2023, "weekly_price_usd": 514.0,
                "uber_comfort_eligible": False, "lyft_extra_comfort_eligible": False,
                "vehicle_group": "STANDARD", "snapshot_date": snapshot_date,
            },
        ]),
        "lease_contract": pa.Table.from_pylist([
            {
                "lease_id": "l1", "customer_id": "c1", "taxi_id": "t1",
                "lease_started_on": date(2024, 1, 1), "lease_ended_on": None,
                "snapshot_date": snapshot_date,
            },
            {
                "lease_id": "l2", "customer_id": "c2", "taxi_id": "t2",
                "lease_started_on": date(2025, 1, 1), "lease_ended_on": None,
                "snapshot_date": snapshot_date,
            },
        ]),
    }


def _write_source(base_dir: Path, tables: dict[str, pa.Table] | None = None) -> Path:
    partition = base_dir / f"snapshot_date={SNAPSHOT_DATE}"
    partition.mkdir(parents=True)
    for name, table in (tables or _tables()).items():
        pq.write_table(table, partition / f"{name}.parquet")
    return partition


@pytest.mark.parametrize("broken", ["duplicate_pk", "missing_customer_fk", "missing_taxi_fk"])
def test_pk_fk_위반은_적재전에_실패한다(tmp_path, broken):
    tables = _tables()
    if broken == "duplicate_pk":
        rows = tables["customer"].to_pylist()
        rows[1]["customer_id"] = "c1"
        tables["customer"] = pa.Table.from_pylist(rows)
    elif broken == "missing_customer_fk":
        rows = tables["lease_contract"].to_pylist()
        rows[0]["customer_id"] = "missing"
        tables["lease_contract"] = pa.Table.from_pylist(rows)
    else:
        rows = tables["lease_contract"].to_pylist()
        rows[0]["taxi_id"] = "missing"
        tables["lease_contract"] = pa.Table.from_pylist(rows)
    _write_source(tmp_path, tables)

    with pytest.raises(ValueError, match="고유|FK 위반"):
        CompanySnapshotExtractor(str(tmp_path), SNAPSHOT_DATE).extract()


def test_원천파일이_하나라도_없으면_실패한다(tmp_path):
    tables = _tables()
    tables.pop("taxi")
    _write_source(tmp_path, tables)

    with pytest.raises(FileNotFoundError, match="taxi.parquet"):
        CompanySnapshotExtractor(str(tmp_path), SNAPSHOT_DATE).extract()


def test_원천테이블이_0행이면_실패한다(tmp_path):
    tables = _tables()
    tables["taxi"] = tables["taxi"].slice(0, 0)
    _write_source(tmp_path, tables)

    with pytest.raises(ValueError, match="비어 있습니다"):
        CompanySnapshotExtractor(str(tmp_path), SNAPSHOT_DATE).extract()


def test_snapshot_date가_요청일과_다르면_실패한다(tmp_path):
    tables = _tables(date(2026, 8, 11))
    _write_source(tmp_path, tables)

    with pytest.raises(ValueError, match="snapshot_date 불일치"):
        CompanySnapshotExtractor(str(tmp_path), SNAPSHOT_DATE).extract()
