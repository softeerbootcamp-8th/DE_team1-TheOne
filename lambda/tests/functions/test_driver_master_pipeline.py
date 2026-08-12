"""회사 원천 DB 스냅샷 Raw → Bronze 적재 시나리오.

1. 세 원천 파일 → 테이블별 날짜 파티션과 응답 행 수
2. 원천 → Bronze 스키마·행·값 무변형
3. PK/FK 위반 → 명시적 실패
4. 파일 누락 또는 0행 → 전체 실패
5. 요청일과 데이터 snapshot_date 불일치 → 실패
6. 같은 스냅샷 재수집 → 기존 파일 보존
"""

from datetime import date, datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from functions.driver_master_raw_to_bronze.source_snapshot import CompanySnapshotExtractor
from functions.driver_master_raw_to_bronze.handler import lambda_handler
from functions.driver_master_raw_to_bronze.loader import CompanySnapshotBronzeLoader

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


def test_세_원천파일을_테이블별_bronze에_변형없이_적재한다(tmp_path):
    source_dir, bronze_dir = tmp_path / "source", tmp_path / "bronze"
    source_partition = _write_source(source_dir)

    result = lambda_handler({
        "snapshot_date": SNAPSHOT_DATE,
        "source_dir": str(source_dir),
        "bronze_dir": str(bronze_dir),
    })

    assert result["row_count"] == 6
    assert result["row_counts"] == {"customer": 2, "lease_contract": 2, "taxi": 2}
    assert len(result["locations"]) == 3
    for location in result["locations"]:
        path = Path(location)
        name = path.parents[1].name
        source = pq.ParquetFile(source_partition / f"{name}.parquet").read()
        written = pq.ParquetFile(path).read()
        assert path.parent.name == f"snapshot_date={SNAPSHOT_DATE}"
        assert written.schema == source.schema
        assert written.to_pylist() == source.to_pylist()


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


def test_같은_스냅샷을_재수집해도_기존파일을_보존한다(tmp_path):
    tables = _tables()
    first = CompanySnapshotBronzeLoader(
        str(tmp_path), SNAPSHOT_DATE, datetime(2026, 8, 12, 1, tzinfo=timezone.utc)
    )
    second = CompanySnapshotBronzeLoader(
        str(tmp_path), SNAPSHOT_DATE, datetime(2026, 8, 12, 2, tzinfo=timezone.utc)
    )

    first.write(tables)
    second.write(tables)

    assert len(list((tmp_path / "company").glob("*/snapshot_date=*/*.parquet"))) == 6


def test_snapshot_date가_없으면_수집전에_실패한다(monkeypatch):
    monkeypatch.delenv("SNAPSHOT_DATE", raising=False)

    with pytest.raises(ValueError, match="snapshot_date"):
        lambda_handler({})
