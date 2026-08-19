"""기사 계약 Bronze→Silver 정제·적재 시나리오.

1. Extract → 정제 → 원자적 Load 로 월 파티션 파일 하나 생성
2. 같은 월 재실행은 파일을 늘리지 않고 덮어씀
3. 리스 키 중복·기간 중첩은 적재 전에 실패
4. 교체 중 실패해도 기존 월 파일이 남음
"""

from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from schema.silver.driver_vehicle_leases import SCHEMA
from functions.driver_master_bronze_to_silver.handler import lambda_handler
from functions.driver_master_bronze_to_silver.loader import (
    DriverVehicleLeaseSilverLoader,
)


YEAR_MONTH = "2026-08"


def _rows():
    return [
        {
            "lease_id": "lease-1",
            "customer_id": "customer-1",
            "driver_id": "driver-1",
            "taxi_id": "taxi-1",
            "make_key": "KIA",
            "model_key": "SPORTAGE",
            "model_year": 2023,
            "lease_started_on": date(2024, 1, 1),
            "lease_ended_on": None,
        }
    ]


def _bronze(tmp_path: Path, rows: list[dict]) -> Path:
    path = tmp_path / "bronze.parquet"
    pq.write_table(pa.Table.from_pylist(rows), path)
    return path


def _event(tmp_path: Path, bronze: Path) -> dict:
    return {
        "bronze_path": str(bronze),
        "year_month": YEAR_MONTH,
        "silver_dir": str(tmp_path / "silver"),
    }


def test_정제한_기사계약을_월파티션_한파일로_적재한다(tmp_path):
    rows = _rows()
    rows[0]["make_key"] = " kia "
    rows[0]["model_key"] = " sportage "

    result = lambda_handler(_event(tmp_path, _bronze(tmp_path, rows)))

    path = Path(result["locations"][0])
    assert path == (
        tmp_path / "silver" / f"year_month={YEAR_MONTH}" / "driver_vehicle_leases.parquet"
    )
    assert result["row_count"] == 1
    assert pq.read_schema(path) == SCHEMA
    written = pq.ParquetFile(path).read().to_pylist()[0]
    assert (written["make_key"], written["model_key"]) == ("KIA", "SPORTAGE")


def test_같은월을_다시_정제해도_파일이_늘지않는다(tmp_path):
    bronze = _bronze(tmp_path, _rows())

    first = lambda_handler(_event(tmp_path, bronze))
    second = lambda_handler(_event(tmp_path, bronze))

    assert first == second
    assert len(list((tmp_path / "silver").rglob("*.parquet"))) == 1


@pytest.mark.parametrize("broken", ["duplicate", "taxi_overlap", "driver_overlap"])
def test_중복키나_리스기간중첩은_적재하지_않는다(tmp_path, broken):
    rows = _rows()
    second = {**rows[0], "lease_id": "lease-2"}
    if broken == "duplicate":
        second["lease_id"] = "lease-1"
        second["driver_id"] = "driver-2"
        second["taxi_id"] = "taxi-2"
    elif broken == "taxi_overlap":
        second["driver_id"] = "driver-2"
    else:
        second["taxi_id"] = "taxi-2"
    rows.append(second)

    with pytest.raises(ValueError, match="중복|기간이 겹칩니다"):
        lambda_handler(_event(tmp_path, _bronze(tmp_path, rows)))

    assert not list((tmp_path / "silver").rglob("*.parquet"))


@pytest.mark.parametrize(
    ("broken", "message"),
    [
        ("missing_column", "필수 컬럼 누락"),
        ("empty_required", "필수값"),
        ("model_year", "model_year"),
        ("ended_before_started", "리스 종료일"),
    ],
)
def test_계약_품질이_깨지면_적재하지_않는다(tmp_path, broken, message):
    rows = _rows()
    if broken == "missing_column":
        rows = [{k: v for k, v in rows[0].items() if k != "driver_id"}]
    elif broken == "empty_required":
        rows[0]["driver_id"] = "   "
    elif broken == "model_year":
        rows[0]["model_year"] = 1800
    else:
        rows[0]["lease_ended_on"] = date(2023, 12, 31)

    with pytest.raises(ValueError, match=message):
        lambda_handler(_event(tmp_path, _bronze(tmp_path, rows)))

    assert not list((tmp_path / "silver").rglob("*.parquet"))


def test_교체중_실패해도_기존월파일과_임시파일이_남지않는다(tmp_path, monkeypatch):
    bronze = _bronze(tmp_path, _rows())
    first = lambda_handler(_event(tmp_path, bronze))
    target = Path(first["locations"][0])
    before = target.read_bytes()

    def fail_replace(source, destination):
        raise OSError("교체 실패")

    monkeypatch.setattr(type(target), "replace", fail_replace)
    with pytest.raises(OSError, match="교체 실패"):
        lambda_handler(_event(tmp_path, bronze))

    assert target.read_bytes() == before
    assert not list(target.parent.glob("*.tmp"))


def test_Silver스키마가_아닌_테이블은_적재하지_않는다(tmp_path):
    loader = DriverVehicleLeaseSilverLoader(str(tmp_path / "silver"), YEAR_MONTH)

    with pytest.raises(ValueError, match="Silver 스키마와 다릅니다"):
        loader.write(pa.Table.from_pylist([{"lease_id": "lease-1"}]))

    assert not list((tmp_path / "silver").rglob("*.parquet"))


@pytest.mark.parametrize(
    ("event", "message"),
    [
        ({"year_month": YEAR_MONTH}, "bronze_path"),
        ({"bronze_path": "bronze.parquet", "year_month": "2026-8"}, "year_month"),
    ],
)
def test_필수_이벤트값이_없으면_읽기전에_실패한다(event, message):
    with pytest.raises(ValueError, match=message):
        lambda_handler(event)
