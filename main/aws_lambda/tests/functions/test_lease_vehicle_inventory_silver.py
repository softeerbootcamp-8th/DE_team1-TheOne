"""보유 차량 Bronze→Silver 정제·적재 시나리오.

1. Extract → 정제 → 원자적 Load 로 수집 버전 파일 하나 생성
2. 같은 수집 시각 재실행은 같은 임시 파일만 덮어씀
3. 새 수집 시각은 별도 파일로 보존
4. 재고 품질 위반은 적재 전에 실패
5. 교체 중 실패해도 기존 월 파일이 남음
"""

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from schema.silver import CLEAN_LEASE_VEHICLE_INVENTORY_SCHEMA as SCHEMA
from functions.lease_vehicle_inventory_bronze_to_silver.handler import lambda_handler
from functions.lease_vehicle_inventory_bronze_to_silver.loader import (
    LeaseVehicleInventorySilverLoader,
)


YEAR_MONTH = "2026-08"
FILE_NAME = "20260821T123456123456Z.parquet"


def _rows():
    return [
        {
            "vehicle_model_id": "model-1",
            "manufacturer": "KIA",
            "model_name": "SPORTAGE",
            "model_year": 2023,
            "fuel_type": "GAS",
            "fuel_efficiency": 28.5,
            "comfort_eligible": True,
            "extra_comfort_eligible": False,
            "weekly_lease_fee": 350.0,
            "image_url": "http://images.example/kia-sportage.png",
            "stock": 12,
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
        "silver_file_name": FILE_NAME,
        "silver_dir": str(tmp_path / "silver"),
    }


def test_정제한_보유차량을_월파티션_한파일로_적재한다(tmp_path):
    rows = _rows()
    rows[0]["manufacturer"] = " kia "
    rows[0]["model_name"] = " sportage "

    result = lambda_handler(_event(tmp_path, _bronze(tmp_path, rows)))

    path = Path(result["locations"][0])
    assert path == (
        tmp_path / "silver" / f"year_month={YEAR_MONTH}" / FILE_NAME
    )
    assert result["row_count"] == 1
    assert pq.read_schema(path) == SCHEMA
    written = pq.ParquetFile(path).read().to_pylist()[0]
    # 리스 계약의 make_key·model_key 와 붙일 조인 키라 대문자로 맞춥니다.
    assert (written["manufacturer"], written["model_name"]) == ("KIA", "SPORTAGE")


def test_같은수집시각을_다시_정제해도_파일이_늘지않는다(tmp_path):
    bronze = _bronze(tmp_path, _rows())

    first = lambda_handler(_event(tmp_path, bronze))
    second = lambda_handler(_event(tmp_path, bronze))

    assert first == second
    assert len(list((tmp_path / "silver").rglob("*.parquet"))) == 1


def test_새수집시각은_별도_파일로_적재한다(tmp_path):
    bronze = _bronze(tmp_path, _rows())
    first_event = _event(tmp_path, bronze)
    second_event = {**first_event, "silver_file_name": "20260822T123456123456Z.parquet"}

    first = lambda_handler(first_event)
    second = lambda_handler(second_event)

    assert first["locations"] != second["locations"]
    assert len(list((tmp_path / "silver").rglob("*.parquet"))) == 2


@pytest.mark.parametrize(
    ("broken", "message"),
    [
        ("duplicate_model_id", "중복"),
        ("zero_stock", "0 이하"),
        ("zero_price", "0 이하"),
        ("zero_efficiency", "0 이하"),
        ("empty_image_url", "필수값"),
        ("missing_column", "필수 컬럼 누락"),
    ],
)
def test_재고품질이_깨지면_적재하지_않는다(tmp_path, broken, message):
    rows = _rows()
    if broken == "duplicate_model_id":
        rows.append({**rows[0], "model_year": 2024})
    elif broken == "zero_stock":
        rows[0]["stock"] = 0
    elif broken == "zero_price":
        rows[0]["weekly_lease_fee"] = 0.0
    elif broken == "zero_efficiency":
        rows[0]["fuel_efficiency"] = 0.0
    elif broken == "empty_image_url":
        rows[0]["image_url"] = "   "
    else:
        rows = [{k: v for k, v in rows[0].items() if k != "stock"}]

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
    loader = LeaseVehicleInventorySilverLoader(
        str(tmp_path / "silver"), YEAR_MONTH, FILE_NAME
    )

    with pytest.raises(ValueError, match="Silver 스키마와 다릅니다"):
        loader.write(pa.Table.from_pylist([{"vehicle_model_id": "model-1"}]))

    assert not list((tmp_path / "silver").rglob("*.parquet"))


@pytest.mark.parametrize(
    ("event", "message"),
    [
        ({"year_month": YEAR_MONTH}, "bronze_path"),
        ({"bronze_path": "bronze.parquet", "year_month": "2026-8"}, "year_month"),
        ({"bronze_path": "bronze.parquet", "year_month": YEAR_MONTH}, "silver_file_name"),
    ],
)
def test_필수_이벤트값이_없으면_읽기전에_실패한다(event, message):
    with pytest.raises(ValueError, match=message):
        lambda_handler(event)
