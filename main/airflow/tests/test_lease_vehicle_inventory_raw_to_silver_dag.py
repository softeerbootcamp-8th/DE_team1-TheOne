"""보유 차량 Raw→Bronze→Silver 분기 계약.

1. 기사 계약 분기와 섞이지 않는 독립 네 단계
2. 보유 차량 수집 Lambda에 제공 주소 파라미터 전달
3. 필수 컬럼 누락 시 원천부터 한 번 재수집
4. 재고 품질(고유 ID·양수 값)과 같은 월 재실행 Silver 검증
"""

from datetime import timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from dags import driver_master_raw_to_silver_dag as dag_module
from schema.silver.lease_vehicle_inventory import SCHEMA
from main.airflow.scripts.lease_vehicle_inventory_raw_to_silver import (
    tasks as task_module,
)


DAG = dag_module.driver_master_raw_to_silver_dag
INVENTORY_TASK_IDS = {
    "inventory_raw_to_bronze",
    "validate_inventory_bronze",
    "inventory_bronze_to_silver",
    "validate_inventory_silver",
}


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
            "weekly_price_usd": 350.0,
            "image_url": "http://images.example/kia-sportage.png",
            "stock": 12,
        }
    ]


def test_보유차량분기는_기사계약분기와_독립적으로_Silver까지_처리한다():
    assert INVENTORY_TASK_IDS <= set(DAG.task_ids)
    assert DAG.get_task("inventory_raw_to_bronze").upstream_task_ids == set()
    assert DAG.get_task("inventory_raw_to_bronze").downstream_task_ids == {
        "validate_inventory_bronze"
    }
    assert DAG.get_task("validate_inventory_bronze").downstream_task_ids == {
        "inventory_bronze_to_silver",
        "validate_inventory_silver",
    }
    assert DAG.get_task("inventory_bronze_to_silver").downstream_task_ids == {
        "validate_inventory_silver"
    }
    # 한 분기가 죽어도 다른 분기는 그대로 돌아야 해서 두 분기를 잇지 않습니다.
    assert not INVENTORY_TASK_IDS & DAG.get_task("validate_silver").upstream_task_ids
    assert DAG.get_task("inventory_raw_to_bronze").retries == 2
    assert DAG.get_task("inventory_raw_to_bronze").retry_delay == timedelta(minutes=5)
    assert DAG.get_task("validate_inventory_bronze").retries == 0
    assert DAG.get_task("validate_inventory_silver").retries == 0


def test_보유차량수집task는_제공주소를_보유차량핸들러에_전달한다(monkeypatch):
    called = {}
    handlers = []

    def handler(*, event):
        called.update(event)
        return {"year_month": "2026-08"}

    monkeypatch.setattr(
        task_module,
        "lambda_handler_for",
        lambda name: handlers.append(name) or handler,
    )
    DAG.get_task("inventory_raw_to_bronze").python_callable(
        params={
            "api_base_url": "http://source",
            "base_dir": "/bronze",
            "year": "2026",
            "month": "8",
        }
    )
    assert handlers == ["lease_vehicle_inventory_raw_to_bronze"]
    assert called == {
        "api_base_url": "http://source",
        "base_dir": "/bronze",
        "year": "2026",
        "month": "8",
    }


def test_보유차량필수컬럼이_누락되면_원천부터_다시_수집한다(monkeypatch):
    results = iter(
        [
            (Path("broken.parquet"), ["stock"]),
            (Path("corrected.parquet"), []),
        ]
    )
    recollected = {"year_month": "2026-08", "row_count": 1}
    calls = []
    monkeypatch.setattr(
        task_module,
        "_validate_bronze_result",
        lambda result, base_dir: next(results),
    )
    monkeypatch.setattr(
        task_module,
        "_collect_bronze",
        lambda params: calls.append(params) or recollected,
    )

    validated = DAG.get_task("validate_inventory_bronze").python_callable(
        {"year_month": "2026-08"},
        params={"base_dir": "/bronze", "api_base_url": "http://source"},
    )

    assert validated == recollected
    assert calls == [{"base_dir": "/bronze", "api_base_url": "http://source"}]


def test_보유차량을_정제해_같은월Silver로_멱등적재한다(tmp_path):
    rows = _rows()
    rows[0]["manufacturer"] = " kia "
    rows[0]["model_name"] = " sportage "
    bronze = tmp_path / "bronze.parquet"
    pq.write_table(pa.Table.from_pylist(rows), bronze)

    first = task_module.clean_bronze_to_silver(bronze, tmp_path / "silver", "2026-08")
    second = task_module.clean_bronze_to_silver(bronze, tmp_path / "silver", "2026-08")

    assert first == second
    path = (
        tmp_path / "silver" / "year_month=2026-08" / "lease_vehicle_inventory.parquet"
    )
    assert pq.read_schema(path) == SCHEMA
    assert pq.ParquetFile(path).metadata.num_rows == 1
    written = pq.ParquetFile(path).read().to_pylist()[0]
    assert (written["manufacturer"], written["model_name"]) == ("KIA", "SPORTAGE")
    assert len(list(path.parent.glob("*.parquet"))) == 1
    task_module.validate_silver_result(first, 1)


@pytest.mark.parametrize(
    ("broken", "message"),
    [
        ("duplicate_model_id", "중복"),
        ("zero_stock", "0 이하"),
        ("zero_price", "0 이하"),
        ("zero_efficiency", "0 이하"),
        ("empty_image_url", "필수값"),
    ],
)
def test_재고품질이_깨지면_Silver적재전에_실패한다(tmp_path, broken, message):
    rows = _rows()
    if broken == "duplicate_model_id":
        rows.append({**rows[0], "model_year": 2024})
    elif broken == "zero_stock":
        rows[0]["stock"] = 0
    elif broken == "zero_price":
        rows[0]["weekly_price_usd"] = 0.0
    elif broken == "zero_efficiency":
        rows[0]["fuel_efficiency"] = 0.0
    else:
        rows[0]["image_url"] = "   "
    bronze = tmp_path / "bronze.parquet"
    pq.write_table(pa.Table.from_pylist(rows), bronze)

    with pytest.raises(ValueError, match=message):
        task_module.clean_bronze_to_silver(bronze, tmp_path / "silver", "2026-08")

    assert not list((tmp_path / "silver").rglob("*.parquet"))


def test_컬럼이_빠진_Bronze는_Silver로_넘기지_않는다(tmp_path):
    rows = [
        {key: value for key, value in _rows()[0].items() if key != "stock"}
    ]
    bronze = tmp_path / "bronze.parquet"
    pq.write_table(pa.Table.from_pylist(rows), bronze)

    with pytest.raises(ValueError, match="필수 컬럼 누락"):
        task_module.clean_bronze_to_silver(bronze, tmp_path / "silver", "2026-08")
