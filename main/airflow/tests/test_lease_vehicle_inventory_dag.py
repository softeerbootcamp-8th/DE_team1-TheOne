"""보유 차량 Raw→Bronze→Silver DAG 계약.

1. 기사 계약 DAG 와 분리된 네 단계 월별 DAG
2. 수집·정제 Lambda 에 파라미터 전달
3. 필수 컬럼 누락 시 원천부터 한 번 재수집
4. Bronze 행 수·스키마·재고 품질로 Silver 확인
"""

from datetime import timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from dags import lease_vehicle_inventory_raw_to_silver_dag as dag_module
from dags.driver_master_raw_to_silver_dag import driver_master_raw_to_silver_dag
from schema.silver.lease_vehicle_inventory import SCHEMA
from main.airflow.scripts.lease_vehicle_inventory_raw_to_silver import (
    tasks as task_module,
)


DAG = dag_module.lease_vehicle_inventory_raw_to_silver_dag


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


def _silver_file(tmp_path: Path, rows: list[dict]) -> dict:
    path = tmp_path / "year_month=2026-08" / "lease_vehicle_inventory.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows, schema=SCHEMA), path)
    return {"locations": [str(path)], "row_count": len(rows), "year_month": "2026-08"}


def test_보유차량은_기사계약과_분리된_DAG에서_Silver까지_처리한다():
    assert DAG.dag_id == "lease_vehicle_inventory_raw_to_silver_pipeline"
    assert DAG.schedule == "0 0 10 * *"
    assert set(DAG.task_ids) == {
        "raw_to_bronze",
        "validate_bronze",
        "bronze_to_silver",
        "validate_silver",
    }
    assert DAG.get_task("raw_to_bronze").downstream_task_ids == {"validate_bronze"}
    assert DAG.get_task("validate_bronze").downstream_task_ids == {
        "bronze_to_silver",
        "validate_silver",
    }
    assert DAG.get_task("bronze_to_silver").downstream_task_ids == {"validate_silver"}
    assert DAG.get_task("raw_to_bronze").retries == 2
    assert DAG.get_task("raw_to_bronze").retry_delay == timedelta(minutes=5)
    assert DAG.get_task("validate_bronze").retries == 0
    assert DAG.get_task("validate_silver").retries == 0


def test_기사계약_DAG와_출력_파티션을_다투지_않는다():
    """한쪽 원천이 늦어도 다른 쪽 월 적재가 멈추지 않도록 DAG 를 나눴습니다.
    나눈 이상 두 DAG 가 같은 Silver 디렉터리를 동시에 쓰면 안 됩니다."""
    assert DAG.dag_id != driver_master_raw_to_silver_dag.dag_id
    assert DAG.params["silver_dir"] != driver_master_raw_to_silver_dag.params[
        "silver_dir"
    ]


def test_수집task는_제공주소를_보유차량_수집핸들러에_전달한다(monkeypatch):
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
    DAG.get_task("raw_to_bronze").python_callable(
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


def test_정제task는_Bronze경로와_적재위치를_정제핸들러에_전달한다(monkeypatch):
    called = {}
    handlers = []

    def handler(*, event):
        called.update(event)
        return {"row_count": 1, "locations": ["/silver/x.parquet"], "year_month": "2026-08"}

    monkeypatch.setattr(
        task_module,
        "lambda_handler_for",
        lambda name: handlers.append(name) or handler,
    )
    DAG.get_task("bronze_to_silver").python_callable(
        {"locations": ["/bronze/data.parquet"], "year_month": "2026-08"},
        params={"silver_dir": "/silver"},
    )
    assert handlers == ["lease_vehicle_inventory_bronze_to_silver"]
    assert called == {
        "bronze_path": "/bronze/data.parquet",
        "year_month": "2026-08",
        "silver_dir": "/silver",
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

    validated = DAG.get_task("validate_bronze").python_callable(
        {"year_month": "2026-08"},
        params={"base_dir": "/bronze", "api_base_url": "http://source"},
    )

    assert validated == recollected
    assert calls == [{"base_dir": "/bronze", "api_base_url": "http://source"}]


def test_Bronze와_행수가_같고_품질이_맞아야_Silver를_통과시킨다(tmp_path):
    result = _silver_file(tmp_path, _rows())

    task_module.validate_silver_result(result, 1)

    with pytest.raises(ValueError, match="행 수가 Bronze와 다릅니다"):
        task_module.validate_silver_result(result, 2)


def test_적재된_Silver가_재고품질을_깨면_검증에서_잡는다(tmp_path):
    rows = _rows()
    rows.append({**rows[0], "model_year": 2024})
    result = _silver_file(tmp_path, rows)

    with pytest.raises(ValueError, match="vehicle_model_id가 중복됩니다"):
        task_module.validate_silver_result(result, 2)


def test_Silver파일이_없으면_검증에서_실패한다(tmp_path):
    with pytest.raises(ValueError, match="Silver 파일이 없습니다"):
        task_module.validate_silver_result(
            {"locations": [str(tmp_path / "없는파일.parquet")], "row_count": 1}, 1
        )
