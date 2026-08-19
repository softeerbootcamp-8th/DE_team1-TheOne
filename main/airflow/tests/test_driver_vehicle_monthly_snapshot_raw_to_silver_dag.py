"""기사 차량 월별 스냅샷 Raw→Bronze→Silver DAG 계약.

1. HVFHV와 분리된 네 단계 월별 DAG
2. 수집·정제 Lambda 에 파라미터 전달
3. 필수 컬럼 누락 시 원천부터 한 번 재수집
4. Bronze 행 수·스키마·driver_id 중복 규칙으로 Silver 확인
"""

from datetime import datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from dags import driver_vehicle_monthly_snapshot_raw_to_silver_dag as dag_module
from schema.silver import CLEAN_DRIVER_VEHICLE_MONTHLY_SNAPSHOT_SCHEMA as SCHEMA
from main.airflow.scripts.driver_vehicle_monthly_snapshot_raw_to_silver import tasks as task_module


DAG = dag_module.driver_vehicle_monthly_snapshot_raw_to_silver_dag


def _rows():
    return [
        {
            "snapshot_month": "2026-08",
            "driver_id": "driver-1",
            "taxi_id": "taxi-1",
            "vehicle_model_id": "model-1",
            "manufacturer": "KIA",
            "model_name": "SPORTAGE",
            "fuel_type": "GAS",
            "comfort_eligible": True,
            "weekly_lease_fee": 350.0,
            "snapshot_created_at": datetime(2026, 8, 1),
        }
    ]


def _silver_file(tmp_path: Path, rows: list[dict]) -> dict:
    path = tmp_path / "year_month=2026-08" / "driver_vehicle_monthly_snapshot.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows, schema=SCHEMA), path)
    return {"locations": [str(path)], "row_count": len(rows), "year_month": "2026-08"}


def test_DAG는_HVFHV와_분리되어_기사차량스냅샷만_Silver까지_처리한다():
    assert DAG.dag_id == "driver_vehicle_monthly_snapshot_raw_to_silver_pipeline"
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


def test_수집task는_제공주소를_수집핸들러에_전달한다(monkeypatch):
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
    assert handlers == ["driver_vehicle_monthly_snapshot_raw_to_bronze"]
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
        return {
            "row_count": 1,
            "locations": ["/silver/x.parquet"],
            "year_month": "2026-08",
        }

    monkeypatch.setattr(
        task_module,
        "lambda_handler_for",
        lambda name: handlers.append(name) or handler,
    )
    DAG.get_task("bronze_to_silver").python_callable(
        {"locations": ["/bronze/data.parquet"], "year_month": "2026-08"},
        params={"silver_dir": "/silver"},
    )
    assert handlers == ["driver_vehicle_monthly_snapshot_bronze_to_silver"]
    assert called == {
        "bronze_path": "/bronze/data.parquet",
        "year_month": "2026-08",
        "silver_dir": "/silver",
    }


def test_필수컬럼이_누락되면_원천부터_다시_수집한다(monkeypatch):
    results = iter(
        [
            (Path("broken.parquet"), ["driver_id"]),
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


def test_Bronze와_행수가_같고_규칙이_맞아야_Silver를_통과시킨다(tmp_path):
    result = _silver_file(tmp_path, _rows())

    task_module.validate_silver_result(result, 1)

    with pytest.raises(ValueError, match="행 수가 Bronze와 다릅니다"):
        task_module.validate_silver_result(result, 2)


def test_적재된_Silver가_driver_id중복을_깨면_검증에서_잡는다(tmp_path):
    rows = _rows()
    rows.append({**rows[0], "taxi_id": "taxi-2"})
    result = _silver_file(tmp_path, rows)

    with pytest.raises(ValueError, match="driver_id가 중복됩니다"):
        task_module.validate_silver_result(result, 2)


def test_Silver파일이_없으면_검증에서_실패한다(tmp_path):
    with pytest.raises(ValueError, match="Silver 파일이 없습니다"):
        task_module.validate_silver_result(
            {"locations": [str(tmp_path / "없는파일.parquet")], "row_count": 1}, 1
        )
