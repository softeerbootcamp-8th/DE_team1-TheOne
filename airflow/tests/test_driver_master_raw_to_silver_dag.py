"""기사 데이터 Raw→Bronze→Silver DAG 계약.

1. HVFHV와 분리된 네 단계 월별 DAG
2. 기사 데이터 수집 Lambda에 제공 주소 파라미터 전달
3. 리스 키·기간·재실행 Silver 검증
"""

from datetime import date, timedelta

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from dags import driver_master_raw_to_silver_dag as dag_module
from schema.silver.driver_vehicle_leases import SCHEMA
from scripts.driver_master_raw_to_silver import tasks as task_module


DAG = dag_module.driver_master_raw_to_silver_dag


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


def test_DAG는_HVFHV와_분리되어_기사데이터만_Silver까지_처리한다():
    assert DAG.dag_id == "driver_master_raw_to_silver_pipeline"
    assert DAG.schedule == "0 0 10 * *"
    assert set(DAG.task_ids) == {
        "raw_to_bronze",
        "validate_bronze",
        "bronze_to_silver",
        "validate_silver",
    }
    assert DAG.get_task("raw_to_bronze").downstream_task_ids == {
        "validate_bronze",
        "validate_silver",
    }
    assert DAG.get_task("validate_bronze").downstream_task_ids == {
        "bronze_to_silver"
    }
    assert DAG.get_task("bronze_to_silver").downstream_task_ids == {
        "validate_silver"
    }
    assert DAG.get_task("raw_to_bronze").retries == 2
    assert DAG.get_task("raw_to_bronze").retry_delay == timedelta(minutes=5)
    assert DAG.get_task("validate_bronze").retries == 0
    assert DAG.get_task("validate_silver").retries == 0


def test_기사데이터수집task는_제공주소를_기존핸들러에_전달한다(monkeypatch):
    called = {}

    def handler(*, event):
        called.update(event)
        return {"year_month": "2026-08"}

    monkeypatch.setattr(task_module, "lambda_handler_for", lambda name: handler)
    DAG.get_task("raw_to_bronze").python_callable(
        params={
            "api_base_url": "http://source",
            "base_dir": "/bronze",
            "year": "2026",
            "month": "8",
        }
    )
    assert called == {
        "api_base_url": "http://source",
        "base_dir": "/bronze",
        "year": "2026",
        "month": "8",
    }


def test_기사데이터를_정제해_같은월Silver로_멱등적재한다(tmp_path):
    rows = _rows()
    rows[0]["make_key"] = " kia "
    rows[0]["model_key"] = " sportage "
    bronze = tmp_path / "bronze.parquet"
    pq.write_table(pa.Table.from_pylist(rows), bronze)

    first = task_module.clean_bronze_to_silver(
        bronze, tmp_path / "silver", "2026-08"
    )
    second = task_module.clean_bronze_to_silver(
        bronze, tmp_path / "silver", "2026-08"
    )

    assert first == second
    path = tmp_path / "silver" / "year_month=2026-08" / "driver_vehicle_leases.parquet"
    assert pq.read_schema(path) == SCHEMA
    assert pq.ParquetFile(path).metadata.num_rows == 1
    written = pq.ParquetFile(path).read().to_pylist()[0]
    assert (written["make_key"], written["model_key"]) == ("KIA", "SPORTAGE")
    assert len(list(path.parent.glob("*.parquet"))) == 1


@pytest.mark.parametrize("broken", ["duplicate", "taxi_overlap", "driver_overlap"])
def test_중복키나_리스기간중첩은_Silver적재전에_실패한다(tmp_path, broken):
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
    bronze = tmp_path / "bronze.parquet"
    pq.write_table(pa.Table.from_pylist(rows), bronze)

    with pytest.raises(ValueError, match="중복|기간이 겹칩니다"):
        task_module.clean_bronze_to_silver(
            bronze, tmp_path / "silver", "2026-08"
        )

    assert not list((tmp_path / "silver").rglob("*.parquet"))


def test_Silver교체중_실패해도_기존월파일을_보존한다(tmp_path, monkeypatch):
    target = task_module.write_silver(
        pa.Table.from_pylist(_rows(), schema=SCHEMA),
        tmp_path / "silver",
        "2026-08",
    )
    before = target.read_bytes()

    def fail_replace(source, destination):
        raise OSError("교체 실패")

    monkeypatch.setattr(type(target), "replace", fail_replace)
    with pytest.raises(OSError, match="교체 실패"):
        task_module.write_silver(
            pa.Table.from_pylist(_rows(), schema=SCHEMA),
            tmp_path / "silver",
            "2026-08",
        )

    assert target.read_bytes() == before
    assert not list(target.parent.glob("*.tmp"))
