"""보유 차량 Raw→Bronze→Silver DAG 계약.

1. 기사 계약 DAG와 분리되고 감시 DAG가 호출하는 네 단계 월별 DAG
2. 수집·정제 Lambda 를 AWS 동기 호출하고 payload 전달
3. 필수 컬럼 누락 시 Airflow 프로세스에서 재수집하지 않고 실패
4. Bronze 행 수·스키마·재고 품질로 Silver 확인
5. S3 Silver 경로를 로컬 Path로 접지 않고 검증
6. service_area를 Bronze 수집·정제와 Silver 경로에 전달
"""

from datetime import timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from dags import lease_vehicle_inventory_raw_to_silver_dag as dag_module
from dags.driver_vehicle_monthly_snapshot_raw_to_silver_dag import driver_vehicle_monthly_snapshot_raw_to_silver_dag
from schema.silver import CLEAN_LEASE_VEHICLE_INVENTORY_SCHEMA as SCHEMA
from main.airflow.scripts.lease_vehicle_inventory_raw_to_silver import (
    tasks as task_module,
)


DAG = dag_module.lease_vehicle_inventory_raw_to_silver_dag
FILE_NAME = "20260821T123456123456Z.parquet"
SOURCE_VERSION = "source_collected_at=20260821T123456123456Z"


@pytest.fixture(autouse=True)
def _stub_success_publish(monkeypatch):
    monkeypatch.setattr(task_module, "publish_success_marker", lambda directory: None)


def _raw_result(source_changed: bool = True) -> dict:
    return {
        "locations": [f"/bronze/lease_vehicle_inventory/year_month=2026-08/{FILE_NAME}"],
        "year_month": "2026-08",
        "row_count": 1,
        "source_changed": source_changed,
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
    assert DAG.schedule is None
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


def test_기본_API_주소는_내부_제공서버를_사용한다():
    assert DAG.params["api_base_url"] == "http://10.0.10.81:8091"


def test_보유차량_DAG는_NYC를_기본_서비스지역으로_사용한다():
    assert DAG.params["service_area"] == "NYC"


def test_기사계약_DAG와_출력_파티션을_다투지_않는다():
    """한쪽 원천이 늦어도 다른 쪽 월 적재가 멈추지 않도록 DAG 를 나눴습니다.
    나눈 이상 두 DAG 가 같은 Silver 디렉터리를 동시에 쓰면 안 됩니다."""
    assert DAG.dag_id != driver_vehicle_monthly_snapshot_raw_to_silver_dag.dag_id
    assert DAG.params["silver_dir"] != driver_vehicle_monthly_snapshot_raw_to_silver_dag.params[
        "silver_dir"
    ]


def test_수집과_정제task는_AWS_Lambda_payload를_사용한다():
    raw = DAG.get_task("raw_to_bronze")
    silver = DAG.get_task("bronze_to_silver")

    assert raw.function_name == "lease_vehicle_inventory_raw_to_bronze"
    assert silver.function_name == "lease_vehicle_inventory_bronze_to_silver"
    assert "params.api_base_url" in raw.payload
    assert "silver_version_path" in silver.payload


def test_동일한_Bronze도_감시DAG가_호출하면_Silver처리한다(tmp_path, monkeypatch):
    validated = []
    monkeypatch.setattr(
        task_module,
        "_validate_bronze_result",
        lambda result, base_dir, service_area: validated.append(result)
        or (Path("same.parquet"), []),
    )

    result = _raw_result(source_changed=False)
    validated_result = DAG.get_task("validate_bronze").python_callable(
        result,
        params={
            "base_dir": "/bronze",
            "silver_dir": str(tmp_path),
            "service_area": "NYC",
        },
    )

    assert validated_result["silver_version_path"].endswith(SOURCE_VERSION)
    assert validated == [result]


def test_TX_Bronze검증은_지역별_Silver경로를_만든다(tmp_path, monkeypatch):
    seen = []
    monkeypatch.setattr(
        task_module,
        "_validate_bronze_result",
        lambda result, base_dir, service_area: seen.append(service_area)
        or (Path("same.parquet"), []),
    )

    result = DAG.get_task("validate_bronze").python_callable(
        _raw_result(),
        params={
            "base_dir": "/bronze",
            "silver_dir": str(tmp_path / "silver"),
            "service_area": "TX",
        },
    )

    expected_root = tmp_path / "silver" / "service_area=TX" / "year_month=2026-08"
    assert Path(result["silver_version_path"]).parent == expected_root
    assert seen == ["TX"]


def test_Bronze와_행수가_같고_품질이_맞아야_Silver를_통과시킨다(tmp_path):
    result = _silver_file(tmp_path, _rows())

    task_module.validate_silver_result(result, 1)

    with pytest.raises(ValueError, match="행 수가 Bronze와 다릅니다"):
        task_module.validate_silver_result(result, 2)


def test_Silver검증후_최종버전에_SUCCESS를_공개한다(tmp_path, monkeypatch):
    final = tmp_path / "year_month=2026-08" / SOURCE_VERSION
    part = final / "data.parquet"
    part.parent.mkdir(parents=True)
    pq.write_table(pa.Table.from_pylist(_rows(), schema=SCHEMA), part)
    published = []
    monkeypatch.setattr(
        task_module,
        "publish_success_marker",
        lambda directory: published.append(directory),
    )

    DAG.get_task("validate_silver").python_callable(
        {"locations": [str(part)], "row_count": 1, "year_month": "2026-08"},
        {
            "row_count": 1,
            "silver_version_path": str(final),
        },
    )

    assert published == [final]


def test_S3_Silver_경로를_로컬_Path로_변환하지_않는다(monkeypatch):
    seen = []
    table = pa.Table.from_pylist(_rows(), schema=SCHEMA)
    monkeypatch.setattr(
        task_module,
        "read_parquet",
        lambda path: seen.append(path) or table,
    )

    task_module.validate_silver_result(
        {
            "locations": [
                "s3://de-theone/silver/lease_vehicle_inventory/"
                "year_month=2026-08/data.parquet"
            ],
            "row_count": 1,
        },
        1,
    )

    assert isinstance(seen[0], task_module.S3Location)


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
