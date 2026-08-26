"""기사 차량 월별 스냅샷 Raw→Bronze→Silver DAG 계약.

1. HVFHV와 분리되고 감시 DAG가 호출하는 네 단계 월별 DAG
2. 수집·정제 Lambda 에 파라미터 전달
3. Bronze는 manifest·경로·크기·행 수의 수집 무결성만 확인
4. Silver가 Bronze 보존식·스키마·driver_id 중복 규칙을 확인
5. S3 Silver 경로를 로컬 Path로 접지 않고 검증
6. service_area가 수집·정제 Lambda와 Bronze·Silver 경로에 반영됨
7. S3 Silver 전체 레코드를 공통 GX 검증에 전달
"""

import hashlib
from datetime import date, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from shared.airflow.common import lambda_invoke
from shared.airflow.common.validation import run_quality_gate as real_run_quality_gate
from dags import driver_vehicle_monthly_snapshot_raw_to_silver_dag as dag_module
from schema.bronze import DRIVER_VEHICLE_MONTHLY_SNAPSHOT_SCHEMA as BRONZE_SCHEMA
from schema.silver import CLEAN_DRIVER_VEHICLE_MONTHLY_SNAPSHOT_SCHEMA as SCHEMA
from main.airflow.scripts.driver_vehicle_monthly_snapshot_raw_to_silver import tasks as task_module
from shared.common.bronze_manifest import bronze_manifest_bytes, build_bronze_manifest


DAG = dag_module.driver_vehicle_monthly_snapshot_raw_to_silver_dag
FILE_NAME = "20260821T123456123456Z.parquet"
SOURCE_VERSION = "source_collected_at=20260821T123456123456Z"


@pytest.fixture(autouse=True)
def _stub_quality_gate(monkeypatch):
    monkeypatch.setattr(
        task_module,
        "run_quality_gate",
        lambda directory, validator, **kwargs: validator(),
    )


def _raw_result(source_changed: bool = True) -> dict:
    return {
        "locations": [f"/bronze/driver_vehicle_monthly_snapshot/year_month=2026-08/{FILE_NAME}"],
        "year_month": "2026-08",
        "row_count": 1,
        "source_changed": source_changed,
    }


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
            "extra_comfort_eligible": False,
            "weekly_lease_fee": 350.0,
            "join_date": date(2024, 1, 1),
            "exit_date": None,
            "experience_years": 5,
            "vehicle_since": date(2025, 1, 1),
            "snapshot_created_at": datetime(2026, 8, 1),
        }
    ]


def _write_manifest(path: Path, service_area: str) -> None:
    content = path.read_bytes()
    manifest = build_bronze_manifest(
        {
            "dataset": "driver_vehicle_monthly_snapshot",
            "year_month": "2026-08",
            "collected_at": "2026-08-21T12:34:56.123456Z",
            "content": content,
            "sha256": hashlib.sha256(content).hexdigest(),
            "api_base_url": "http://source",
            "source_etag": '"snapshot-etag"',
            "source_last_modified": "Fri, 21 Aug 2026 12:34:56 GMT",
        },
        service_area=service_area,
        row_count=pq.ParquetFile(path).metadata.num_rows,
    )
    (path.parent / "manifest.json").write_bytes(bronze_manifest_bytes(manifest))


def _silver_file(tmp_path: Path, rows: list[dict]) -> dict:
    path = tmp_path / "year_month=2026-08" / "driver_vehicle_monthly_snapshot.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows, schema=SCHEMA), path)
    return {"locations": [str(path)], "row_count": len(rows), "year_month": "2026-08"}


def _bronze_file(
    tmp_path: Path,
    rows: list[dict],
    *,
    schema: pa.Schema = BRONZE_SCHEMA,
    service_area: str = "NYC",
) -> dict:
    path = (
        tmp_path
        / "driver_vehicle_monthly_snapshot"
        / f"service_area={service_area}"
        / "year_month=2026-08"
        / "collected_at=20260821T123456123456Z"
        / "data.parquet"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), path)
    _write_manifest(path, service_area)
    return {
        "locations": [str(path)],
        "year_month": "2026-08",
        "collected_at": "2026-08-21T12:34:56.123456Z",
        "row_count": len(rows),
        "file_size_bytes": path.stat().st_size,
        "source_changed": True,
    }


def test_DAG는_HVFHV와_분리되어_기사차량스냅샷만_Silver까지_처리한다():
    assert DAG.dag_id == "driver_vehicle_monthly_snapshot_raw_to_silver_pipeline"
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


def test_API_주소는_환경변수_설정값을_사용한다():
    assert DAG.params["api_base_url"] == "http://source-api.test:8091"


def test_수집task는_제공주소를_수집핸들러에_전달한다(monkeypatch):
    called = {}
    handlers = []

    def handler(*, event):
        called.update(event)
        return {"year_month": "2026-08"}

    monkeypatch.setattr(
        lambda_invoke,
        "lambda_handler_for",
        lambda name, **_: handlers.append(name) or handler,
    )
    DAG.get_task("raw_to_bronze").python_callable(
        params={
            "api_base_url": "http://source",
            "base_dir": "/bronze",
            "year": "2026",
            "month": "8",
            "service_area": "TX",
        }
    )
    assert handlers == ["driver_vehicle_monthly_snapshot_raw_to_bronze"]
    assert called == {
        "api_base_url": "http://source",
        "base_dir": "/bronze",
        "year": "2026",
        "month": "8",
        "service_area": "TX",
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
        lambda_invoke,
        "lambda_handler_for",
        lambda name, **_: handlers.append(name) or handler,
    )
    DAG.get_task("bronze_to_silver").python_callable(
        {
            "locations": [f"/bronze/{FILE_NAME}"],
            "year_month": "2026-08",
            "silver_version_path": f"/silver/year_month=2026-08/{SOURCE_VERSION}",
        },
        params={"silver_dir": "/silver", "service_area": "TX"},
    )
    assert handlers == ["driver_vehicle_monthly_snapshot_bronze_to_silver"]
    assert called == {
        "year_month": "2026-08",
        "silver_output_path": f"/silver/year_month=2026-08/{SOURCE_VERSION}",
        "service_area": "TX",
    }


def test_Bronze는_레코드스키마를_판정하지_않고_수집무결성만_확인한다(
    tmp_path, monkeypatch
):
    original = _bronze_file(
        tmp_path,
        [{"source_payload": "원본"}],
        schema=pa.schema([("source_payload", pa.string())]),
    )
    monkeypatch.setattr(task_module, "run_quality_gate", real_run_quality_gate)

    validated = DAG.get_task("validate_bronze").python_callable(
        original,
        params={
            "base_dir": str(tmp_path),
            "silver_dir": str(tmp_path / "silver"),
            "service_area": "NYC",
        },
        run_id="manual__collection-only",
    )

    assert {key: validated[key] for key in original} == original
    assert validated["silver_version_path"].endswith(SOURCE_VERSION)
    assert (Path(original["locations"][0]).parent / "_SUCCESS").is_file()


def test_동일한_Bronze도_감시DAG가_호출하면_Silver처리한다(tmp_path, monkeypatch):
    validated = []
    monkeypatch.setattr(
        task_module,
        "validate_monthly_parquet_bronze",
        lambda result, **kwargs: validated.append(result)
        or (Path("same.parquet"), "2026-08"),
    )

    result = _raw_result(source_changed=False)
    validated_result = DAG.get_task("validate_bronze").python_callable(
        result,
        params={"base_dir": "/bronze", "silver_dir": str(tmp_path)},
    )

    assert validated_result["silver_version_path"].endswith(SOURCE_VERSION)
    assert validated == [result]


def test_Bronze검증과_Silver경로는_service_area_파라미터를_따른다(tmp_path):
    bronze = (
        tmp_path
        / "bronze"
        / "driver_vehicle_monthly_snapshot"
        / "service_area=TX"
        / "year_month=2026-08"
        / "collected_at=20260821T123456123456Z"
        / "data.parquet"
    )
    bronze.parent.mkdir(parents=True)
    pq.write_table(pa.Table.from_pylist(_rows(), schema=SCHEMA), bronze)
    _write_manifest(bronze, "TX")

    validated = DAG.get_task("validate_bronze").python_callable(
        {
            "locations": [str(bronze)],
            "year_month": "2026-08",
            "collected_at": "2026-08-21T12:34:56.123456Z",
            "row_count": 1,
            "file_size_bytes": bronze.stat().st_size,
            "source_changed": True,
        },
        params={
            "base_dir": str(tmp_path / "bronze"),
            "silver_dir": str(tmp_path / "silver"),
            "service_area": "TX",
        },
    )

    assert "service_area=TX/year_month=2026-08" in validated["silver_version_path"]


def test_Bronze와_행수가_같고_규칙이_맞아야_Silver를_통과시킨다(tmp_path):
    result = _silver_file(tmp_path, _rows())

    task_module.validate_silver_result(result, 1)

    with pytest.raises(ValueError, match="행 수가 Bronze와 다릅니다"):
        task_module.validate_silver_result(result, 2)


def test_Silver검증후_최종버전에_SUCCESS를_공개한다(tmp_path, monkeypatch):
    final = tmp_path / "year_month=2026-08" / SOURCE_VERSION
    part = final / "data.parquet"
    part.parent.mkdir(parents=True)
    pq.write_table(pa.Table.from_pylist(_rows(), schema=SCHEMA), part)
    raw_result = _bronze_file(tmp_path / "raw", _rows())
    raw_result["silver_version_path"] = str(final)
    monkeypatch.setattr(task_module, "run_quality_gate", real_run_quality_gate)

    DAG.get_task("validate_silver").python_callable(
        {"locations": [str(part)], "row_count": 1, "year_month": "2026-08"},
        raw_result,
    )

    assert (final / "_SUCCESS").is_file()
    assert not (final / "_QUARANTINED.json").exists()


def test_Bronze_타입이_달라도_수집무결성이_맞으면_원본을_공개한다(
    tmp_path, monkeypatch
):
    schema = pa.schema(
        [
            pa.field(field.name, pa.int64() if field.name == "driver_id" else field.type)
            for field in BRONZE_SCHEMA
        ]
    )
    rows = [{**_rows()[0], "driver_id": 1}]
    result = _bronze_file(tmp_path, rows, schema=schema)
    bronze = Path(result["locations"][0])
    monkeypatch.setattr(task_module, "run_quality_gate", real_run_quality_gate)

    DAG.get_task("validate_bronze").python_callable(
        result,
        params={"base_dir": str(tmp_path), "silver_dir": str(tmp_path / "silver")},
        run_id="manual__type-mismatch",
    )

    assert bronze.is_file()
    assert (bronze.parent / "_SUCCESS").is_file()
    assert not (bronze.parent / "_QUARANTINED.json").exists()


def test_Bronze_snapshot시각_ns는_us계약과_호환한다(tmp_path):
    snapshot_index = BRONZE_SCHEMA.get_field_index("snapshot_created_at")
    snapshot_field = BRONZE_SCHEMA.field(snapshot_index)
    schema = BRONZE_SCHEMA.set(
        snapshot_index,
        snapshot_field.with_type(pa.timestamp("ns")),
    )
    bronze = (
        tmp_path
        / "driver_vehicle_monthly_snapshot"
        / "service_area=NYC"
        / "year_month=2026-08"
        / "collected_at=20260821T123456123456Z"
        / "data.parquet"
    )
    bronze.parent.mkdir(parents=True)
    pq.write_table(pa.Table.from_pylist(_rows(), schema=schema), bronze)
    _write_manifest(bronze, "NYC")

    validated = DAG.get_task("validate_bronze").python_callable(
        {
            "locations": [str(bronze)],
            "year_month": "2026-08",
            "collected_at": "2026-08-21T12:34:56.123456Z",
            "row_count": 1,
            "file_size_bytes": bronze.stat().st_size,
        },
        params={"base_dir": str(tmp_path), "silver_dir": str(tmp_path / "silver")},
    )

    assert validated["silver_version_path"].endswith(SOURCE_VERSION)


def test_S3_Bronze는_레코드_GX를_호출하지_않는다(monkeypatch):
    location = task_module.S3Location(
        "de-theone",
        "bronze/driver_vehicle_monthly_snapshot/service_area=NYC/"
        "year_month=2026-08/collected_at=20260821T123456123456Z/data.parquet",
    )
    calls = []
    monkeypatch.setattr(
        task_module,
        "validate_monthly_parquet_bronze",
        lambda *args, **kwargs: (location, "2026-08"),
    )
    monkeypatch.setattr(
        task_module,
        "run_table_gx_validation",
        lambda *args, **kwargs: calls.append(kwargs),
    )

    task_module._validate_bronze(
        {"result": _raw_result()}, {"silver_dir": "/silver"}, {}
    )

    assert calls == []


def test_Silver_보존식은_INT96_Bronze_타임스탬프를_호환한다(monkeypatch):
    location = task_module.S3Location(
        "de-theone",
        "bronze/driver_vehicle_monthly_snapshot/service_area=TX/"
        "year_month=2026-01/collected_at=20260825T090906998387Z/data.parquet",
    )
    ns_schema = pa.schema(
        [
            field.with_type(pa.timestamp("ns"))
            if field.name == "snapshot_created_at"
            else field
            for field in BRONZE_SCHEMA
        ]
    )
    ns_table = pa.Table.from_pylist(_rows(), schema=ns_schema)
    assert ns_table.schema.field("snapshot_created_at").type == pa.timestamp("ns")

    monkeypatch.setattr(task_module, "read_parquet", lambda path: ns_table)

    expected_rows = task_module._expected_silver_rows(
        {"locations": [str(location)], "row_count": 1}
    )

    assert expected_rows == 1


def test_Silver_검증이_실패하면_산출물을_보존하고_격리한다(tmp_path, monkeypatch):
    final = tmp_path / "year_month=2026-08" / SOURCE_VERSION
    part = final / "data.parquet"
    part.parent.mkdir(parents=True)
    pq.write_table(pa.Table.from_pylist(_rows(), schema=SCHEMA), part)
    bronze_rows = [
        {**_rows()[0], "driver_id": f"driver-{index}", "taxi_id": f"taxi-{index}"}
        for index in range(2)
    ]
    raw_result = _bronze_file(tmp_path / "raw", bronze_rows)
    raw_result["silver_version_path"] = str(final)
    monkeypatch.setattr(task_module, "run_quality_gate", real_run_quality_gate)

    with pytest.raises(ValueError, match="행 수가 Bronze와 다릅니다"):
        DAG.get_task("validate_silver").python_callable(
            {"locations": [str(part)], "row_count": 2, "year_month": "2026-08"},
            raw_result,
            run_id="manual__silver-invalid",
        )

    assert part.is_file()
    assert (final / "_QUARANTINED.json").is_file()
    assert not (final / "_SUCCESS").exists()


def test_S3_Silver_경로를_로컬_Path로_변환하지_않는다(monkeypatch):
    seen = []
    gx = []
    table = pa.Table.from_pylist(_rows(), schema=SCHEMA)
    monkeypatch.setattr(
        task_module,
        "read_parquet",
        lambda path: seen.append(path) or table,
    )
    monkeypatch.setattr(
        task_module,
        "run_table_gx_validation",
        lambda table, *args, **kwargs: gx.append((table, kwargs)),
    )

    task_module.validate_silver_result(
        {
            "locations": [
                "s3://de-theone/silver/driver_vehicle_monthly_snapshot/"
                "year_month=2026-08/data.parquet"
            ],
            "row_count": 1,
        },
        1,
    )

    assert isinstance(seen[0], task_module.S3Location)
    assert gx[0][0].num_rows == 1
    assert gx[0][1]["required_warning_ratio"] is None
    assert gx[0][1]["required_error_ratio"] == 0


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


def test_퇴사자로_설명되지_않는_유실은_막는다(tmp_path, monkeypatch):
    """예전에는 핸들러가 보고한 행 수를 그 자신의 기대값으로 넘겨 항진명제였다.

        validate_silver_result(silver_result, silver_result["row_count"], ...)

    그래서 변환과 적재가 같이 틀리면 통과했다. Bronze 에서 되짚은 기대값과 비교한다.
    """
    final = tmp_path / "year_month=2026-08" / SOURCE_VERSION
    part = final / "data.parquet"
    part.parent.mkdir(parents=True)
    pq.write_table(pa.Table.from_pylist(_rows(), schema=SCHEMA), part)  # 1행
    bronze_rows = [
        {**_rows()[0], "driver_id": f"driver-{index}", "taxi_id": f"taxi-{index}"}
        for index in range(10)
    ]
    raw_result = _bronze_file(tmp_path / "raw", bronze_rows)
    raw_result["silver_version_path"] = str(final)
    monkeypatch.setattr(task_module, "run_quality_gate", real_run_quality_gate)

    with pytest.raises(ValueError, match="행 수가 Bronze와 다릅니다"):
        DAG.get_task("validate_silver").python_callable(
            {"locations": [str(part)], "row_count": 1, "year_month": "2026-08"},
            raw_result,
            run_id="manual__recon-gap",
        )


def test_퇴사자로_설명되면_줄어들어도_통과한다(tmp_path, monkeypatch):
    """Silver 가 퇴사 기사를 빼므로 줄어드는 건 정상이다 — 설명되는지만 본다."""
    final = tmp_path / "year_month=2026-08" / SOURCE_VERSION
    part = final / "data.parquet"
    part.parent.mkdir(parents=True)
    pq.write_table(pa.Table.from_pylist(_rows(), schema=SCHEMA), part)  # 1행
    bronze_rows = [
        {
            **_rows()[0],
            "driver_id": f"driver-{index}",
            "taxi_id": f"taxi-{index}",
            "exit_date": None if index == 0 else date(2026, 7, 31),
        }
        for index in range(4)
    ]
    raw_result = _bronze_file(tmp_path / "raw", bronze_rows)
    raw_result["silver_version_path"] = str(final)
    monkeypatch.setattr(task_module, "run_quality_gate", real_run_quality_gate)

    DAG.get_task("validate_silver").python_callable(
        {"locations": [str(part)], "row_count": 1, "year_month": "2026-08"},
        raw_result,
    )

    assert (final / "_SUCCESS").is_file()


def test_Silver_보존식은_Bronze경로가_없으면_막는다(tmp_path, monkeypatch):
    final = tmp_path / "year_month=2026-08" / SOURCE_VERSION
    part = final / "data.parquet"
    part.parent.mkdir(parents=True)
    pq.write_table(pa.Table.from_pylist(_rows(), schema=SCHEMA), part)
    monkeypatch.setattr(task_module, "run_quality_gate", real_run_quality_gate)

    with pytest.raises(ValueError, match="locations는 비어 있지 않은"):
        DAG.get_task("validate_silver").python_callable(
            {"locations": [str(part)], "row_count": 1, "year_month": "2026-08"},
            {"row_count": 1, "silver_version_path": str(final)},
            run_id="manual__no-recon",
        )


def test_Silver_검증이_Bronze_퇴사자를_세어_보존식을_만든다(tmp_path):
    """`exit_date` 가 있는 행이 퇴사자다. NULL 이 재직 중."""
    rows = [
        {**_rows()[0], "driver_id": "d1", "exit_date": None},
        {**_rows()[0], "driver_id": "d2", "exit_date": None},
        {**_rows()[0], "driver_id": "d3", "exit_date": date(2026, 7, 31)},
    ]
    table = pa.Table.from_pylist(rows, schema=BRONZE_SCHEMA)

    counts = task_module._bronze_recon_counts(table)

    assert counts == {"bronze_row_count": 3, "exited_driver_rows": 1}
