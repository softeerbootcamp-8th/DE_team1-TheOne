"""월별 택시 운행 DAG의 Bronze/Silver 경계와 공개 규칙을 검증합니다.

Bronze 는 manifest·체크섬·경로·크기·footer 행 수만 검증합니다.
레코드 품질은 Spark GX가 맡고 Airflow Silver는 파일 스키마와 reconciliation을
직접 확인합니다.
검증 태스크의 값어치는 "통과한다"가 아니라 "불량을 통과시키지 않는다"입니다.

실제 Parquet 을 tmp_path 에 쓰며 네트워크와 Spark 는 사용하지 않습니다.
Silver timestamp는 unit 차이는 허용하되 timezone identity는 유지합니다.
S3 Bronze 위치는 로컬 Path로 변환하지 않고 객체 바이트로 검증합니다.
S3 Silver 버전은 Bronze와 같은 버킷의 monthly_taxi_trip prefix로 계산합니다.
Silver는 검증 성공 시 `_SUCCESS`, 실패 시 `_QUARANTINED.json`으로 전환됩니다.
격리 버전은 데이터 파일을 보존하되 후속 reader가 읽지 않습니다(#945).
Spark GX 요약과 Data Docs 경로는 별도 Airflow task가 성공·실패 모두 기록합니다.
"""

import hashlib
import io
import json
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from dags import monthly_taxi_trip_raw_to_silver_dag as dag_module
from main.airflow.scripts.monthly_taxi_trip_raw_to_silver import tasks as task_module
from shared.airflow.common.slack_quality_warning import build_quality_warning
from shared.common.bronze_manifest import bronze_manifest_bytes, build_bronze_manifest
from schema.bronze import MONTHLY_TAXI_TRIP_SCHEMA as BRONZE_SCHEMA

DAG = dag_module.monthly_taxi_trip_dag
COLLECTED_AT = datetime(2026, 8, 11, 8, 53, 54, tzinfo=timezone.utc)
YEAR_MONTH = "2026-07"
SILVER_SCHEMA = task_module.SILVER_SCHEMA
SILVER_COLUMNS = list(SILVER_SCHEMA.names)
validate_bronze = DAG.get_task("validate_bronze").python_callable
validate_silver = DAG.get_task("validate_silver").python_callable


def bronze_rows(count: int = 3, schema=None) -> list[dict]:
    schema = BRONZE_SCHEMA if schema is None else schema
    row = {
        field.name: COLLECTED_AT if pa.types.is_timestamp(field.type)
        else 1 if pa.types.is_integer(field.type)
        else 1.0 if pa.types.is_floating(field.type)
        else "x"
        for field in schema
    }
    row.update(
        {
            "hvfhs_license_num": "HV0003",
            "estimated_service_tier": "Standard",
            "dropoff_datetime": datetime(2026, 8, 11, 9, 3, 54, tzinfo=timezone.utc),
        }
    )
    return [row.copy() for _ in range(count)]


def write_bronze(
    base_dir,
    year_month: str = YEAR_MONTH,
    rows: int = 3,
    schema=None,
    records: list[dict] | None = None,
    service_area: str = "NYC",
) -> str:
    schema = BRONZE_SCHEMA if schema is None else schema
    records = bronze_rows(rows, schema) if records is None else records
    dataset_root = Path(base_dir) / "monthly_taxi_trip"
    dataset_root /= f"service_area={service_area}"
    path = (
        dataset_root
        / f"year_month={year_month}"
        / "collected_at=20260811T085354000000Z"
        / "data.parquet"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(records, schema=schema), path)
    _write_manifest(path, year_month, service_area, len(records))
    return str(path)


def _write_manifest(
    path: Path, year_month: str, service_area: str, row_count: int
) -> None:
    content = path.read_bytes()
    manifest = build_bronze_manifest(
        {
            "dataset": "monthly_taxi_trip",
            "year_month": year_month,
            "collected_at": "2026-08-11T08:53:54.000000Z",
            "content": content,
            "sha256": hashlib.sha256(content).hexdigest(),
            "api_base_url": "http://source.example",
            "source_etag": '"source-etag"',
            "source_last_modified": "Tue, 11 Aug 2026 08:53:54 GMT",
        },
        service_area=service_area,
        row_count=row_count,
    )
    (path.parent / "manifest.json").write_bytes(bronze_manifest_bytes(manifest))


def write_directory_bronze(
    base_dir, year_month: str = YEAR_MONTH, rows: int = 3,
    service_area: str = "NYC",
) -> str:
    path = (
        Path(base_dir)
        / "monthly_taxi_trip"
        / f"service_area={service_area}"
        / f"year_month={year_month}"
        / "collected_at=20260811T085354000000Z"
        / "data.parquet"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.Table.from_pylist(bronze_rows(rows, BRONZE_SCHEMA), schema=BRONZE_SCHEMA),
        path,
    )
    _write_manifest(path, year_month, service_area, rows)
    return str(path)


def result_for(
    path: str, year_month: str = YEAR_MONTH, service_area: str = "NYC"
) -> dict:
    parquet = pq.ParquetFile(path)
    version = (
        Path(task_module.DEFAULT_SILVER_DIR)
        / f"service_area={service_area}"
        / f"year_month={year_month}"
        / "source_collected_at=20260811T085354000000Z"
    )
    return {
        "row_count": parquet.metadata.num_rows,
        "locations": [path],
        "year_month": year_month,
        "collected_at": "2026-08-11T08:53:54.000000Z",
        "file_size_bytes": Path(path).stat().st_size,
        "source_changed": True,
        "silver_version_path": str(version),
    }


def bronze_params(base_dir, service_area: str = "NYC") -> dict:
    return {"base_dir": str(base_dir), "service_area": service_area}


def test_품질경고메시지는_판정근거와_처리결과를_표시한다():
    text = build_quality_warning(
        dataset="monthly_taxi_trip", year_month="2026-08",
        invalid_rows=23, row_count=1000, invalid_ratio=0.023,
        extra_columns=["airport_fee", "congestion_fee"],
    )

    for expected in ("23 / 1,000", "2.3%", "airport_fee, congestion_fee", "Silver 진행"):
        assert expected in text


def test_Spark_GX결과와_Data_Docs경로를_Airflow로그에_남긴다(
    monkeypatch, caplog
):
    import logging

    version = (
        "s3://de-theone/silver/monthly_taxi_trip/service_area=NYC/"
        "year_month=2026-08/source_collected_at=20260811T085354000000Z"
    )
    docs = (
        "s3://de-theone/logs/gx-data-docs/silver/monthly_taxi_trip/"
        "service_area=NYC/year_month=2026-08/"
        "source_collected_at=20260811T085354000000Z"
    )
    summary = {
        "success": True,
        "total": 100,
        "valid": 98,
        "invalid": 2,
        "extra_columns": ["airport_fee"],
        "data_docs_path": docs,
    }

    def get_object(bucket, key):
        assert bucket == "de-theone"
        if key.endswith("_GX_VALIDATION.json"):
            return json.dumps(summary).encode()
        if key.endswith("index.html"):
            return b"<html>GX Data Docs</html>"
        raise AssertionError(key)

    monkeypatch.setattr(task_module, "get_object_bytes", get_object)
    caplog.set_level(logging.INFO, logger=task_module.logger.name)

    actual = task_module._report_gx_validation({"silver_version_path": version})

    assert actual == summary
    assert "success=True total=100 valid=98 invalid=2" in caplog.text
    assert "extra_columns=['airport_fee']" in caplog.text
    assert f"data_docs={docs}" in caplog.text


def test_Data_Docs_index가_없으면_Airflow보고task가_실패한다(monkeypatch):
    version = (
        "s3://de-theone/silver/monthly_taxi_trip/service_area=NYC/"
        "year_month=2026-08/source_collected_at=20260811T085354000000Z"
    )
    docs = version.replace(
        "s3://de-theone/silver/", "s3://de-theone/logs/gx-data-docs/silver/"
    )
    summary = {
        "success": True,
        "total": 1,
        "valid": 1,
        "invalid": 0,
        "extra_columns": [],
        "data_docs_path": docs,
    }

    def get_object(bucket, key):
        if key.endswith("_GX_VALIDATION.json"):
            return json.dumps(summary).encode()
        raise FileNotFoundError(key)

    monkeypatch.setattr(task_module, "get_object_bytes", get_object)

    with pytest.raises(ValueError, match="GX Data Docs index가 없습니다"):
        task_module._report_gx_validation({"silver_version_path": version})


def test_로컬_GX요약은_S3_Data_Docs없이_Airflow로그에_남긴다(
    tmp_path, caplog
):
    import logging

    version = tmp_path / "year_month=2026-08/source_collected_at=x"
    version.mkdir(parents=True)
    (version / "_GX_VALIDATION.json").write_text(
        json.dumps(
            {
                "success": True,
                "total": 1,
                "valid": 1,
                "invalid": 0,
                "extra_columns": [],
                "data_docs_path": None,
            }
        )
    )
    caplog.set_level(logging.INFO, logger=task_module.logger.name)

    task_module._report_gx_validation({"silver_version_path": str(version)})

    assert "data_docs=disabled(local)" in caplog.text


def test_Spark_GX실패결과도_Airflow보고task가_error로그로_남긴다(
    tmp_path, caplog
):
    import logging

    version = tmp_path / "year_month=2026-08/source_collected_at=x"
    version.mkdir(parents=True)
    (version / "_GX_VALIDATION.json").write_text(
        json.dumps(
            {
                "success": False,
                "total": 20,
                "valid": 19,
                "invalid": 1,
                "extra_columns": ["airport_fee"],
                "data_docs_path": None,
            }
        )
    )
    caplog.set_level(logging.ERROR, logger=task_module.logger.name)

    summary = task_module._report_gx_validation(
        {"silver_version_path": str(version)}
    )

    assert summary["success"] is False
    assert "success=False total=20 valid=19 invalid=1" in caplog.text


def test_Validation_Task에_Slack_실패_콜백이_연결된다():
    for task_id in ("validate_bronze", "validate_silver"):
        validation_task = DAG.get_task(task_id)
        assert task_module.slack_failure_callback in validation_task.on_failure_callback


def test_정상_적재는_통과한다(tmp_path):
    path = write_bronze(tmp_path)
    validate_bronze(result_for(path), params=bronze_params(tmp_path))


def test_Bronze_manifest가_없으면_공개하지않는다(tmp_path):
    path = Path(write_bronze(tmp_path))
    (path.parent / "manifest.json").unlink()

    with pytest.raises(ValueError, match="manifest가 없습니다"):
        validate_bronze(result_for(str(path)), params=bronze_params(tmp_path))

    assert (path.parent / "_QUARANTINED.json").is_file()
    assert not (path.parent / "_SUCCESS").exists()


def test_Bronze_manifest_SHA256이_원본과_다르면_공개하지않는다(tmp_path):
    path = Path(write_bronze(tmp_path))
    manifest_path = path.parent / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["sha256"] = "0" * 64
    manifest_path.write_text(json.dumps(manifest))

    with pytest.raises(ValueError, match="manifest와 원본이 다릅니다"):
        validate_bronze(result_for(str(path)), params=bronze_params(tmp_path))

    quarantine = json.loads((path.parent / "_QUARANTINED.json").read_text())
    assert quarantine["layer"] == "bronze"
    assert "sha256" in quarantine["reason"]
    assert not (path.parent / "_SUCCESS").exists()


def test_TX_Bronze를_검증하고_같은지역의_Silver경로를_만든다(
    tmp_path, monkeypatch
):
    bronze_dir = tmp_path / "bronze"
    silver_dir = tmp_path / "silver" / "monthly_taxi_trip"
    monkeypatch.setattr(task_module, "DEFAULT_SILVER_DIR", str(silver_dir))
    path = write_bronze(bronze_dir, service_area="TX")

    result = validate_bronze(
        result_for(path),
        params=bronze_params(bronze_dir, "TX"),
    )

    expected_root = silver_dir / "service_area=TX" / f"year_month={YEAR_MONTH}"
    assert Path(result["silver_version_path"]).parent == expected_root


def test_요청지역과_Bronze경로지역이_다르면_거부한다(tmp_path):
    path = write_bronze(tmp_path, service_area="TX")

    with pytest.raises(ValueError, match="월 파티션 계약과 다릅니다"):
        validate_bronze(
            result_for(path),
            params=bronze_params(tmp_path, "NYC"),
        )


def test_collected_at_디렉터리_Bronze도_검증을_통과한다(tmp_path):
    path = write_directory_bronze(tmp_path)

    validate_bronze(result_for(path), params=bronze_params(tmp_path))


def test_S3_Bronze를_로컬_Path로_변환하지_않고_검증한다(tmp_path, monkeypatch):
    local_path = write_bronze(tmp_path)
    payload = Path(local_path).read_bytes()
    manifest = (Path(local_path).parent / "manifest.json").read_bytes()
    s3_path = (
        "s3://de-theone/bronze/monthly_taxi_trip/service_area=NYC/"
        "year_month=2026-07/collected_at=20260811T085354000000Z/data.parquet"
    )
    result = result_for(local_path)
    result["locations"] = [s3_path]
    monkeypatch.setattr(
        "shared.airflow.common.validation.get_object_stream",
        lambda bucket, key: (io.BytesIO(payload), len(payload)),
    )
    monkeypatch.setattr(
        "shared.airflow.common.validation.get_object_bytes",
        lambda bucket, key: payload,
    )
    monkeypatch.setattr(
        "main.airflow.common.monthly_bronze.get_object_bytes",
        lambda bucket, key: manifest if key.endswith("manifest.json") else payload,
    )
    monkeypatch.setattr(task_module, "existing_silver_partitions", lambda _: [])

    validated = task_module._validate_bronze(
        {"result": result},
        {
            "params": {
                "base_dir": "s3://de-theone/bronze",
                "service_area": "NYC",
            }
        },
    )

    assert validated["locations"] == [s3_path]
    assert validated["silver_version_path"].startswith(
        "s3://de-theone/silver/monthly_taxi_trip/"
    )


def test_S3_Bronze의_Silver버전은_같은버킷의_monthly_taxi_trip경로다():
    file_name = "20260811T085354000000Z.parquet"
    result = {
        "row_count": 3,
        "year_month": YEAR_MONTH,
        "locations": [
            f"s3://de-theone/bronze/monthly_taxi_trip/"
            f"service_area=NYC/year_month={YEAR_MONTH}/{file_name}"
        ],
    }

    actual = task_module.silver_version_path(
        task_module.DEFAULT_SILVER_DIR,
        result,
        "NYC",
    )

    assert str(actual) == (
        f"s3://de-theone/silver/monthly_taxi_trip/"
        f"service_area=NYC/year_month={YEAR_MONTH}/"
        f"source_collected_at={Path(file_name).stem}"
    )


def test_동일한_Bronze도_감시DAG가_호출하면_Silver처리한다(tmp_path, monkeypatch):
    monkeypatch.setattr(task_module, "DEFAULT_SILVER_DIR", str(tmp_path / "silver"))
    path = write_bronze(tmp_path)
    result = result_for(path)
    result["source_changed"] = False

    assert validate_bronze(result, params=bronze_params(tmp_path))[
        "silver_version_path"
    ].endswith("source_collected_at=20260811T085354000000Z")


def test_Bronze_변경여부_신호가_없어도_감시DAG호출이면_처리한다(tmp_path):
    path = write_bronze(tmp_path)
    result = result_for(path)
    result.pop("source_changed")

    assert validate_bronze(result, params=bronze_params(tmp_path))[
        "silver_version_path"
    ].endswith("source_collected_at=20260811T085354000000Z")


def test_Bronze는_레코드_GX없이_manifest와_파일무결성만_검사한다(
    tmp_path, monkeypatch
):
    records = bronze_rows(20)
    records[0]["pickup_datetime"] = None
    records[0]["trip_miles"] = -1
    path = write_bronze(tmp_path, records=records)

    def reject_airflow_gx(*args, **kwargs):
        raise AssertionError("Bronze 레코드 GX는 Spark Silver 책임이어야 합니다")

    monkeypatch.setattr(
        task_module,
        "run_gx_validation",
        reject_airflow_gx,
        raising=False,
    )

    result = validate_bronze(
        result_for(path), params=bronze_params(tmp_path)
    )

    assert result["silver_version_path"].endswith(
        "source_collected_at=20260811T085354000000Z"
    )
    assert (Path(path).parent / "_SUCCESS").is_file()


def test_Bronze_스키마누락은_재수집하지_않고_Spark로_넘긴다(
    tmp_path, monkeypatch
):
    schema = pa.schema(
        field for field in BRONZE_SCHEMA if field.name != "pickup_datetime"
    )
    path = write_bronze(tmp_path, schema=schema)

    def reject_recollect(params):
        raise AssertionError("Bronze 레코드 계약으로 재수집하면 안 됩니다")

    monkeypatch.setattr(task_module, "_collect_bronze", reject_recollect)

    result = validate_bronze(
        result_for(path), params=bronze_params(tmp_path)
    )

    assert result["locations"] == [path]


def test_추가_on_scene_datetime이_있어도_Bronze무결성은_통과한다(
    tmp_path, monkeypatch
):
    """원천이 MONTHLY_TAXI_TRIP_SCHEMA 보다 컬럼이 많아도(TLC 원본처럼) 막지 않습니다.

    물리 스키마 전체 일치는 더 이상 보지 않습니다(#529) — 필수 컬럼만 있으면 통과합니다.
    """
    extra_schema = pa.schema(
        [*BRONZE_SCHEMA, pa.field("on_scene_datetime", pa.timestamp("us"))]
    )
    path = write_bronze(tmp_path, schema=extra_schema)

    warnings = []
    monkeypatch.setattr(
        task_module,
        "send_quality_warning",
        lambda context, **values: warnings.append(values),
    )

    validate_bronze(result_for(path), params=bronze_params(tmp_path))

    assert warnings == []


def test_파일이_없으면_막는다(tmp_path):
    missing = tmp_path / "monthly_taxi_trip" / f"year_month={YEAR_MONTH}" / "missing.parquet"
    result = {
        "row_count": 1,
        "locations": [str(missing)],
        "year_month": YEAR_MONTH,
        "file_size_bytes": 0,
    }
    with pytest.raises(ValueError, match="파일이 없습니다"):
        validate_bronze(result, params=bronze_params(tmp_path))


def test_크기가_다르면_막는다_잘린_다운로드(tmp_path):
    path = write_bronze(tmp_path)
    result = result_for(path)
    result["file_size_bytes"] += 1
    with pytest.raises(ValueError, match="파일 크기"):
        validate_bronze(result, params=bronze_params(tmp_path))


def test_파티션이_year_month와_다르면_막는다(tmp_path):
    path = write_bronze(tmp_path)
    result = result_for(path, year_month="2026-08")
    with pytest.raises(ValueError, match="월 파티션 계약과 다릅니다"):
        validate_bronze(result, params=bronze_params(tmp_path))


def test_파일명의_수집시각이_handler_결과와_다르면_막는다(tmp_path):
    path = write_bronze(tmp_path)
    result = result_for(path)
    result["collected_at"] = "2026-08-11T09:00:00.000000Z"

    with pytest.raises(ValueError, match="collected_at과 다릅니다"):
        validate_bronze(result, params=bronze_params(tmp_path))


def test_Bronze_경로가_base_dir_layout과_다르면_막는다(tmp_path):
    path = write_bronze(tmp_path / "elsewhere")

    with pytest.raises(ValueError, match="base_dir layout과 다릅니다"):
        validate_bronze(result_for(path), params=bronze_params(tmp_path))


def test_행_수가_0이면_막는다(tmp_path):
    path = write_bronze(tmp_path, rows=0)
    result = result_for(path)
    result["row_count"] = 1
    with pytest.raises(ValueError, match="행 수가 수집 결과와 다릅니다"):
        validate_bronze(result, params=bronze_params(tmp_path))


# --- validate_silver -------------------------------------------------------


def silver_rows(count: int = 3, schema=None) -> list[dict]:
    schema = SILVER_SCHEMA if schema is None else schema
    row = {
        field.name: (
            COLLECTED_AT
            if pa.types.is_timestamp(field.type)
            else 1
            if pa.types.is_integer(field.type)
            else 1.0
            if pa.types.is_floating(field.type)
            else "x"
        )
        for field in schema
    }
    return [row.copy() for _ in range(count)]


def write_recon(version_dir: Path, excluded: int = 0) -> None:
    """Spark 가 남기는 `_RECON.json` 을 흉내냅니다.

    `validate_silver` 는 `Bronze = Silver + excluded` 를 이 파일로 판정합니다.
    """
    (version_dir / "_RECON.json").write_text(
        json.dumps(
            {
                "total": 0,
                "valid": 0,
                "invalid": excluded,
                "missing_or_type_mismatch": excluded,
                "invalid_value": 0,
                "invalid_service_tier": 0,
            }
        ),
        encoding="utf-8",
    )


def write_silver(
    silver_dir,
    year_month: str = YEAR_MONTH,
    rows: int = 3,
    schema=None,
    records: list[dict] | None = None,
    service_area: str = "NYC",
    excluded: int = 0,
) -> Path:
    """`validate_silver`가 마커 공개 전에 읽는 최종 경로 part를 씁니다(#912)."""
    schema = SILVER_SCHEMA if schema is None else schema
    records = silver_rows(rows, schema) if records is None else records
    partition = (
        Path(silver_dir) / f"service_area={service_area}"
        / f"year_month={year_month}"
    )
    target = (
        partition
        / "source_collected_at=20260811T085354000000Z/part-00000.parquet"
    )
    target.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(records, schema=schema), target)
    write_recon(target.parent, excluded)
    return partition


def write_committed_silver(
    silver_dir,
    year_month: str = YEAR_MONTH,
    rows: int = 3,
    schema=None,
    service_area: str = "NYC",
) -> Path:
    """검증을 통과해 `_SUCCESS`까지 공개된 Silver를 흉내냅니다."""
    schema = SILVER_SCHEMA if schema is None else schema
    records = silver_rows(rows, schema)
    partition = (
        Path(silver_dir) / f"service_area={service_area}"
        / f"year_month={year_month}"
    )
    partition.mkdir(parents=True, exist_ok=True)
    version = partition / "source_collected_at=20260811T085354000000Z"
    version.mkdir(parents=True, exist_ok=True)
    target = version / "part-00000.parquet"
    pq.write_table(pa.Table.from_pylist(records, schema=schema), target)
    write_recon(version)
    (version / "_SUCCESS").touch()
    return partition


def test_정상_silver_적재는_통과한다(tmp_path, monkeypatch):
    monkeypatch.setattr(task_module, "DEFAULT_SILVER_DIR", str(tmp_path / "silver"))
    bronze_path = write_bronze(tmp_path / "bronze", rows=10)
    write_silver(tmp_path / "silver", rows=5, excluded=5)

    result = result_for(bronze_path)
    quarantine = Path(result["silver_version_path"]) / "_QUARANTINED.json"
    quarantine.write_text("{}")
    validate_silver(result)

    assert Path(result["silver_version_path"]).is_dir()
    assert (Path(result["silver_version_path"]) / "_SUCCESS").is_file()
    assert not quarantine.exists()


def test_Silver는_Airflow요약_GX없이_Spark결과와_파일계약만_확인한다(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(task_module, "DEFAULT_SILVER_DIR", str(tmp_path / "silver"))
    bronze_path = write_bronze(tmp_path / "bronze", rows=10)
    write_silver(tmp_path / "silver", rows=8, excluded=2)

    def reject_airflow_gx(*args, **kwargs):
        raise AssertionError("Silver 레코드 GX는 Spark에서 이미 끝나야 합니다")

    monkeypatch.setattr(
        task_module,
        "run_gx_validation",
        reject_airflow_gx,
        raising=False,
    )

    result = result_for(bronze_path)
    validate_silver(result)

    assert (Path(result["silver_version_path"]) / "_SUCCESS").is_file()


def test_Spark_GX경고비율은_Silver검증에서_Slack으로_알린다(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(task_module, "DEFAULT_SILVER_DIR", str(tmp_path / "silver"))
    bronze_path = write_bronze(tmp_path / "bronze", rows=100)
    partition = write_silver(tmp_path / "silver", rows=98, excluded=2)
    version = next(partition.glob("source_collected_at=*"))
    (version / "_RECON.json").write_text(
        json.dumps(
            {
                "total": 100,
                "valid": 98,
                "invalid": 2,
                "invalid_ratio": 0.02,
                "warning": True,
                "warning_threshold": 0.01,
                "error_threshold": 0.05,
                "missing_or_type_mismatch": 1,
                "invalid_value": 1,
                "invalid_service_tier": 0,
            }
        )
    )
    warnings = []
    monkeypatch.setattr(
        task_module,
        "send_quality_warning",
        lambda context, **values: warnings.append(values),
    )

    validate_silver(result_for(bronze_path), params={})

    assert warnings == [
        {
            "dataset": "monthly_taxi_trip",
            "year_month": YEAR_MONTH,
            "invalid_rows": 2,
            "row_count": 100,
            "invalid_ratio": 0.02,
            "extra_columns": [],
        }
    ]


def test_검증에_실패하면_최종_파일은_있어도_SUCCESS는_없다(tmp_path, monkeypatch):
    monkeypatch.setattr(task_module, "DEFAULT_SILVER_DIR", str(tmp_path / "silver"))
    bronze_path = write_bronze(tmp_path / "bronze", rows=10)
    write_silver(tmp_path / "silver", rows=0)  # 행 수 0 -> GX 검증 실패

    result = result_for(bronze_path)
    with pytest.raises(ValueError):
        validate_silver(result)

    assert Path(result["silver_version_path"]).is_dir()
    assert not (Path(result["silver_version_path"]) / "_SUCCESS").exists()
    quarantine = Path(result["silver_version_path"]) / "_QUARANTINED.json"
    assert json.loads(quarantine.read_text())["layer"] == "silver"
    assert list(Path(result["silver_version_path"]).glob("part-*.parquet"))


def test_silver_파티션에_파일이_없으면_막는다(tmp_path, monkeypatch):
    monkeypatch.setattr(task_module, "DEFAULT_SILVER_DIR", str(tmp_path / "silver"))
    bronze_path = write_bronze(tmp_path / "bronze", rows=10)

    with pytest.raises(ValueError, match="Silver part 파일이 없습니다"):
        validate_silver(result_for(bronze_path))


def test_silver_스키마_컬럼이_다르면_막는다(tmp_path, monkeypatch):
    monkeypatch.setattr(task_module, "DEFAULT_SILVER_DIR", str(tmp_path / "silver"))
    bronze_path = write_bronze(tmp_path / "bronze", rows=10)
    schema = pa.schema(
        field for field in SILVER_SCHEMA if field.name != SILVER_COLUMNS[-1]
    )
    write_silver(tmp_path / "silver", rows=5, schema=schema)

    with pytest.raises(
        ValueError,
        match="Silver 스키마가 계약과 다릅니다",
    ):
        validate_silver(result_for(bronze_path))


def test_silver_행_수가_0이면_막는다(tmp_path, monkeypatch):
    monkeypatch.setattr(task_module, "DEFAULT_SILVER_DIR", str(tmp_path / "silver"))
    bronze_path = write_bronze(tmp_path / "bronze", rows=10)
    write_silver(tmp_path / "silver", rows=0)

    with pytest.raises(ValueError, match="Silver 레코드가 0건입니다"):
        validate_silver(result_for(bronze_path))


def test_on_scene_datetime은_전체_운행계약에서_제외된다():
    from schema.bronze import MONTHLY_TAXI_TRIP_SCHEMA as BRONZE_SCHEMA
    from schema.silver.monthly_taxi_trip import FINAL_SCHEMA, REQUIRED_COLUMNS
    from schema.source import MONTHLY_TAXI_TRIP_SCHEMA as SOURCE_SCHEMA

    assert "on_scene_datetime" not in SOURCE_SCHEMA.names
    assert "on_scene_datetime" not in BRONZE_SCHEMA.names
    assert "on_scene_datetime" not in SILVER_SCHEMA.names
    assert "on_scene_datetime" not in FINAL_SCHEMA.names
    assert "on_scene_datetime" not in REQUIRED_COLUMNS


def test_silver_FINAL_SCHEMA_타입이_다르면_GX가_실패한다(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(task_module, "DEFAULT_SILVER_DIR", str(tmp_path / "silver"))
    bronze_path = write_bronze(tmp_path / "bronze", rows=10)
    schema = pa.schema(
        pa.field(field.name, pa.int32())
        if field.name == "trip_time"
        else field
        for field in SILVER_SCHEMA
    )
    write_silver(tmp_path / "silver", rows=5, schema=schema)

    with pytest.raises(
        ValueError,
        match="Silver 스키마가 계약과 다릅니다",
    ):
        validate_silver(result_for(bronze_path))


def test_silver_timestamp_unit이_달라도_논리_타입이_같으면_통과한다(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(task_module, "DEFAULT_SILVER_DIR", str(tmp_path / "silver"))
    bronze_path = write_bronze(tmp_path / "bronze", rows=10)
    schema = pa.schema(
        pa.field(field.name, pa.timestamp("ms"))
        if field.name == "pickup_datetime"
        else field
        for field in SILVER_SCHEMA
    )
    write_silver(tmp_path / "silver", rows=5, schema=schema, excluded=5)

    validate_silver(result_for(bronze_path))


def test_silver_timestamp_timezone이_기대_스키마와_다르면_실패한다(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(task_module, "DEFAULT_SILVER_DIR", str(tmp_path / "silver"))
    bronze_path = write_bronze(tmp_path / "bronze", rows=10)
    schema = pa.schema(
        pa.field(field.name, pa.timestamp("ms", tz="UTC"))
        if field.name == "pickup_datetime"
        else field
        for field in SILVER_SCHEMA
    )
    write_silver(tmp_path / "silver", rows=5, schema=schema)

    with pytest.raises(
        ValueError,
        match="Silver 스키마가 계약과 다릅니다",
    ):
        validate_silver(result_for(bronze_path))


def test_silver_행_수가_bronze_보다_많으면_막는다(tmp_path, monkeypatch):
    monkeypatch.setattr(task_module, "DEFAULT_SILVER_DIR", str(tmp_path / "silver"))
    bronze_path = write_bronze(tmp_path / "bronze", rows=3)
    write_silver(tmp_path / "silver", rows=5)

    with pytest.raises(ValueError, match="reconciliation 실패"):
        validate_silver(result_for(bronze_path))


def test_대량_유실은_제외_건수로_설명되지_않으면_막는다(tmp_path, monkeypatch):
    """예전 검사는 `silver_rows > bronze_rows` 만 봤다.

    그래서 Bronze 10건이 Silver 1건이 되어도 통과했다 — 정확히 이 계열이 이번
    파이프라인에서 반복된 "조용히 틀린 값" 이다. 보존식은 줄어든 만큼이 Spark 가
    보고한 제외 건수로 설명되는지 본다.
    """
    monkeypatch.setattr(task_module, "DEFAULT_SILVER_DIR", str(tmp_path / "silver"))
    bronze_path = write_bronze(tmp_path / "bronze", rows=10)
    # Spark 는 "아무것도 안 걸렀다" 고 보고했는데 실제로는 1건만 남았다.
    write_silver(tmp_path / "silver", rows=1, excluded=0)

    with pytest.raises(ValueError, match="bronze=10 silver=1 excluded=0"):
        validate_silver(result_for(bronze_path))


def test_제외_건수로_설명되면_줄어들어도_통과한다(tmp_path, monkeypatch):
    """정상 실행은 행이 줄어든다 — 필수값·값범위·등급으로 걸러지기 때문이다.

    줄어든 것 자체를 막으면 파이프라인이 매번 죽는다. 설명되는지만 본다.
    """
    monkeypatch.setattr(task_module, "DEFAULT_SILVER_DIR", str(tmp_path / "silver"))
    bronze_path = write_bronze(tmp_path / "bronze", rows=10)
    write_silver(tmp_path / "silver", rows=7, excluded=3)

    result = result_for(bronze_path)
    validate_silver(result)

    assert (Path(result["silver_version_path"]) / "_SUCCESS").is_file()


def test_reconciliation_sidecar가_없으면_막는다(tmp_path, monkeypatch):
    """없는 걸 통과시키면 옛 코드로 돈 실행이 조용히 검사를 건너뛴다."""
    monkeypatch.setattr(task_module, "DEFAULT_SILVER_DIR", str(tmp_path / "silver"))
    bronze_path = write_bronze(tmp_path / "bronze", rows=5)
    partition = write_silver(tmp_path / "silver", rows=5)
    for sidecar in partition.rglob("_RECON.json"):
        sidecar.unlink()

    with pytest.raises(ValueError, match="sidecar 가 없습니다"):
        validate_silver(result_for(bronze_path))


def test_쓰기_전에_있던_파티션이_사라지면_165_재발로_막는다(tmp_path, monkeypatch):
    """정적 overwrite(#165)가 재발하면 이번에 쓴 달만 남고 나머지가 지워집니다."""
    monkeypatch.setattr(task_module, "DEFAULT_SILVER_DIR", str(tmp_path / "silver"))
    bronze_path = write_bronze(tmp_path / "bronze", rows=10)
    write_silver(tmp_path / "silver", year_month=YEAR_MONTH, rows=5, excluded=5)
    # 쓰기 전에는 2026-05 도 있었는데 지금은 없는 상황 — 정확히 #165 의 signature
    result = result_for(bronze_path)
    result["silver_partitions_before"] = ["year_month=2026-05"]

    with pytest.raises(ValueError, match="쓰기 전에 있던 Silver 파티션이 사라졌습니다"):
        validate_silver(result)


def test_과거_달_백필은_통과한다(tmp_path, monkeypatch):
    """과거 달을 새로 채우는 것은 정상입니다. 예전 검사는 직전 달이 없다는 이유로
    이걸 항상 막았습니다 (#532) — 어느 달을 넣든 그 직전 달은 없기 마련입니다."""
    monkeypatch.setattr(task_module, "DEFAULT_SILVER_DIR", str(tmp_path / "silver"))
    bronze_path = write_bronze(tmp_path / "bronze", rows=10)
    write_committed_silver(tmp_path / "silver", year_month="2026-06", rows=5)
    write_silver(tmp_path / "silver", year_month=YEAR_MONTH, rows=5, excluded=5)
    result = result_for(bronze_path)
    result["silver_partitions_before"] = ["year_month=2026-06"]

    validate_silver(result)


def test_쓰기_전_스냅샷이_없어도_통과한다(tmp_path, monkeypatch):
    """첫 실행이나 예전 XCom 에는 이 키가 없습니다. 없다고 막으면 안 됩니다."""
    monkeypatch.setattr(task_module, "DEFAULT_SILVER_DIR", str(tmp_path / "silver"))
    bronze_path = write_bronze(tmp_path / "bronze", rows=10)
    write_silver(tmp_path / "silver", year_month=YEAR_MONTH, rows=5, excluded=5)

    validate_silver(result_for(bronze_path))


def test_같은_월_재처리중_SUCCESS가_없어도_다시_공개한다(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(task_module, "DEFAULT_SILVER_DIR", str(tmp_path / "silver"))
    bronze_path = write_bronze(tmp_path / "bronze", rows=10)
    write_silver(tmp_path / "silver", year_month=YEAR_MONTH, rows=5, excluded=5)
    result = result_for(bronze_path)
    result["silver_partitions_before"] = [f"year_month={YEAR_MONTH}"]

    validate_silver(result)

    assert (Path(result["silver_version_path"]) / "_SUCCESS").is_file()


def test_쓰기_전_파티션_목록은_parquet_이_있는_것만_센다(tmp_path):
    """빈 디렉터리가 남아 있는 경우가 있습니다. 그걸 세면 "사라졌다" 오탐이 납니다."""
    silver = tmp_path / "silver"
    write_committed_silver(silver, year_month="2026-06", rows=5)
    (silver / "service_area=NYC/year_month=2026-07").mkdir(parents=True)

    assert task_module.existing_silver_partitions(
        str(silver / "service_area=NYC")
    ) == ["year_month=2026-06"]


def test_쓰기_전_파티션_목록은_SUCCESS_없는_파일은_세지_않는다(tmp_path):
    """검증 전 최종 파일은 있어도 아직 공개본이 아닙니다(#912)."""
    silver = tmp_path / "silver"
    write_silver(silver, year_month="2026-06", rows=5)

    assert task_module.existing_silver_partitions(
        str(silver / "service_area=NYC")
    ) == []


def test_Silver_디렉터리가_없으면_빈_목록이다(tmp_path):
    assert task_module.existing_silver_partitions(str(tmp_path / "none")) == []


def test_지역_계층_아래_파티션도_센다(tmp_path):
    """지역 계층(#674)이 들어가도 가드가 빈 목록을 반환하면 안 됩니다.

    빈 목록이면 `before - after` 가 항상 공집합이 되어 **#165 가드가 조용히
    통과**합니다 — 파티션이 실제로 사라져도 아무도 모릅니다. 이 함수는 호출부가
    `version_path.parent.parent` 로 넘겨주는 **지역 스코프 루트**를 받으므로,
    그 루트 아래를 정상적으로 세는지 확인합니다.
    """
    scoped_root = tmp_path / "silver" / "service_area=NYC"
    write_committed_silver(
        tmp_path / "silver", year_month="2026-06", rows=5
    )

    assert task_module.existing_silver_partitions(str(scoped_root)) == [
        "year_month=2026-06"
    ]


def test_지역_스코프_루트는_다른_지역_파티션을_섞지_않는다(tmp_path):
    """before/after 가 서로 다른 지역을 보면 가드가 거짓 실패합니다."""
    silver = tmp_path / "silver"
    write_committed_silver(
        silver, year_month="2026-06", rows=5, service_area="NYC"
    )
    write_committed_silver(
        silver, year_month="2026-07", rows=5, service_area="TX"
    )

    assert task_module.existing_silver_partitions(
        str(silver / "service_area=NYC")
    ) == ["year_month=2026-06"]
    assert task_module.existing_silver_partitions(
        str(silver / "service_area=TX")
    ) == ["year_month=2026-07"]
