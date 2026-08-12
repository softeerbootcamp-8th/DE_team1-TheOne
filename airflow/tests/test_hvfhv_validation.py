"""HVFHV DAG의 validate_bronze/validate_silver 태스크가 실제로 불량을 잡는지 봅니다.

Bronze 는 원본 Parquet 을 파싱 없이 그대로 받아 쓰고, 행 수도 세지 않고 파일 1개를
1로 셉니다. 그래서 다운로드가 잘려도 핸들러는 성공으로 끝납니다. Silver 는 Spark
BashOperator 라 handler 결과 dict 자체가 없어 파티션을 직접 열어서 봐야 합니다.
검증 태스크의 값어치는 "통과한다"가 아니라 "불량을 통과시키지 않는다"입니다.

실제 Parquet 을 tmp_path 에 씁니다. 네트워크는 타지 않고 Spark 도 띄우지 않습니다.
"""

import importlib
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from dags import hvfhv_raw_to_silver_dag as dag_module

bronze_loader = importlib.import_module("lambda.functions.hvfhv_raw_to_bronze.loader")
transformer = importlib.import_module("jobs.bronze_to_silver.hvfhv.transformer")

DAG = dag_module.hvfhv_dag
COLLECTED_AT = datetime(2026, 8, 11, 8, 53, 54, tzinfo=timezone.utc)
YEAR_MONTH = "2026-07"
SILVER_COLUMNS = [field.name for field in transformer.FINAL_SCHEMA.fields if field.name != "year_month"]

validate_bronze = DAG.get_task("validate_bronze").python_callable
validate_silver = DAG.get_task("validate_silver").python_callable


def write_bronze(base_dir, year_month: str = YEAR_MONTH, rows: int = 3, schema=None) -> str:
    schema = schema or bronze_loader.SCHEMA
    row = {
        field.name: COLLECTED_AT if pa.types.is_timestamp(field.type)
        else 1 if pa.types.is_integer(field.type)
        else 1.0 if pa.types.is_floating(field.type)
        else "x"
        for field in schema
    }
    path = bronze_loader.HvfhvBronzeLoader(str(base_dir), year_month, COLLECTED_AT).partition_path() / "x.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist([row] * rows, schema=schema), path)
    return str(path)


def result_for(path: str, year_month: str = YEAR_MONTH) -> dict:
    return {
        "locations": [path],
        "year_month": year_month,
        "file_size_bytes": Path(path).stat().st_size,
    }


def test_정상_적재는_통과한다(tmp_path):
    path = write_bronze(tmp_path)
    validate_bronze(result_for(path))


def test_파일이_없으면_막는다(tmp_path):
    missing = tmp_path / "hvfhv" / f"year_month={YEAR_MONTH}" / "missing.parquet"
    result = {"locations": [str(missing)], "year_month": YEAR_MONTH, "file_size_bytes": 0}
    with pytest.raises(ValueError, match="파일이 없거나"):
        validate_bronze(result)


def test_크기가_다르면_막는다_잘린_다운로드(tmp_path):
    path = write_bronze(tmp_path)
    result = result_for(path)
    result["file_size_bytes"] += 1
    with pytest.raises(ValueError, match="크기가 다릅니다"):
        validate_bronze(result)


def test_파티션이_year_month와_다르면_막는다(tmp_path):
    path = write_bronze(tmp_path)
    result = result_for(path, year_month="2026-08")
    with pytest.raises(ValueError, match="파티션이 year_month와 다릅니다"):
        validate_bronze(result)


def test_스키마가_다르면_막는다_잘린_다운로드(tmp_path):
    broken_schema = pa.schema([("hvfhs_license_num", pa.string())])
    path = write_bronze(tmp_path, schema=broken_schema)
    with pytest.raises(ValueError, match="스키마가 loader.SCHEMA"):
        validate_bronze(result_for(path))


def test_행_수가_0이면_막는다(tmp_path):
    path = write_bronze(tmp_path, rows=0)
    with pytest.raises(ValueError, match="행 수가 0"):
        validate_bronze(result_for(path))


# --- validate_silver -------------------------------------------------------


def write_silver(silver_dir, year_month: str = YEAR_MONTH, rows: int = 3, columns=None) -> Path:
    columns = SILVER_COLUMNS if columns is None else columns
    schema = pa.schema([(name, pa.string()) for name in columns])
    row = {name: "x" for name in columns}
    partition = Path(silver_dir) / f"year_month={year_month}"
    partition.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist([row] * rows, schema=schema), partition / "part-0.parquet")
    return partition


def test_정상_silver_적재는_통과한다(tmp_path, monkeypatch):
    monkeypatch.setattr(dag_module, "DEFAULT_SILVER_DIR", str(tmp_path / "silver"))
    bronze_path = write_bronze(tmp_path / "bronze", rows=10)
    write_silver(tmp_path / "silver", rows=5)

    validate_silver(result_for(bronze_path))


def test_silver_파티션에_파일이_없으면_막는다(tmp_path, monkeypatch):
    monkeypatch.setattr(dag_module, "DEFAULT_SILVER_DIR", str(tmp_path / "silver"))
    bronze_path = write_bronze(tmp_path / "bronze", rows=10)

    with pytest.raises(ValueError, match="Parquet 파일이 없습니다"):
        validate_silver(result_for(bronze_path))


def test_silver_스키마_컬럼이_다르면_막는다(tmp_path, monkeypatch):
    monkeypatch.setattr(dag_module, "DEFAULT_SILVER_DIR", str(tmp_path / "silver"))
    bronze_path = write_bronze(tmp_path / "bronze", rows=10)
    write_silver(tmp_path / "silver", rows=5, columns=SILVER_COLUMNS[:-1])

    with pytest.raises(ValueError, match="스키마 컬럼이 FINAL_SCHEMA"):
        validate_silver(result_for(bronze_path))


def test_silver_행_수가_0이면_막는다(tmp_path, monkeypatch):
    monkeypatch.setattr(dag_module, "DEFAULT_SILVER_DIR", str(tmp_path / "silver"))
    bronze_path = write_bronze(tmp_path / "bronze", rows=10)
    write_silver(tmp_path / "silver", rows=0)

    with pytest.raises(ValueError, match="Silver 행 수가 0"):
        validate_silver(result_for(bronze_path))


def test_silver_행_수가_bronze_보다_많으면_막는다(tmp_path, monkeypatch):
    monkeypatch.setattr(dag_module, "DEFAULT_SILVER_DIR", str(tmp_path / "silver"))
    bronze_path = write_bronze(tmp_path / "bronze", rows=3)
    write_silver(tmp_path / "silver", rows=5)

    with pytest.raises(ValueError, match="Bronze 보다 많습니다"):
        validate_silver(result_for(bronze_path))


def test_직전_달_파티션이_사라지면_165_재발로_막는다(tmp_path, monkeypatch):
    """정적 overwrite(#165)가 재발하면 최신 달만 남고 다른 달은 지워집니다."""
    monkeypatch.setattr(dag_module, "DEFAULT_SILVER_DIR", str(tmp_path / "silver"))
    bronze_path = write_bronze(tmp_path / "bronze", rows=10)
    write_silver(tmp_path / "silver", year_month=YEAR_MONTH, rows=5)
    # 직전 달(2026-06)이 아니라 두 달 전(2026-05)만 남아 있는 상황 — #165 재발
    write_silver(tmp_path / "silver", year_month="2026-05", rows=5)

    with pytest.raises(ValueError, match="직전 달 파티션이 사라졌습니다"):
        validate_silver(result_for(bronze_path))


def test_직전_달_파티션이_있으면_통과한다(tmp_path, monkeypatch):
    monkeypatch.setattr(dag_module, "DEFAULT_SILVER_DIR", str(tmp_path / "silver"))
    bronze_path = write_bronze(tmp_path / "bronze", rows=10)
    write_silver(tmp_path / "silver", year_month=YEAR_MONTH, rows=5)
    write_silver(tmp_path / "silver", year_month="2026-06", rows=5)

    validate_silver(result_for(bronze_path))
