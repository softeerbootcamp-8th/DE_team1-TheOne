"""HVFHV DAG의 경계 검사와 GX 데이터 품질 규칙을 검증합니다.

Bronze 는 원본 Parquet 을 파싱 없이 그대로 받아 쓰고, 행 수도 세지 않고 파일 1개를
1로 셉니다. 그래서 다운로드가 잘려도 핸들러는 성공으로 끝납니다. Silver 는 Spark
BashOperator 라 handler 결과 dict 자체가 없어 파티션을 직접 열어서 봐야 합니다.
검증 태스크의 값어치는 "통과한다"가 아니라 "불량을 통과시키지 않는다"입니다.

대용량 원본을 Pandas 에 모두 올리지 않도록 Parquet 을 배치 단위로 검사합니다.
실제 Parquet 을 tmp_path 에 쓰며 네트워크와 Spark 는 사용하지 않습니다.
Silver timestamp는 unit 차이는 허용하되 timezone identity는 유지합니다.
"""

import importlib
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from dags import hvfhv_raw_to_silver_dag as dag_module
from scripts.hvfhv_raw_to_silver import tasks as task_module

bronze_loader = importlib.import_module("lambda.functions.hvfhv_raw_to_bronze.loader")
transformer = importlib.import_module("jobs.bronze_to_silver.hvfhv.transformer")

DAG = dag_module.hvfhv_dag
COLLECTED_AT = datetime(2026, 8, 11, 8, 53, 54, tzinfo=timezone.utc)
YEAR_MONTH = "2026-07"
SILVER_COLUMNS = [field.name for field in transformer.FINAL_SCHEMA.fields if field.name != "year_month"]
BRONZE_REQUIRED_COLUMNS = transformer.REQUIRED_COLUMNS
SILVER_REQUIRED_COLUMNS = [
    field.name
    for field in transformer.FINAL_SCHEMA.fields
    if not field.nullable and field.name != "year_month"
]


def spark_type_to_arrow(data_type):
    return {
        "string": pa.string(),
        "timestamp": pa.timestamp("us"),
        "int": pa.int32(),
        "bigint": pa.int64(),
        "double": pa.float64(),
    }[data_type.simpleString()]


SILVER_SCHEMA = pa.schema(
    [
        pa.field(field.name, spark_type_to_arrow(field.dataType))
        for field in transformer.FINAL_SCHEMA.fields
        if field.name != "year_month"
    ]
)

validate_bronze = DAG.get_task("validate_bronze").python_callable
validate_silver = DAG.get_task("validate_silver").python_callable


def bronze_rows(count: int = 3, schema=None) -> list[dict]:
    schema = bronze_loader.SCHEMA if schema is None else schema
    row = {
        field.name: COLLECTED_AT if pa.types.is_timestamp(field.type)
        else 1 if pa.types.is_integer(field.type)
        else 1.0 if pa.types.is_floating(field.type)
        else "x"
        for field in schema
    }
    return [row.copy() for _ in range(count)]


def write_bronze(
    base_dir,
    year_month: str = YEAR_MONTH,
    rows: int = 3,
    schema=None,
    records: list[dict] | None = None,
) -> str:
    schema = bronze_loader.SCHEMA if schema is None else schema
    records = bronze_rows(rows, schema) if records is None else records
    path = (
        bronze_loader.HvfhvBronzeLoader(
            str(base_dir), year_month, COLLECTED_AT
        ).partition_path()
        / f"{COLLECTED_AT:%Y%m%dT%H%M%SZ}.parquet"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(records, schema=schema), path)
    return str(path)


def result_for(path: str, year_month: str = YEAR_MONTH) -> dict:
    return {
        "row_count": 1,
        "locations": [path],
        "year_month": year_month,
        "file_size_bytes": Path(path).stat().st_size,
    }


def bronze_params(base_dir) -> dict:
    return {"base_dir": str(base_dir)}


def test_Validation_Task에_재시도와_Slack_콜백이_연결된다():
    for task_id in ("validate_bronze", "validate_silver"):
        validation_task = DAG.get_task(task_id)
        assert validation_task.retries == 1
        assert validation_task.retry_delay == timedelta(minutes=10)
        assert task_module.slack_failure_callback in validation_task.on_failure_callback


def test_정상_적재는_통과한다(tmp_path):
    path = write_bronze(tmp_path)
    validate_bronze(result_for(path), params=bronze_params(tmp_path))


def test_파일이_없으면_막는다(tmp_path):
    missing = tmp_path / "hvfhv" / f"year_month={YEAR_MONTH}" / "missing.parquet"
    result = {
        "row_count": 1,
        "locations": [str(missing)],
        "year_month": YEAR_MONTH,
        "file_size_bytes": 0,
    }
    with pytest.raises(ValueError, match="파일이 없거나"):
        validate_bronze(result, params=bronze_params(tmp_path))


def test_크기가_다르면_막는다_잘린_다운로드(tmp_path):
    path = write_bronze(tmp_path)
    result = result_for(path)
    result["file_size_bytes"] += 1
    with pytest.raises(ValueError, match="크기가 다릅니다"):
        validate_bronze(result, params=bronze_params(tmp_path))


def test_파티션이_year_month와_다르면_막는다(tmp_path):
    path = write_bronze(tmp_path)
    result = result_for(path, year_month="2026-08")
    with pytest.raises(ValueError, match="파티션이 year_month와 다릅니다"):
        validate_bronze(result, params=bronze_params(tmp_path))


def test_Bronze_경로가_base_dir_layout과_다르면_막는다(tmp_path):
    path = write_bronze(tmp_path / "elsewhere")

    with pytest.raises(ValueError, match="layout 규칙과 다릅니다"):
        validate_bronze(result_for(path), params=bronze_params(tmp_path))


# 2026-06 원본 Parquet 의 footer 에서 직접 읽은 실제 시그니처입니다.
# `bronze_loader.SCHEMA` 로 픽스처를 만들면 SCHEMA 가 틀려도 자기 자신과 비교돼
# 통과합니다. 실제 값과의 대조는 이렇게 문자열을 박아두어야만 됩니다 (#324).
TLC_SCHEMA_SIGNATURE = (
    "hvfhs_license_num:large_string|dispatching_base_num:large_string"
    "|originating_base_num:large_string|request_datetime:timestamp[us]"
    "|on_scene_datetime:timestamp[us]|pickup_datetime:timestamp[us]"
    "|dropoff_datetime:timestamp[us]|PULocationID:int32|DOLocationID:int32"
    "|trip_miles:double|trip_time:int64|base_passenger_fare:double|tolls:double"
    "|bcf:double|sales_tax:double|congestion_surcharge:double|airport_fee:double"
    "|tips:double|driver_pay:double|shared_request_flag:large_string"
    "|shared_match_flag:large_string|access_a_ride_flag:large_string"
    "|wav_request_flag:large_string|wav_match_flag:large_string"
    "|cbd_congestion_fee:double"
)


def test_실제_TLC_원본_스키마와_SCHEMA_가_같다():
    """틀리면 Bronze 검증이 **어떤 달을 넣어도** 통과하지 못합니다.

    Bronze Loader 는 원본 바이트를 파싱 없이 그대로 씁니다. 따라서 읽어들인
    스키마는 항상 TLC 의 물리 타입이고, `SCHEMA` 가 그와 다르면 영원히 불일치입니다.
    """
    assert task_module._schema_signature(bronze_loader.SCHEMA) == TLC_SCHEMA_SIGNATURE


def test_cbd_congestion_fee_가_없는_2024년_원본도_통과한다(tmp_path):
    """부트스트랩 풀이 2024년 12개월을 쓰므로 그 달들도 백필할 수 있어야 합니다."""
    path = write_bronze(tmp_path, schema=bronze_loader.LEGACY_SCHEMA)

    validate_bronze(result_for(path), params=bronze_params(tmp_path))


def test_스키마가_다르면_막는다_잘린_다운로드(tmp_path):
    broken_schema = pa.schema([("hvfhs_license_num", pa.string())])
    path = write_bronze(tmp_path, schema=broken_schema)
    with pytest.raises(
        ValueError, match=r"expect_column_values_to_be_in_set\[schema_signature\]"
    ):
        validate_bronze(result_for(path), params=bronze_params(tmp_path))


def test_Spark_필수_컬럼이_없으면_GX가_실패한다(tmp_path):
    missing = "pickup_datetime"
    schema = pa.schema(
        field for field in bronze_loader.SCHEMA if field.name != missing
    )
    path = write_bronze(tmp_path, schema=schema)

    with pytest.raises(
        ValueError,
        match=r"expect_column_values_to_be_in_set\[missing_required_columns\]",
    ):
        validate_bronze(result_for(path), params=bronze_params(tmp_path))


def test_행_수가_0이면_막는다(tmp_path):
    path = write_bronze(tmp_path, rows=0)
    with pytest.raises(
        ValueError, match=r"expect_column_values_to_be_between\[row_count\]"
    ):
        validate_bronze(result_for(path), params=bronze_params(tmp_path))


def test_필수값_NULL_행이_20퍼센트_미만이면_기존_Spark_정책대로_통과한다(tmp_path):
    records = bronze_rows(10)
    records[0]["pickup_datetime"] = None
    path = write_bronze(tmp_path, records=records)

    validate_bronze(result_for(path), params=bronze_params(tmp_path))


def test_필수값_NULL_행이_정확히_20퍼센트면_GX가_실패한다(
    tmp_path, caplog
):
    records = bronze_rows(10)
    records[0]["pickup_datetime"] = None
    records[1]["dropoff_datetime"] = None
    path = write_bronze(tmp_path, records=records)

    with caplog.at_level("ERROR"), pytest.raises(
        ValueError,
        match=r"expect_column_values_to_be_between\[invalid_required_row_ratio\]",
    ):
        validate_bronze(result_for(path), params=bronze_params(tmp_path))

    assert "gx_validation failed layer=bronze" in caplog.text
    assert "column=invalid_required_row_ratio" in caplog.text
    assert "observed_value=[0.2]" in caplog.text


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


def write_silver(
    silver_dir,
    year_month: str = YEAR_MONTH,
    rows: int = 3,
    schema=None,
    records: list[dict] | None = None,
) -> Path:
    schema = SILVER_SCHEMA if schema is None else schema
    records = silver_rows(rows, schema) if records is None else records
    partition = Path(silver_dir) / f"year_month={year_month}"
    partition.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.Table.from_pylist(records, schema=schema),
        partition / "part-0.parquet",
    )
    return partition


def test_정상_silver_적재는_통과한다(tmp_path, monkeypatch):
    monkeypatch.setattr(task_module, "DEFAULT_SILVER_DIR", str(tmp_path / "silver"))
    bronze_path = write_bronze(tmp_path / "bronze", rows=10)
    write_silver(tmp_path / "silver", rows=5)

    validate_silver(result_for(bronze_path))


def test_silver_파티션에_파일이_없으면_막는다(tmp_path, monkeypatch):
    monkeypatch.setattr(task_module, "DEFAULT_SILVER_DIR", str(tmp_path / "silver"))
    bronze_path = write_bronze(tmp_path / "bronze", rows=10)

    with pytest.raises(ValueError, match="Parquet 파일이 없습니다"):
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
        match=r"expect_column_values_to_be_in_set\[schema_signature\]",
    ):
        validate_silver(result_for(bronze_path))


def test_silver_행_수가_0이면_막는다(tmp_path, monkeypatch):
    monkeypatch.setattr(task_module, "DEFAULT_SILVER_DIR", str(tmp_path / "silver"))
    bronze_path = write_bronze(tmp_path / "bronze", rows=10)
    write_silver(tmp_path / "silver", rows=0)

    with pytest.raises(
        ValueError, match=r"expect_column_values_to_be_between\[row_count\]"
    ):
        validate_silver(result_for(bronze_path))


@pytest.mark.parametrize("column", SILVER_REQUIRED_COLUMNS)
def test_silver_필수값이_NULL이면_GX가_실패한다(
    tmp_path, monkeypatch, caplog, column
):
    monkeypatch.setattr(task_module, "DEFAULT_SILVER_DIR", str(tmp_path / "silver"))
    bronze_path = write_bronze(tmp_path / "bronze", rows=10)
    records = silver_rows(5)
    records[0][column] = None
    write_silver(tmp_path / "silver", records=records)

    with caplog.at_level("ERROR"), pytest.raises(
        ValueError,
        match=rf"expect_column_values_to_be_in_set\[{column}_null_count\]",
    ):
        validate_silver(result_for(bronze_path))

    assert "gx_validation failed layer=silver" in caplog.text
    assert f"column={column}_null_count" in caplog.text
    assert "observed_value=[1]" in caplog.text


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
        match=r"expect_column_values_to_be_in_set\[schema_signature\]",
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
    write_silver(tmp_path / "silver", rows=5, schema=schema)

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
        match=r"expect_column_values_to_be_in_set\[schema_signature\]",
    ):
        validate_silver(result_for(bronze_path))


def test_silver_행_수가_bronze_보다_많으면_막는다(tmp_path, monkeypatch):
    monkeypatch.setattr(task_module, "DEFAULT_SILVER_DIR", str(tmp_path / "silver"))
    bronze_path = write_bronze(tmp_path / "bronze", rows=3)
    write_silver(tmp_path / "silver", rows=5)

    with pytest.raises(ValueError, match="Bronze 보다 많습니다"):
        validate_silver(result_for(bronze_path))


def test_직전_달_파티션이_사라지면_165_재발로_막는다(tmp_path, monkeypatch):
    """정적 overwrite(#165)가 재발하면 최신 달만 남고 다른 달은 지워집니다."""
    monkeypatch.setattr(task_module, "DEFAULT_SILVER_DIR", str(tmp_path / "silver"))
    bronze_path = write_bronze(tmp_path / "bronze", rows=10)
    write_silver(tmp_path / "silver", year_month=YEAR_MONTH, rows=5)
    # 직전 달(2026-06)이 아니라 두 달 전(2026-05)만 남아 있는 상황 — #165 재발
    write_silver(tmp_path / "silver", year_month="2026-05", rows=5)

    with pytest.raises(ValueError, match="직전 달 파티션이 사라졌습니다"):
        validate_silver(result_for(bronze_path))


def test_직전_달_파티션이_있으면_통과한다(tmp_path, monkeypatch):
    monkeypatch.setattr(task_module, "DEFAULT_SILVER_DIR", str(tmp_path / "silver"))
    bronze_path = write_bronze(tmp_path / "bronze", rows=10)
    write_silver(tmp_path / "silver", year_month=YEAR_MONTH, rows=5)
    write_silver(tmp_path / "silver", year_month="2026-06", rows=5)

    validate_silver(result_for(bronze_path))


def test_Bronze_GX_실패는_재시도후_Spark와_Silver를_실행하지_않는다(
    tmp_path, monkeypatch, caplog
):
    records = bronze_rows(10)
    records[0]["pickup_datetime"] = None
    records[1]["dropoff_datetime"] = None
    path = write_bronze(tmp_path, records=records)
    result = result_for(path)
    result.update({"year": "2026", "month": "07"})

    raw_task = DAG.get_task("raw_to_bronze")
    validation_task = DAG.get_task("validate_bronze")
    callbacks = []
    monkeypatch.setattr(raw_task, "python_callable", lambda **_: result)
    monkeypatch.setattr(validation_task, "retry_delay", timedelta(0))
    monkeypatch.setattr(
        validation_task,
        "on_failure_callback",
        [lambda context: callbacks.append(context["task_instance"].task_id)],
    )

    run = DAG.test(
        logical_date=datetime(2026, 8, 13, tzinfo=timezone.utc),
        run_conf={
            "year": "2026",
            "month": "07",
            "base_dir": str(tmp_path),
        },
    )
    instances = {instance.task_id: instance for instance in run.get_task_instances()}

    assert run.state == "failed"
    assert instances["raw_to_bronze"].state == "success"
    assert instances["validate_bronze"].state == "failed"
    assert instances["validate_bronze"].try_number == 2
    assert instances["bronze_to_silver"].state == "upstream_failed"
    assert instances["validate_silver"].state == "upstream_failed"
    assert callbacks == ["validate_bronze"]
    assert "gx_validation failed layer=bronze" in caplog.text
    assert "column=invalid_required_row_ratio" in caplog.text
    assert "observed_value=[0.2]" in caplog.text
