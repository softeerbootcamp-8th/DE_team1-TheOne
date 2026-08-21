"""HVFHV DAG의 경계 검사와 GX 데이터 품질 규칙을 검증합니다.

Bronze 는 제공된 Parquet 원본의 경로·크기·footer 행 수를 검증합니다.
Silver 는 Spark BashOperator 라 handler 결과 dict 자체가 없어 파티션을
직접 열어서 봐야 합니다.
검증 태스크의 값어치는 "통과한다"가 아니라 "불량을 통과시키지 않는다"입니다.

대용량 원본을 Pandas 에 모두 올리지 않도록 Parquet 을 배치 단위로 검사합니다.
실제 Parquet 을 tmp_path 에 쓰며 네트워크와 Spark 는 사용하지 않습니다.
Silver timestamp는 unit 차이는 허용하되 timezone identity는 유지합니다.
"""

from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from dags import hvfhv_raw_to_silver_dag as dag_module
from main.airflow.scripts.hvfhv_raw_to_silver import tasks as task_module

DAG = dag_module.hvfhv_dag
COLLECTED_AT = datetime(2026, 8, 11, 8, 53, 54, tzinfo=timezone.utc)
YEAR_MONTH = "2026-07"
SILVER_SCHEMA = task_module.SILVER_SCHEMA
SILVER_COLUMNS = list(SILVER_SCHEMA.names)
SILVER_REQUIRED_COLUMNS = [
    name
    for name in SILVER_SCHEMA.names
    if name in task_module.SILVER_REQUIRED_NON_NULL
]
SILVER_NULLABLE_COLUMNS = [
    name
    for name in SILVER_SCHEMA.names
    if name not in task_module.SILVER_REQUIRED_NON_NULL
]

validate_bronze = DAG.get_task("validate_bronze").python_callable
validate_silver = DAG.get_task("validate_silver").python_callable


def bronze_rows(count: int = 3, schema=None) -> list[dict]:
    schema = task_module.SCHEMA if schema is None else schema
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
    schema = task_module.SCHEMA if schema is None else schema
    records = bronze_rows(rows, schema) if records is None else records
    path = (
        Path(base_dir)
        / "hvfhv"
        / f"year_month={year_month}"
        / "20260811T085354000000Z.parquet"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(records, schema=schema), path)
    return str(path)


def result_for(path: str, year_month: str = YEAR_MONTH) -> dict:
    parquet = pq.ParquetFile(path)
    return {
        "row_count": parquet.metadata.num_rows,
        "locations": [path],
        "year_month": year_month,
        "collected_at": "2026-08-11T08:53:54.000000Z",
        "file_size_bytes": Path(path).stat().st_size,
        "source_changed": True,
    }


def bronze_params(base_dir) -> dict:
    return {"base_dir": str(base_dir)}


def test_Validation_Task에_Slack_실패_콜백이_연결된다():
    for task_id in ("validate_bronze", "validate_silver"):
        validation_task = DAG.get_task(task_id)
        assert task_module.slack_failure_callback in validation_task.on_failure_callback


def test_정상_적재는_통과한다(tmp_path):
    path = write_bronze(tmp_path)
    validate_bronze(result_for(path), params=bronze_params(tmp_path))


def test_동일한_Bronze도_검증한_뒤_Silver후속처리를_중단한다(tmp_path):
    path = write_bronze(tmp_path)
    result = result_for(path)
    result["source_changed"] = False

    assert validate_bronze(result, params=bronze_params(tmp_path)) is False


def test_Bronze_변경여부_신호가_없으면_조용히_skip하지않고_실패한다(tmp_path):
    path = write_bronze(tmp_path)
    result = result_for(path)
    result.pop("source_changed")

    with pytest.raises(ValueError, match="source_changed"):
        validate_bronze(result, params=bronze_params(tmp_path))


def test_필수컬럼보다_컬럼이_많아도_통과한다(tmp_path):
    """원천이 MONTHLY_TAXI_TRIP_SCHEMA 보다 컬럼이 많아도(TLC 원본처럼) 막지 않습니다.

    물리 스키마 전체 일치는 더 이상 보지 않습니다(#529) — 필수 컬럼만 있으면 통과합니다.
    """
    extra_schema = pa.schema(
        [*task_module.SCHEMA, pa.field("source_trace_id", pa.string())]
    )
    path = write_bronze(tmp_path, schema=extra_schema)

    validate_bronze(result_for(path), params=bronze_params(tmp_path))


def test_파일이_없으면_막는다(tmp_path):
    missing = tmp_path / "hvfhv" / f"year_month={YEAR_MONTH}" / "missing.parquet"
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


def test_필수컬럼이_전부빠지면_재수집후에도_막는다(tmp_path, monkeypatch):
    broken_schema = pa.schema([("hvfhs_license_num", pa.string())])
    path = write_bronze(tmp_path, schema=broken_schema)
    result = result_for(path)
    monkeypatch.setattr(task_module, "_collect_bronze", lambda params: result)
    with pytest.raises(
        ValueError,
        match=r"expect_column_values_to_be_in_set\[missing_required_columns\]",
    ):
        validate_bronze(result, params=bronze_params(tmp_path))


def test_Spark_필수_컬럼이_재수집후에도_없으면_GX가_실패한다(
    tmp_path, monkeypatch
):
    missing = "pickup_datetime"
    schema = pa.schema(
        field for field in task_module.SCHEMA if field.name != missing
    )
    path = write_bronze(tmp_path, schema=schema)
    result = result_for(path)
    monkeypatch.setattr(task_module, "_collect_bronze", lambda params: result)

    with pytest.raises(
        ValueError,
        match=r"expect_column_values_to_be_in_set\[missing_required_columns\]",
    ):
        validate_bronze(result, params=bronze_params(tmp_path))


def test_Spark_필수_컬럼이_누락되면_원천부터_다시_수집한다(
    tmp_path, monkeypatch
):
    schema = pa.schema(
        field
        for field in task_module.SCHEMA
        if field.name != "pickup_datetime"
    )
    path = write_bronze(tmp_path, schema=schema)
    calls = []
    refreshed_results = []

    def recollect(params):
        calls.append(params)
        corrected_path = write_bronze(tmp_path)
        refreshed_results.append(result_for(corrected_path))
        return refreshed_results[-1]

    monkeypatch.setattr(task_module, "_collect_bronze", recollect)

    refreshed = validate_bronze(
        result_for(path), params=bronze_params(tmp_path)
    )

    assert len(calls) == 1
    # 재수집 결과를 그대로 넘깁니다. `silver_partitions_before` 는 #165 감시용으로
    # validate_bronze 가 덧붙이는 값이라 비교에서 뺍니다 (#532).
    assert {k: v for k, v in refreshed.items() if k != "silver_partitions_before"} == (
        refreshed_results[0]
    )
    assert "silver_partitions_before" in refreshed


def test_행_수가_0이면_막는다(tmp_path):
    path = write_bronze(tmp_path, rows=0)
    result = result_for(path)
    result["row_count"] = 1
    with pytest.raises(ValueError, match="행 수가 수집 결과와 다릅니다"):
        validate_bronze(result, params=bronze_params(tmp_path))


def test_필수값_NULL_행이_5퍼센트_미만이면_통과한다(tmp_path):
    records = bronze_rows(100)
    for row in records[:4]:
        row["pickup_datetime"] = None
    path = write_bronze(tmp_path, records=records)

    validate_bronze(result_for(path), params=bronze_params(tmp_path))


def test_필수값_NULL_행이_정확히_5퍼센트면_GX가_실패한다(
    tmp_path, caplog
):
    records = bronze_rows(20)
    records[0]["pickup_datetime"] = None
    path = write_bronze(tmp_path, records=records)

    with caplog.at_level("ERROR"), pytest.raises(
        ValueError,
        match=r"expect_column_values_to_be_between\[invalid_required_row_ratio\]",
    ):
        validate_bronze(result_for(path), params=bronze_params(tmp_path))

    assert "gx_validation failed layer=bronze" in caplog.text
    assert "column=invalid_required_row_ratio" in caplog.text
    assert "observed_value=[0.05]" in caplog.text


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


@pytest.mark.parametrize("column", SILVER_NULLABLE_COLUMNS)
def test_silver_필수값이_아닌_컬럼은_전부_NULL이어도_통과한다(
    tmp_path, monkeypatch, column
):
    """원천이 `on_scene_datetime` 을 채우지 않는 달이 있습니다(#582). 스키마에는
    남아 있으므로 컬럼별 NULL 검사만 빠지고, 적재는 그대로 통과해야 합니다."""
    monkeypatch.setattr(task_module, "DEFAULT_SILVER_DIR", str(tmp_path / "silver"))
    bronze_path = write_bronze(tmp_path / "bronze", rows=10)
    records = silver_rows(5)
    for record in records:
        record[column] = None
    write_silver(tmp_path / "silver", records=records)

    validate_silver(result_for(bronze_path))


def test_silver_필수값_목록에_on_scene_datetime이_없다():
    """계약이 되돌아가면(필수값에 다시 들어가면) 원천 릴리스가 100% 불합격합니다."""
    assert "on_scene_datetime" in SILVER_SCHEMA.names
    assert "on_scene_datetime" not in task_module.SILVER_REQUIRED_NON_NULL


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


def test_쓰기_전에_있던_파티션이_사라지면_165_재발로_막는다(tmp_path, monkeypatch):
    """정적 overwrite(#165)가 재발하면 이번에 쓴 달만 남고 나머지가 지워집니다."""
    monkeypatch.setattr(task_module, "DEFAULT_SILVER_DIR", str(tmp_path / "silver"))
    bronze_path = write_bronze(tmp_path / "bronze", rows=10)
    write_silver(tmp_path / "silver", year_month=YEAR_MONTH, rows=5)
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
    write_silver(tmp_path / "silver", year_month="2026-06", rows=5)
    write_silver(tmp_path / "silver", year_month=YEAR_MONTH, rows=5)
    result = result_for(bronze_path)
    result["silver_partitions_before"] = ["year_month=2026-06"]

    validate_silver(result)


def test_쓰기_전_스냅샷이_없어도_통과한다(tmp_path, monkeypatch):
    """첫 실행이나 예전 XCom 에는 이 키가 없습니다. 없다고 막으면 안 됩니다."""
    monkeypatch.setattr(task_module, "DEFAULT_SILVER_DIR", str(tmp_path / "silver"))
    bronze_path = write_bronze(tmp_path / "bronze", rows=10)
    write_silver(tmp_path / "silver", year_month=YEAR_MONTH, rows=5)

    validate_silver(result_for(bronze_path))


def test_쓰기_전_파티션_목록은_parquet_이_있는_것만_센다(tmp_path):
    """빈 디렉터리가 남아 있는 경우가 있습니다. 그걸 세면 "사라졌다" 오탐이 납니다."""
    silver = tmp_path / "silver"
    write_silver(silver, year_month="2026-06", rows=5)
    (silver / "year_month=2026-07").mkdir(parents=True)

    assert task_module.existing_silver_partitions(str(silver)) == ["year_month=2026-06"]


def test_Silver_디렉터리가_없으면_빈_목록이다(tmp_path):
    assert task_module.existing_silver_partitions(str(tmp_path / "none")) == []



def test_Bronze_GX_실패는_재시도없이_Spark와_Silver를_실행하지_않는다(
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
    assert instances["validate_bronze"].try_number == 1
    assert instances["bronze_to_silver"].state == "upstream_failed"
    assert instances["validate_silver"].state == "upstream_failed"
    assert callbacks == ["validate_bronze"]
    assert "gx_validation failed layer=bronze" in caplog.text
    assert "column=invalid_required_row_ratio" in caplog.text
    assert "observed_value=[0.2]" in caplog.text
