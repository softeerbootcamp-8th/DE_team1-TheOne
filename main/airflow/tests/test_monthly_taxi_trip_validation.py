"""월별 택시 운행 DAG의 경계 검사와 GX 데이터 품질 규칙을 검증합니다.

Bronze 는 제공된 Parquet 원본의 경로·크기·footer 행 수를 검증합니다.
Silver 는 Spark BashOperator 라 handler 결과 dict 자체가 없어 파티션을
직접 열어서 봐야 합니다.
검증 태스크의 값어치는 "통과한다"가 아니라 "불량을 통과시키지 않는다"입니다.

대용량 원본을 Pandas 에 모두 올리지 않도록 Parquet 을 배치 단위로 검사합니다.
실제 Parquet 을 tmp_path 에 쓰며 네트워크와 Spark 는 사용하지 않습니다.
Silver timestamp는 unit 차이는 허용하되 timezone identity는 유지합니다.
S3 Bronze 위치는 로컬 Path로 변환하지 않고 객체 바이트로 검증합니다.
S3 Silver 버전은 Bronze와 같은 버킷의 monthly_taxi_trip prefix로 계산합니다.
Silver는 검증 성공 시 `_SUCCESS`, 실패 시 `_QUARANTINED.json`으로 전환됩니다.
격리 버전은 데이터 파일을 보존하되 후속 reader가 읽지 않습니다(#945).
"""

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

DAG = dag_module.monthly_taxi_trip_dag
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
    schema = task_module.SCHEMA if schema is None else schema
    records = bronze_rows(rows, schema) if records is None else records
    dataset_root = Path(base_dir) / "monthly_taxi_trip"
    dataset_root /= f"service_area={service_area}"
    path = dataset_root / f"year_month={year_month}" / "20260811T085354000000Z.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(records, schema=schema), path)
    return str(path)


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
        pa.Table.from_pylist(bronze_rows(rows, task_module.SCHEMA), schema=task_module.SCHEMA),
        path,
    )
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


def test_Validation_Task에_Slack_실패_콜백이_연결된다():
    for task_id in ("validate_bronze", "validate_silver"):
        validation_task = DAG.get_task(task_id)
        assert task_module.slack_failure_callback in validation_task.on_failure_callback


def test_정상_적재는_통과한다(tmp_path):
    path = write_bronze(tmp_path)
    validate_bronze(result_for(path), params=bronze_params(tmp_path))


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
    s3_path = (
        "s3://de-theone/bronze/monthly_taxi_trip/service_area=NYC/"
        "year_month=2026-07/"
        "20260811T085354000000Z.parquet"
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

    summary = task_module._bronze_quality_result(
        result,
        {"base_dir": "s3://de-theone/bronze", "service_area": "NYC"},
        list(task_module.SCHEMA.names),
    )

    assert summary.at[0, "row_count"] == 3


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
    ].endswith(f"source_collected_at={Path(path).stem}")


def test_Bronze_변경여부_신호가_없어도_감시DAG호출이면_처리한다(tmp_path):
    path = write_bronze(tmp_path)
    result = result_for(path)
    result.pop("source_changed")

    assert validate_bronze(result, params=bronze_params(tmp_path))[
        "silver_version_path"
    ].endswith(f"source_collected_at={Path(path).stem}")


def test_필수컬럼보다_컬럼이_많으면_경고후_통과한다(tmp_path, monkeypatch):
    """원천이 MONTHLY_TAXI_TRIP_SCHEMA 보다 컬럼이 많아도(TLC 원본처럼) 막지 않습니다.

    물리 스키마 전체 일치는 더 이상 보지 않습니다(#529) — 필수 컬럼만 있으면 통과합니다.
    """
    extra_schema = pa.schema(
        [*task_module.SCHEMA, pa.field("source_trace_id", pa.string())]
    )
    path = write_bronze(tmp_path, schema=extra_schema)

    warnings = []
    monkeypatch.setattr(
        task_module,
        "send_quality_warning",
        lambda context, **values: warnings.append(values),
    )

    validate_bronze(result_for(path), params=bronze_params(tmp_path))

    assert warnings == [
        {
            "dataset": "monthly_taxi_trip",
            "year_month": YEAR_MONTH,
            "invalid_rows": 0,
            "row_count": 3,
            "invalid_ratio": 0.0,
            "extra_columns": ["source_trace_id"],
        }
    ]


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
    generated = {
        "silver_version_path",
        "silver_partitions_before",
    }
    assert {k: v for k, v in refreshed.items() if k not in generated} == {
        k: v for k, v in refreshed_results[0].items() if k not in generated
    }
    assert "silver_partitions_before" in refreshed


def test_행_수가_0이면_막는다(tmp_path):
    path = write_bronze(tmp_path, rows=0)
    result = result_for(path)
    result["row_count"] = 1
    with pytest.raises(ValueError, match="행 수가 수집 결과와 다릅니다"):
        validate_bronze(result, params=bronze_params(tmp_path))


def test_필수값_불량률이_1퍼센트_미만이면_경고없이_통과한다(
    tmp_path, monkeypatch
):
    records = bronze_rows(200)
    records[0]["pickup_datetime"] = None
    path = write_bronze(tmp_path, records=records)
    warnings = []
    monkeypatch.setattr(
        task_module,
        "send_quality_warning",
        lambda context, **values: warnings.append(values),
    )

    validate_bronze(result_for(path), params=bronze_params(tmp_path))

    assert warnings == []


def test_한레코드의_복수위반은_한건으로_세고_1퍼센트부터_경고한다(
    tmp_path, monkeypatch
):
    records = bronze_rows(100)
    records[0]["pickup_datetime"] = None
    records[0]["trip_miles"] = -1
    path = write_bronze(tmp_path, records=records)
    warnings = []
    monkeypatch.setattr(
        task_module,
        "send_quality_warning",
        lambda context, **values: warnings.append(values),
    )

    validate_bronze(result_for(path), params=bronze_params(tmp_path))

    assert warnings[0]["invalid_rows"] == 1
    assert warnings[0]["invalid_ratio"] == 0.01


def test_Spark서비스등급규칙_위반도_레코드불량으로_집계한다(
    tmp_path, monkeypatch
):
    records = bronze_rows(100)
    records[0]["estimated_service_tier"] = "Unknown"
    path = write_bronze(tmp_path, records=records)
    warnings = []
    monkeypatch.setattr(
        task_module,
        "send_quality_warning",
        lambda context, **values: warnings.append(values),
    )

    validate_bronze(result_for(path), params=bronze_params(tmp_path))

    assert warnings[0]["invalid_rows"] == 1


def test_필수값_NULL_행이_정확히_5퍼센트면_GX가_실패한다(
    tmp_path, caplog, monkeypatch
):
    records = bronze_rows(20)
    records[0]["pickup_datetime"] = None
    path = write_bronze(tmp_path, records=records)
    warnings = []
    monkeypatch.setattr(
        task_module,
        "send_quality_warning",
        lambda context, **values: warnings.append(values),
    )

    with caplog.at_level("ERROR"), pytest.raises(
        ValueError,
        match=r"expect_column_values_to_be_between\[invalid_required_row_ratio\]",
    ):
        validate_bronze(result_for(path), params=bronze_params(tmp_path))

    assert "gx_validation failed layer=bronze" in caplog.text
    assert "column=invalid_required_row_ratio" in caplog.text
    assert "observed_value=[0.05]" in caplog.text
    assert warnings == []


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
    service_area: str = "NYC",
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
    (version / "_SUCCESS").touch()
    return partition


def test_정상_silver_적재는_통과한다(tmp_path, monkeypatch):
    monkeypatch.setattr(task_module, "DEFAULT_SILVER_DIR", str(tmp_path / "silver"))
    bronze_path = write_bronze(tmp_path / "bronze", rows=10)
    write_silver(tmp_path / "silver", rows=5)

    result = result_for(bronze_path)
    quarantine = Path(result["silver_version_path"]) / "_QUARANTINED.json"
    quarantine.write_text("{}")
    validate_silver(result)

    assert Path(result["silver_version_path"]).is_dir()
    assert (Path(result["silver_version_path"]) / "_SUCCESS").is_file()
    assert not quarantine.exists()


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
    write_committed_silver(tmp_path / "silver", year_month="2026-06", rows=5)
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


def test_같은_월_재처리중_SUCCESS가_없어도_다시_공개한다(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(task_module, "DEFAULT_SILVER_DIR", str(tmp_path / "silver"))
    bronze_path = write_bronze(tmp_path / "bronze", rows=10)
    write_silver(tmp_path / "silver", year_month=YEAR_MONTH, rows=5)
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



def test_Bronze_GX_실패는_재시도없이_Spark와_Silver를_실행하지_않는다(
    tmp_path, monkeypatch, caplog
):
    records = bronze_rows(10)
    records[0]["pickup_datetime"] = None
    records[1]["dropoff_datetime"] = None
    path = write_bronze(tmp_path, records=records, service_area="NYC")
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
            "service_area": "NYC",
        },
    )
    instances = {instance.task_id: instance for instance in run.get_task_instances()}

    assert run.state == "failed"
    assert instances["raw_to_bronze"].state == "success"
    assert instances["validate_bronze"].state == "failed"
    assert instances["validate_bronze"].try_number == 1
    assert instances["bronze_to_silver"].state == "upstream_failed"
    assert instances["validate_silver"].state == "upstream_failed"
    quarantine = Path(path).parent / "_QUARANTINED.json"
    payload = json.loads(quarantine.read_text())
    assert payload["layer"] == "bronze"
    assert payload["retryable"] is False
    assert payload["run_id"] == run.run_id
    assert callbacks == ["validate_bronze"]
    assert "gx_validation failed layer=bronze" in caplog.text
    assert "column=invalid_required_row_ratio" in caplog.text
    assert "observed_value=[0.2]" in caplog.text
