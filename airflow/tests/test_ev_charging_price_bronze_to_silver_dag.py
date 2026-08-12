"""EV Charging Bronze -> Silver DAG 경계와 GX Silver Suite를 확인합니다.

CI 의 `check_dags.py` 는 DAG 가 import 되는지만 봅니다. 여기서는 그 다음,
`validate_silver` 가 경계 오류와 데이터 품질 오류를 실제로 걸러내는지를 봅니다.
"""

import importlib
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from airflow.exceptions import ParamValidationError

from dags import ev_charging_price_bronze_to_silver_dag as dag_module

# DAG 모듈이 import 될 때 저장소 루트를 sys.path 에 넣습니다. 그래서 이 import 들은
# 위 import 보다 뒤에 있어야 합니다 (`lambda` 는 예약어라 import 문을 못 씁니다).
layout = importlib.import_module("lambda.functions.common.ev_charging_layout")
loader = importlib.import_module(
    "lambda.functions.ev_charging_stations_bronze_to_silver.loader"
)

DAG = dag_module.ev_charging_price_bronze_to_silver_dag
validate_silver = DAG.get_task("validate_silver").python_callable

COLLECTED_MONTH = "2026-07"
def row(price_date: str = "2026-07-01", **overrides) -> dict:
    year, month, day = (int(part) for part in price_date.split("-"))
    values = {
        "city": "New York City",
        "state": "NY",
        "fuel_type_code": "ELEC",
        "average_price_usd_per_kwh": 0.31,
        "price_date": datetime(year, month, day).date(),
        "currency": "USD",
        "price_unit": "kWh",
        "nyc_station_count": 10,
        "normalized_price_count": 8,
        "free_station_count": 1,
        "missing_price_count": 1,
        "unsupported_price_count": 0,
        "source_url": "https://developer.nlr.gov/",
        "collected_at": datetime(year, month, day, 10, 0, tzinfo=timezone.utc),
        "bronze_path": "s3://bronze/ev_charging_stations",
    }
    values.update(overrides)
    return values


@pytest.fixture
def silver_dir(tmp_path, monkeypatch):
    """검증 태스크가 보는 Silver 루트를 임시 디렉터리로 돌립니다."""
    monkeypatch.setattr(dag_module, "SILVER_DIR", str(tmp_path))
    return str(tmp_path)


def write_silver(silver_dir: str, rows: list[dict], schema: pa.Schema = None):
    path = layout.silver_file(silver_dir, COLLECTED_MONTH)
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows, schema=schema or loader.SCHEMA), path)
    return path


def result_of(path, **overrides) -> dict:
    result = {
        "row_count": 1,
        "locations": [str(path)],
        "collected_month": COLLECTED_MONTH,
    }
    result.update(overrides)
    return result


# --- DAG 구조 -----------------------------------------------------------------


def test_dag_id_and_tasks():
    assert DAG.dag_id == "ev_charging_price_bronze_to_silver_pipeline"
    assert {task.task_id for task in DAG.tasks} == {"bronze_to_silver", "validate_silver"}


def test_validate_runs_after_load():
    assert DAG.get_task("bronze_to_silver").downstream_task_ids == {"validate_silver"}


@pytest.mark.parametrize("month", ["2026-01", "2026-12"])
def test_collected_month_param_accepts_valid_month(month):
    assert DAG.params.get_param("collected_month").resolve(month) == month


@pytest.mark.parametrize(
    "month",
    [
        pytest.param("2026-13", id="13월"),
        pytest.param("2026-00", id="0월"),
        pytest.param("2026-8", id="0패딩 누락"),
        pytest.param("2026-07-01", id="일까지 들어옴"),
    ],
)
def test_collected_month_param_rejects_invalid_month(month):
    with pytest.raises(ParamValidationError):
        DAG.params.get_param("collected_month").resolve(month)


# --- 대상 월 계산 --------------------------------------------------------------


@pytest.mark.parametrize(
    ("interval_end", "expected"),
    [
        pytest.param(datetime(2026, 8, 1), "2026-07", id="월초 실행"),
        pytest.param(datetime(2026, 8, 15), "2026-07", id="월중 수동 실행"),
        pytest.param(datetime(2026, 1, 1), "2025-12", id="연초 실행은 직전 연도 12월"),
        pytest.param(datetime(2026, 3, 1), "2026-02", id="윤년 아닌 2월"),
    ],
)
def test_previous_month(interval_end, expected):
    assert dag_module.previous_month(interval_end) == expected


# --- 정상 케이스 ---------------------------------------------------------------


def test_valid_silver_passes(silver_dir):
    path = write_silver(silver_dir, [row(), row("2026-07-02")])

    validate_silver(result_of(path, row_count=2))


# --- Handler 응답이 잘못된 경우 -------------------------------------------------


def test_result_must_be_dict():
    with pytest.raises(TypeError):
        validate_silver(["not", "a", "dict"])


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param({"row_count": "1"}, id="row_count가 문자열"),
        pytest.param({"row_count": True}, id="row_count가 bool"),
        pytest.param({"row_count": 0}, id="row_count가 0"),
        pytest.param({"locations": []}, id="locations가 비어 있음"),
        pytest.param({"locations": ["a", "b"]}, id="locations가 2개"),
        pytest.param({"locations": [""]}, id="locations가 빈 문자열"),
        pytest.param({"collected_month": 202607}, id="collected_month가 문자열이 아님"),
        pytest.param({"collected_month": "2026/07"}, id="collected_month 구분자 오류"),
        pytest.param({"collected_month": "2026-7"}, id="collected_month 0패딩 누락"),
    ],
)
def test_invalid_result_is_rejected(silver_dir, overrides):
    path = write_silver(silver_dir, [row()])

    with pytest.raises(ValueError):
        validate_silver(result_of(path, **overrides))


# --- 적재된 파일이 잘못된 경우 --------------------------------------------------


def test_file_outside_layout_is_rejected(silver_dir, tmp_path):
    path = write_silver(str(tmp_path / "elsewhere"), [row()])

    with pytest.raises(ValueError, match="적재 경로"):
        validate_silver(result_of(path))


def test_missing_file_is_rejected(silver_dir):
    path = layout.silver_file(silver_dir, COLLECTED_MONTH)

    with pytest.raises(FileNotFoundError):
        validate_silver(result_of(path))


def test_non_parquet_file_is_rejected(silver_dir):
    path = layout.silver_file(silver_dir, COLLECTED_MONTH)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("parquet 이 아닌 파일")

    with pytest.raises(RuntimeError):
        validate_silver(result_of(path))


def test_row_count_must_match_file(silver_dir):
    path = write_silver(silver_dir, [row()])

    with pytest.raises(ValueError, match="expect_table_row_count_to_equal"):
        validate_silver(result_of(path, row_count=2))


def test_schema_must_match_loader(silver_dir):
    # 컬럼 하나의 타입만 어긋나도 걸러야 합니다 (float64 -> string).
    broken = loader.SCHEMA.set(
        loader.SCHEMA.get_field_index("average_price_usd_per_kwh"),
        pa.field("average_price_usd_per_kwh", pa.string()),
    )
    path = write_silver(silver_dir, [{**row(), "average_price_usd_per_kwh": "0.31"}], broken)

    with pytest.raises(ValueError, match="expect_column_values_to_be_of_type"):
        validate_silver(result_of(path))


def test_missing_count_column_is_reported_as_gx_failure(silver_dir, caplog):
    fields = [
        field
        for field in loader.SCHEMA
        if field.name != "normalized_price_count"
    ]
    broken = pa.schema(fields)
    broken_row = row()
    broken_row.pop("normalized_price_count")
    path = write_silver(silver_dir, [broken_row], broken)

    with pytest.raises(
        ValueError,
        match="expect_table_columns_to_match_ordered_list",
    ):
        validate_silver(result_of(path))

    assert "gx_validation failed layer=silver" in caplog.text


def test_invalid_collected_at_type_is_reported_as_gx_failure(
    silver_dir, caplog
):
    broken = loader.SCHEMA.set(
        loader.SCHEMA.get_field_index("collected_at"),
        pa.field("collected_at", pa.string()),
    )
    path = write_silver(
        silver_dir,
        [row(collected_at="not-a-datetime")],
        broken,
    )

    with pytest.raises(
        ValueError,
        match=r"expect_column_values_to_be_of_type\[collected_at\]",
    ):
        validate_silver(result_of(path))

    assert "gx_validation failed layer=silver" in caplog.text


@pytest.mark.parametrize(
    ("column", "value"),
    [
        pytest.param("city", "Albany", id="city"),
        pytest.param("state", "CA", id="state"),
        pytest.param("fuel_type_code", "LPG", id="fuel_type_code"),
        pytest.param("currency", "KRW", id="currency"),
        pytest.param("price_unit", "hour", id="price_unit"),
    ],
)
def test_gx_silver_constant_value_is_enforced(silver_dir, column, value):
    path = write_silver(silver_dir, [row(**{column: value})])

    with pytest.raises(
        ValueError,
        match=rf"expect_column_values_to_be_in_set\[{column}\]",
    ):
        validate_silver(result_of(path))


@pytest.mark.parametrize(
    ("price", "failed_rule"),
    [
        pytest.param(-0.01, "expect_column_values_to_be_between", id="음수"),
        pytest.param(5.01, "expect_column_values_to_be_between", id="상한 초과"),
        pytest.param(float("inf"), "expect_column_values_to_be_between", id="무한대"),
        pytest.param(float("nan"), "expect_column_values_to_not_be_null", id="NaN"),
    ],
)
def test_gx_silver_invalid_price_is_rejected(
    silver_dir, price, failed_rule, caplog
):
    path = write_silver(
        silver_dir, [row(average_price_usd_per_kwh=price)]
    )

    with pytest.raises(ValueError, match=failed_rule):
        validate_silver(result_of(path))

    assert f"expectation={failed_rule}" in caplog.text


def test_gx_silver_zero_price_is_allowed(silver_dir):
    path = write_silver(
        silver_dir, [row(average_price_usd_per_kwh=0.0)]
    )

    validate_silver(result_of(path))


def test_gx_silver_required_value_cannot_be_null(silver_dir):
    path = write_silver(silver_dir, [row(source_url=None)])

    with pytest.raises(
        ValueError,
        match=r"expect_column_values_to_not_be_null\[source_url\]",
    ):
        validate_silver(result_of(path))


@pytest.mark.parametrize("column", ["source_url", "bronze_path"])
def test_gx_silver_lineage_value_cannot_be_blank(silver_dir, column):
    path = write_silver(silver_dir, [row(**{column: "   "})])

    with pytest.raises(
        ValueError,
        match=rf"expect_column_values_to_match_regex\[{column}\]",
    ):
        validate_silver(result_of(path))


def test_gx_silver_price_date_must_be_in_target_month(silver_dir):
    path = write_silver(silver_dir, [row("2026-08-01")])

    with pytest.raises(
        ValueError,
        match=r"expect_column_values_to_be_between\[price_date\]",
    ):
        validate_silver(result_of(path))


def test_gx_silver_price_date_must_be_unique(silver_dir):
    path = write_silver(silver_dir, [row(), row()])

    with pytest.raises(
        ValueError,
        match=r"expect_column_values_to_be_unique\[price_date\]",
    ):
        validate_silver(result_of(path, row_count=2))


def test_gx_silver_station_counts_must_add_up(silver_dir):
    path = write_silver(
        silver_dir,
        [row(nyc_station_count=10, normalized_price_count=1)],
    )

    with pytest.raises(
        ValueError,
        match=r"expect_column_pair_values_to_be_equal\[table\]",
    ):
        validate_silver(result_of(path))


def test_gx_silver_collected_at_date_must_match_price_date(silver_dir):
    path = write_silver(
        silver_dir,
        [row(collected_at=datetime(2026, 7, 2, tzinfo=timezone.utc))],
    )

    with pytest.raises(
        ValueError,
        match=r"expect_column_pair_values_to_be_equal\[table\]",
    ):
        validate_silver(result_of(path))


@pytest.mark.parametrize(
    ("column", "value"),
    [
        pytest.param("nyc_station_count", -1, id="전체 충전소 음수"),
        pytest.param("normalized_price_count", -1, id="정규화 요금 음수"),
        pytest.param("normalized_price_count", 0, id="정규화 요금 0건"),
        pytest.param("free_station_count", -1, id="무료 충전소 음수"),
        pytest.param("missing_price_count", -1, id="요금 누락 음수"),
        pytest.param("unsupported_price_count", -1, id="미지원 요금 음수"),
    ],
)
def test_gx_silver_count_rule_is_enforced(silver_dir, column, value):
    path = write_silver(silver_dir, [row(**{column: value})])

    with pytest.raises(
        ValueError,
        match=rf"expect_column_values_to_be_between\[{column}\]",
    ):
        validate_silver(result_of(path))


def test_gx_silver_columns_must_follow_loader_order(silver_dir):
    reordered = pa.schema(
        [
            loader.SCHEMA.field(1),
            loader.SCHEMA.field(0),
            *(loader.SCHEMA.field(index) for index in range(2, len(loader.SCHEMA))),
        ]
    )
    path = write_silver(silver_dir, [row()], reordered)

    with pytest.raises(ValueError, match="expect_table_columns_to_match_ordered_list"):
        validate_silver(result_of(path))


def test_arrow_schema_timestamp_unit_must_match_loader(silver_dir):
    broken = loader.SCHEMA.set(
        loader.SCHEMA.get_field_index("collected_at"),
        pa.field("collected_at", pa.timestamp("ms", tz="UTC")),
    )
    path = write_silver(silver_dir, [row()], broken)

    with pytest.raises(ValueError, match="스키마"):
        validate_silver(result_of(path))
