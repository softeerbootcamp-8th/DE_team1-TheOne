"""EV Charging Bronze -> Silver DAG 의 구조와 validate_silver 검증 분기를 확인합니다.

CI 의 `check_dags.py` 는 DAG 가 import 되는지만 봅니다. 여기서는 그 다음,
`validate_silver` 가 잘못된 Silver Parquet 을 실제로 걸러내는지를 봅니다.
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
COLLECTED_AT = datetime(2026, 8, 1, 10, 0, tzinfo=timezone.utc)


def row(price_date: str = "2026-07-01") -> dict:
    year, month, day = (int(part) for part in price_date.split("-"))
    return {
        "city": "New York",
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
        "collected_at": COLLECTED_AT,
        "bronze_path": "s3://bronze/ev_charging_stations",
    }


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

    with pytest.raises(ValueError, match="행 수"):
        validate_silver(result_of(path, row_count=2))


def test_schema_must_match_loader(silver_dir):
    # 컬럼 하나의 타입만 어긋나도 걸러야 합니다 (float64 -> string).
    broken = loader.SCHEMA.set(
        loader.SCHEMA.get_field_index("average_price_usd_per_kwh"),
        pa.field("average_price_usd_per_kwh", pa.string()),
    )
    path = write_silver(silver_dir, [{**row(), "average_price_usd_per_kwh": "0.31"}], broken)

    with pytest.raises(ValueError, match="스키마"):
        validate_silver(result_of(path))
