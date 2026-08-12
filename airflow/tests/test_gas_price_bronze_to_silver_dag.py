"""Gas Price Bronze -> Silver DAG 경계와 GX Silver Suite를 확인합니다.

1. 정기 실행은 직전 완료 월을, 수동 파라미터가 있으면 지정 월을 처리한다.
2. DAG는 월별 Silver 적재 후 검증을 실행한다.
3. layout 경로의 정상 Parquet은 통과한다.
4. Handler 응답·경로·파일 경계와 GX 행·값·날짜 규칙을 검증한다.
5. 정확한 Arrow 물리 스키마가 다르면 거부한다.
"""

import importlib
from datetime import date, datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from dags import gas_price_bronze_to_silver_dag as dag_module

# DAG 모듈이 저장소 루트를 sys.path에 추가한 뒤 불러옵니다.
# `lambda`는 예약어라 일반 import 문을 사용할 수 없습니다.
layout = importlib.import_module("lambda.functions.common.gas_price_layout")
loader = importlib.import_module(
    "lambda.functions.gas_price_bronze_to_silver.loader"
)

DAG = dag_module.gas_price_bronze_to_silver_dag
run_bronze_to_silver = DAG.get_task("bronze_to_silver").python_callable
validate_silver = DAG.get_task("validate_silver").python_callable

COLLECTED_MONTH = "2026-07"
COLLECTED_AT = datetime(2026, 7, 2, 9, 0, tzinfo=timezone.utc)


def row(price_date: date = date(2026, 7, 1), **overrides) -> dict:
    values = {
        "state": "NY",
        "fuel_type": "regular",
        "price_usd_per_gallon": 3.159,
        "price_date": price_date,
        "source_url": "https://gasprices.aaa.com/?state=NY",
        "collected_at": COLLECTED_AT,
        "bronze_path": "data/bronze/gas_price/collected_date=2026-07-02/gas_price.json",
    }
    values.update(overrides)
    return values


@pytest.fixture
def silver_dir(tmp_path, monkeypatch):
    """검증 Task가 보는 Silver 루트를 임시 디렉터리로 바꿉니다."""
    monkeypatch.setattr(dag_module, "SILVER_DIR", str(tmp_path))
    return str(tmp_path)


def write_silver(silver_dir: str, rows: list[dict], schema: pa.Schema | None = None):
    path = layout.silver_file(silver_dir, COLLECTED_MONTH)
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(rows, schema=schema or loader.SCHEMA)
    pq.write_table(table, path)
    return path


def result_of(path, **overrides) -> dict:
    result = {
        "row_count": 1,
        "locations": [str(path)],
        "collected_month": COLLECTED_MONTH,
    }
    result.update(overrides)
    return result


# --- DAG 구조와 대상 월 --------------------------------------------------------


def test_dag_id와_task_구성이_정확하다():
    assert DAG.dag_id == "gas_price_bronze_to_silver_pipeline"
    assert {task.task_id for task in DAG.tasks} == {
        "bronze_to_silver",
        "validate_silver",
    }


def test_silver_검증은_적재_뒤에_실행된다():
    assert DAG.get_task("bronze_to_silver").downstream_task_ids == {
        "validate_silver"
    }


def test_1월의_직전_완료_월은_전년도_12월이다():
    interval_end = datetime(2026, 1, 1, tzinfo=timezone.utc)

    assert dag_module.previous_month(interval_end) == "2025-12"


def test_수동_수집월은_data_interval_end보다_우선한다(monkeypatch):
    captured_event = {}

    def fake_handler(event, context=None):
        captured_event.update(event)
        return {
            "row_count": 1,
            "locations": ["unused.parquet"],
            "collected_month": event["collected_month"],
        }

    monkeypatch.setattr(dag_module, "lambda_handler_for", lambda _: fake_handler)

    result = run_bronze_to_silver(
        params={"collected_month": "2026-05"},
        data_interval_end=datetime(2030, 1, 1, tzinfo=timezone.utc),
    )

    assert captured_event["collected_month"] == "2026-05"
    assert result["collected_month"] == "2026-05"


# --- 정상 케이스 ---------------------------------------------------------------


def test_정상_silver_parquet은_검증을_통과한다(silver_dir):
    path = write_silver(
        silver_dir,
        [row(), row(date(2026, 7, 2))],
    )

    validate_silver(result_of(path, row_count=2))


def test_전월_price_date도_수집월이_맞으면_통과한다(silver_dir):
    path = write_silver(
        silver_dir,
        [row(date(2026, 6, 30), collected_at=COLLECTED_AT)],
    )

    validate_silver(result_of(path))


# --- Handler 응답이 잘못된 경우 -------------------------------------------------


def test_handler_결과가_dict가_아니면_거부한다():
    with pytest.raises(TypeError, match="dict"):
        validate_silver(["not", "a", "dict"])


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param({"row_count": "1"}, id="row_count가 문자열"),
        pytest.param({"row_count": True}, id="row_count가 bool"),
        pytest.param({"row_count": 0}, id="row_count가 0"),
        pytest.param({"locations": []}, id="locations가 비어 있음"),
        pytest.param({"locations": ["a", "b"]}, id="locations가 2개"),
        pytest.param({"collected_month": 202601}, id="collected_month가 정수"),
        pytest.param({"collected_month": "2026-1"}, id="월 0패딩 누락"),
        pytest.param({"collected_month": "2026-13"}, id="13월"),
    ],
)
def test_handler_응답_계약이_깨지면_거부한다(silver_dir, overrides):
    path = write_silver(silver_dir, [row()])

    with pytest.raises(ValueError):
        validate_silver(result_of(path, **overrides))


# --- 적재된 파일이 잘못된 경우 --------------------------------------------------


def test_layout이_정한_경로가_아니면_거부한다(silver_dir, tmp_path):
    path = write_silver(str(tmp_path / "elsewhere"), [row()])

    with pytest.raises(ValueError, match="적재 경로"):
        validate_silver(result_of(path))


def test_적재_파일이_없으면_거부한다(silver_dir):
    path = layout.silver_file(silver_dir, COLLECTED_MONTH)

    with pytest.raises(FileNotFoundError, match="적재 파일"):
        validate_silver(result_of(path))


def test_parquet이_아닌_파일이면_거부한다(silver_dir):
    path = layout.silver_file(silver_dir, COLLECTED_MONTH)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("not parquet", encoding="utf-8")

    with pytest.raises(RuntimeError, match="Parquet"):
        validate_silver(result_of(path))


def test_실제_행_수와_row_count가_다르면_거부한다(silver_dir):
    path = write_silver(silver_dir, [row()])

    with pytest.raises(ValueError, match="expect_table_row_count_to_equal"):
        validate_silver(result_of(path, row_count=2))


def test_loader_schema와_다르면_거부한다(silver_dir):
    price_index = loader.SCHEMA.get_field_index("price_usd_per_gallon")
    broken_schema = loader.SCHEMA.set(
        price_index,
        pa.field("price_usd_per_gallon", pa.string()),
    )
    path = write_silver(
        silver_dir,
        [{**row(), "price_usd_per_gallon": "3.159"}],
        schema=broken_schema,
    )

    with pytest.raises(ValueError, match="expect_column_values_to_be_of_type"):
        validate_silver(result_of(path))


def test_silver_필수_컬럼이_누락되면_gx가_거부한다(silver_dir, caplog):
    fields = [field for field in loader.SCHEMA if field.name != "source_url"]
    broken_schema = pa.schema(fields)
    broken_row = row()
    broken_row.pop("source_url")
    path = write_silver(silver_dir, [broken_row], schema=broken_schema)

    with pytest.raises(
        ValueError,
        match="expect_table_columns_to_match_ordered_list",
    ):
        validate_silver(result_of(path))

    assert "gx_validation failed layer=silver" in caplog.text


def test_arrow_timestamp_단위가_loader_schema와_다르면_거부한다(silver_dir):
    timestamp_index = loader.SCHEMA.get_field_index("collected_at")
    broken_schema = loader.SCHEMA.set(
        timestamp_index,
        pa.field("collected_at", pa.timestamp("ms", tz="UTC")),
    )
    path = write_silver(silver_dir, [row()], schema=broken_schema)

    with pytest.raises(ValueError, match="스키마"):
        validate_silver(result_of(path))


def test_gx_silver_가격일은_중복될_수_없다(silver_dir, caplog):
    path = write_silver(silver_dir, [row(), row()])

    with pytest.raises(
        ValueError,
        match=r"expect_column_values_to_be_unique\[price_date\]",
    ):
        validate_silver(result_of(path, row_count=2))

    assert "expectation=expect_column_values_to_be_unique" in caplog.text


@pytest.mark.parametrize(
    ("rows", "failed_rule", "column"),
    [
        pytest.param(
            [row(state="NJ")],
            "expect_column_values_to_be_in_set",
            "state",
            id="뉴욕주가 아님",
        ),
        pytest.param(
            [row(fuel_type="premium")],
            "expect_column_values_to_be_in_set",
            "fuel_type",
            id="regular가 아님",
        ),
        pytest.param(
            [row(source_url=None)],
            "expect_column_values_to_not_be_null",
            "source_url",
            id="필수값 NULL",
        ),
        pytest.param(
            [row(price_usd_per_gallon=0.0)],
            "expect_column_values_to_be_between",
            "price_usd_per_gallon",
            id="가격 0",
        ),
        pytest.param(
            [row(price_usd_per_gallon=-1.0)],
            "expect_column_values_to_be_between",
            "price_usd_per_gallon",
            id="가격 음수",
        ),
        pytest.param(
            [row(price_usd_per_gallon=float("nan"))],
            "expect_column_values_to_not_be_null",
            "price_usd_per_gallon",
            id="가격 NaN",
        ),
        pytest.param(
            [row(price_usd_per_gallon=float("inf"))],
            "expect_column_values_to_be_in_set",
            "price_is_finite",
            id="가격 Infinity",
        ),
        pytest.param(
            [row(collected_at=datetime(2026, 8, 1, tzinfo=timezone.utc))],
            "expect_column_values_to_be_in_set",
            "collected_month_utc",
            id="대상 수집월 불일치",
        ),
        pytest.param(
            [row(date(2026, 7, 3))],
            "expect_column_pair_values_a_to_be_greater_than_b",
            "collected_date_utc/price_date",
            id="가격일이 수집일보다 미래",
        ),
        pytest.param(
            [row(source_url="   ")],
            "expect_column_values_to_match_regex",
            "source_url",
            id="출처 URL 공백",
        ),
        pytest.param(
            [row(bronze_path="")],
            "expect_column_values_to_match_regex",
            "bronze_path",
            id="Bronze 경로 공백",
        ),
    ],
)
def test_gx_silver_규칙_위반을_거부하고_로그에_남긴다(
    silver_dir, rows, failed_rule, column, caplog
):
    path = write_silver(silver_dir, rows)

    with pytest.raises(ValueError, match=rf"{failed_rule}\[{column}\]"):
        validate_silver(result_of(path, row_count=len(rows)))

    assert f"expectation={failed_rule}" in caplog.text
