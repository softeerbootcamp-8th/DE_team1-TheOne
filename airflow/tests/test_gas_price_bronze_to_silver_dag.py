"""Gas Price Bronze -> Silver DAG의 2컬럼 적재·GX 계약을 검증합니다."""

import importlib
from datetime import date, datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from dags import gas_price_bronze_to_silver_dag as dag_module


layout = importlib.import_module("lambda.functions.common.gas_price_layout")
loader = importlib.import_module(
    "lambda.functions.gas_price_bronze_to_silver.loader"
)

DAG = dag_module.gas_price_bronze_to_silver_dag
run_bronze_to_silver = DAG.get_task("bronze_to_silver").python_callable
validate_silver = DAG.get_task("validate_silver").python_callable

COLLECTED_MONTH = "2026-07"


def row(target_date: date = date(2026, 7, 1), **overrides) -> dict:
    values = {"date": target_date, "gas_price": 3.159}
    values.update(overrides)
    return values


@pytest.fixture
def silver_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(dag_module, "SILVER_DIR", str(tmp_path))
    return str(tmp_path)


def write_silver(
    silver_dir: str,
    rows: list[dict],
    schema: pa.Schema | None = None,
):
    path = layout.silver_file(silver_dir, COLLECTED_MONTH)
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(
        rows,
        schema=loader.SCHEMA if schema is None else schema,
    )
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


def test_dag_id와_task_구성이_정확하다():
    assert DAG.dag_id == "gas_price_bronze_to_silver_pipeline"
    assert {task.task_id for task in DAG.tasks} == {
        "bronze_to_silver",
        "validate_silver",
    }
    assert DAG.get_task("bronze_to_silver").downstream_task_ids == {
        "validate_silver"
    }


def test_validation_task는_재시도와_slack_callback을_사용한다():
    task = DAG.get_task("validate_silver")

    assert task.retries == 1
    assert dag_module.slack_failure_callback in task.on_failure_callback


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


def test_정상_2컬럼_silver는_검증을_통과한다(silver_dir):
    path = write_silver(
        silver_dir,
        [row(), row(date(2026, 7, 2), gas_price=3.2)],
    )

    validate_silver(result_of(path, row_count=2))


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


def test_실제_행_수와_row_count가_다르면_gx가_거부한다(silver_dir):
    path = write_silver(silver_dir, [row()])

    with pytest.raises(ValueError, match="expect_table_row_count_to_equal"):
        validate_silver(result_of(path, row_count=2))


def test_필수_컬럼이_누락되면_gx가_거부한다(silver_dir, caplog):
    broken_schema = pa.schema([("date", pa.date32())])
    path = write_silver(silver_dir, [{"date": date(2026, 7, 1)}], broken_schema)

    with pytest.raises(ValueError, match="expect_table_columns_to_match_ordered_list"):
        validate_silver(result_of(path))

    assert "gx_validation failed layer=silver" in caplog.text


def test_arrow_물리_스키마가_loader와_다르면_거부한다(silver_dir):
    broken_schema = loader.SCHEMA.set(
        loader.SCHEMA.get_field_index("gas_price"),
        pa.field("gas_price", pa.float64(), nullable=False),
    )
    path = write_silver(silver_dir, [row()], broken_schema)

    with pytest.raises(ValueError, match="스키마"):
        validate_silver(result_of(path))


@pytest.mark.parametrize(
    ("rows", "failed_rule", "column", "observed"),
    [
        pytest.param(
            [row(), row()],
            "expect_column_values_to_be_unique",
            "date",
            "datetime.date(2026, 7, 1)",
            id="날짜 중복",
        ),
        pytest.param(
            [row(gas_price=None)],
            "expect_column_values_to_not_be_null",
            "gas_price",
            None,
            id="가격 NULL",
        ),
        pytest.param(
            [row(gas_price=0.0)],
            "expect_column_values_to_be_between",
            "gas_price",
            "0.0",
            id="가격 0",
        ),
        pytest.param(
            [row(gas_price=-1.0)],
            "expect_column_values_to_be_between",
            "gas_price",
            "-1.0",
            id="가격 음수",
        ),
        pytest.param(
            [row(gas_price=float("nan"))],
            "expect_column_values_to_not_be_null",
            "gas_price",
            None,
            id="가격 NaN",
        ),
        pytest.param(
            [row(gas_price=float("inf"))],
            "expect_column_values_to_be_in_set",
            "gas_price_is_finite",
            "False",
            id="가격 Infinity",
        ),
        pytest.param(
            [row(date(2026, 8, 1))],
            "expect_column_values_to_be_between",
            "date",
            "datetime.date(2026, 8, 1)",
            id="대상 월 밖 날짜",
        ),
    ],
)
def test_gx_규칙_위반을_거부하고_관측값을_로그에_남긴다(
    silver_dir, rows, failed_rule, column, observed, caplog
):
    path = write_silver(silver_dir, rows)

    with pytest.raises(ValueError, match=rf"{failed_rule}\[{column}\]"):
        validate_silver(result_of(path, row_count=len(rows)))

    assert f"expectation={failed_rule}" in caplog.text
    assert f"column={column}" in caplog.text
    if observed is not None:
        assert observed in caplog.text
