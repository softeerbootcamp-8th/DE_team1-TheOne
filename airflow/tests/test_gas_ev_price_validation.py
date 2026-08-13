"""Gas·EV 개별·통합 Silver Validation Task의 GX 계약을 확인합니다.

논리 스키마는 nullable 차이를 허용하지만 숫자 폭과 컬럼 순서는 엄격히 검증합니다.
"""

import importlib
from datetime import date, timedelta

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from dags import gas_ev_price_bronze_to_silver_dag as dag_module

gas_layout = importlib.import_module("lambda.functions.common.gas_price_layout")
ev_layout = importlib.import_module("lambda.functions.common.ev_charging_layout")
gas_loader = importlib.import_module(
    "lambda.functions.gas_price_bronze_to_silver.loader"
)
ev_loader = importlib.import_module(
    "lambda.functions.ev_charging_stations_bronze_to_silver.loader"
)

DAG = dag_module.gas_ev_price_bronze_to_silver_dag
validate_gas = DAG.get_task("validate_gas_silver").python_callable
validate_ev = DAG.get_task("validate_ev_silver").python_callable
validate_integrated = DAG.get_task("validate_integrated_silver").python_callable


@pytest.fixture
def silver_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(dag_module, "SILVER_DIR", str(tmp_path))
    return str(tmp_path)


def write_table(path, rows, schema):
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), path)
    return path


def result(path, rows=1):
    return {
        "row_count": rows,
        "locations": [str(path)],
        "collected_month": "2026-07",
    }


def gas_path(silver_dir, rows, schema=None):
    return write_table(
        gas_layout.silver_file(silver_dir, "2026-07"),
        rows,
        schema or gas_loader.SCHEMA,
    )


def ev_path(silver_dir, rows, schema=None):
    return write_table(
        ev_layout.silver_file(silver_dir, "2026-07"),
        rows,
        schema or ev_loader.SCHEMA,
    )


def integrated_path(silver_dir, rows, schema=None):
    return write_table(
        dag_module.integrated_silver_file(silver_dir, "2026-07"),
        rows,
        schema or dag_module.INTEGRATED_SCHEMA,
    )


VALIDATION_CASES = [
    pytest.param(
        validate_gas,
        gas_path,
        gas_loader.SCHEMA,
        {"date": date(2026, 7, 1), "gas_price": 3.1},
        "gas_price",
        id="gas",
    ),
    pytest.param(
        validate_ev,
        ev_path,
        ev_loader.SCHEMA,
        {"date": date(2026, 7, 1), "ev_price": 0.3},
        "ev_price",
        id="ev",
    ),
    pytest.param(
        validate_integrated,
        integrated_path,
        dag_module.INTEGRATED_SCHEMA,
        {"date": date(2026, 7, 1), "gas_price": 3.1, "ev_price": 0.3},
        "gas_price",
        id="integrated",
    ),
]


def test_정상_개별_통합_silver는_모두_통과한다(silver_dir):
    gas = gas_path(
        silver_dir, [{"date": date(2026, 7, 1), "gas_price": 3.1}]
    )
    ev = ev_path(
        silver_dir, [{"date": date(2026, 7, 1), "ev_price": 0.3}]
    )
    integrated = integrated_path(
        silver_dir,
        [{"date": date(2026, 7, 1), "gas_price": 3.1, "ev_price": 0.3}],
    )

    validate_gas(result(gas))
    validate_ev(result(ev))
    validate_integrated(result(integrated))


@pytest.mark.parametrize(
    ("validator", "writer", "schema", "row", "price_column"),
    VALIDATION_CASES,
)
def test_nullable_차이는_논리_스키마에서_허용한다(
    silver_dir, validator, writer, schema, row, price_column
):
    non_nullable_schema = pa.schema(
        pa.field(field.name, field.type, nullable=False)
        if field.name == price_column
        else field
        for field in schema
    )
    path = writer(silver_dir, [row], schema=non_nullable_schema)

    validator(result(path))


@pytest.mark.parametrize(
    ("validator", "writer", "schema", "row", "price_column"),
    VALIDATION_CASES,
)
def test_float32_가격은_float64_논리_계약과_달라_거부한다(
    silver_dir, validator, writer, schema, row, price_column
):
    float32_schema = pa.schema(
        pa.field(field.name, pa.float32(), nullable=field.nullable)
        if field.name == price_column
        else field
        for field in schema
    )
    path = writer(silver_dir, [row], schema=float32_schema)

    with pytest.raises(
        ValueError,
        match=rf"expect_column_values_to_be_of_type\[{price_column}\]",
    ):
        validator(result(path))


@pytest.mark.parametrize(
    ("validator", "writer", "schema", "row", "price_column"),
    VALIDATION_CASES,
)
def test_컬럼_순서가_계약과_다르면_거부한다(
    silver_dir, validator, writer, schema, row, price_column
):
    reordered_schema = pa.schema(
        [
            schema.field(price_column),
            *(field for field in schema if field.name != price_column),
        ]
    )
    path = writer(silver_dir, [row], schema=reordered_schema)

    with pytest.raises(
        ValueError,
        match="expect_table_columns_to_match_ordered_list",
    ):
        validator(result(path))


@pytest.mark.parametrize(
    ("validator", "writer", "row", "rule"),
    [
        (
            validate_gas,
            gas_path,
            {"date": date(2026, 7, 1), "gas_price": 0.0},
            "expect_column_values_to_be_between",
        ),
        (
            validate_ev,
            ev_path,
            {"date": date(2026, 7, 1), "ev_price": 0.0},
            "expect_column_values_to_be_between",
        ),
        (
            validate_ev,
            ev_path,
            {"date": date(2026, 7, 1), "ev_price": 5.01},
            "expect_column_values_to_be_between",
        ),
    ],
)
def test_0이하_또는_허용범위_밖_가격을_거부한다(
    silver_dir, validator, writer, row, rule
):
    path = writer(silver_dir, [row])

    with pytest.raises(ValueError, match=rule):
        validator(result(path))


@pytest.mark.parametrize(
    ("validator", "writer", "price_column"),
    [
        (validate_gas, gas_path, "gas_price"),
        (validate_ev, ev_path, "ev_price"),
    ],
)
def test_날짜_중복을_거부한다(silver_dir, validator, writer, price_column):
    rows = [
        {"date": date(2026, 7, 1), price_column: 0.3},
        {"date": date(2026, 7, 1), price_column: 0.4},
    ]
    path = writer(silver_dir, rows)

    with pytest.raises(ValueError, match="expect_column_values_to_be_unique"):
        validator(result(path, 2))


def test_대상월_밖의_날짜를_거부한다(silver_dir):
    path = gas_path(
        silver_dir, [{"date": date(2026, 8, 1), "gas_price": 3.1}]
    )

    with pytest.raises(ValueError, match="expect_column_values_to_be_between"):
        validate_gas(result(path))


def test_Handler_행수와_실제_행수가_다르면_거부한다(silver_dir):
    path = ev_path(
        silver_dir, [{"date": date(2026, 7, 1), "ev_price": 0.3}]
    )

    with pytest.raises(ValueError, match="expect_table_row_count_to_equal"):
        validate_ev(result(path, 2))


def test_통합_silver_NULL을_거부한다(silver_dir):
    path = integrated_path(
        silver_dir,
        [{"date": date(2026, 7, 1), "gas_price": 3.1, "ev_price": None}],
    )

    with pytest.raises(ValueError, match="expect_column_values_to_not_be_null"):
        validate_integrated(result(path))


def test_GX_실패는_규칙과_관측값을_로그에_남긴다(silver_dir, caplog):
    path = gas_path(
        silver_dir, [{"date": date(2026, 7, 1), "gas_price": -1.0}]
    )

    with pytest.raises(ValueError):
        validate_gas(result(path))

    assert "gx_validation failed layer=gas_silver" in caplog.text
    assert "expectation=expect_column_values_to_be_between" in caplog.text
    assert "column=gas_price" in caplog.text
    assert "observed_value=[-1.0]" in caplog.text


def test_잘못된_경로를_정상_silver로_오인하지_않는다(silver_dir, tmp_path):
    path = gas_path(
        str(tmp_path / "elsewhere"),
        [{"date": date(2026, 7, 1), "gas_price": 3.1}],
    )

    with pytest.raises(ValueError, match="적재 경로"):
        validate_gas(result(path))


def test_validation_task는_재시도와_Slack_callback을_유지한다():
    for task_id in (
        "validate_gas_silver",
        "validate_ev_silver",
        "validate_integrated_silver",
    ):
        validation = DAG.get_task(task_id)
        assert validation.retries == 1
        assert validation.retry_delay == timedelta(minutes=10)
        assert dag_module.slack_failure_callback in validation.on_failure_callback
