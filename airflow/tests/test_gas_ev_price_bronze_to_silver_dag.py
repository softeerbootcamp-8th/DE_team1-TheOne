"""Gas·EV 월별 Silver DAG 실행·통합 시나리오.

1. Gas·EV 변환과 검증은 병렬이고 두 검증 뒤 통합·최종 검증이 실행된다.
2. 수동 월이 없으면 실행 시점 기준 직전 완료 월을 두 Lambda에 전달한다.
3. 검증된 두 Silver는 같은 날짜끼리 1:1 결합한다.
4. 날짜 중복·집합 불일치는 통합을 실패시킨다.
5. 같은 월 재실행은 통합 Parquet 하나를 교체한다.
"""

import importlib
from datetime import date, datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from airflow.exceptions import ParamValidationError

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


def gas_table(*rows: dict) -> pa.Table:
    return pa.Table.from_pylist(list(rows), schema=gas_loader.SCHEMA)


def ev_table(*rows: dict) -> pa.Table:
    return pa.Table.from_pylist(list(rows), schema=ev_loader.SCHEMA)


def write_sources(root, gas_rows: list[dict], ev_rows: list[dict]):
    gas_path = gas_layout.silver_file(str(root), "2026-07")
    ev_path = ev_layout.silver_file(str(root), "2026-07")
    gas_path.parent.mkdir(parents=True, exist_ok=True)
    ev_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(gas_table(*gas_rows), gas_path)
    pq.write_table(ev_table(*ev_rows), ev_path)
    return gas_path, ev_path


def result(path, rows: int) -> dict:
    return {
        "row_count": rows,
        "locations": [str(path)],
        "collected_month": "2026-07",
    }


def test_dag는_6개_task를_검증_후_통합_순서로_연결한다():
    assert DAG.dag_id == "gas_ev_price_bronze_to_silver_pipeline"
    assert {task.task_id for task in DAG.tasks} == {
        "gas_bronze_to_silver",
        "validate_gas_silver",
        "ev_bronze_to_silver",
        "validate_ev_silver",
        "integrate_silver",
        "validate_integrated_silver",
    }
    assert DAG.get_task("validate_gas_silver").downstream_task_ids == {
        "integrate_silver"
    }
    assert DAG.get_task("validate_ev_silver").downstream_task_ids == {
        "integrate_silver"
    }
    assert DAG.get_task("integrate_silver").downstream_task_ids == {
        "validate_integrated_silver"
    }


@pytest.mark.parametrize(
    ("interval_end", "expected"),
    [
        (datetime(2026, 8, 1, tzinfo=timezone.utc), "2026-07"),
        (datetime(2026, 1, 1, tzinfo=timezone.utc), "2025-12"),
    ],
)
def test_직전_완료_월을_계산한다(interval_end, expected):
    assert dag_module.previous_month(interval_end) == expected


def test_수동_collected_month가_실행시점보다_우선한다():
    assert dag_module.target_month(
        {
            "params": {"collected_month": "2026-05"},
            "data_interval_end": datetime(2026, 8, 1, tzinfo=timezone.utc),
        }
    ) == "2026-05"


@pytest.mark.parametrize("month", ["2026-1", "2026-13", "2026-07-01"])
def test_collected_month_param은_비표준_월을_거부한다(month):
    with pytest.raises(ParamValidationError):
        DAG.params.get_param("collected_month").resolve(month)


def test_Gas와_EV_lambda에_같은_월을_전달한다(monkeypatch):
    events = {}

    def fake_handler_for(name):
        def handler(event):
            events[name] = event
            return {
                "row_count": 1,
                "locations": [f"/{name}.parquet"],
                "collected_month": event["collected_month"],
            }

        return handler

    monkeypatch.setattr(dag_module, "lambda_handler_for", fake_handler_for)
    context = {
        "params": {"collected_month": None},
        "data_interval_end": datetime(2026, 8, 1, tzinfo=timezone.utc),
    }
    DAG.get_task("gas_bronze_to_silver").python_callable(**context)
    DAG.get_task("ev_bronze_to_silver").python_callable(**context)

    assert {event["collected_month"] for event in events.values()} == {"2026-07"}


def test_같은_날짜_집합을_날짜순으로_통합한다():
    combined = dag_module.combine_price_tables(
        gas_table(
            {"date": date(2026, 7, 2), "gas_price": 3.2},
            {"date": date(2026, 7, 1), "gas_price": 3.1},
        ),
        ev_table(
            {"date": date(2026, 7, 1), "ev_price": 0.3},
            {"date": date(2026, 7, 2), "ev_price": 0.4},
        ),
    )

    assert combined.schema == dag_module.INTEGRATED_SCHEMA
    assert combined.to_pylist() == [
        {"date": date(2026, 7, 1), "gas_price": 3.1, "ev_price": 0.3},
        {"date": date(2026, 7, 2), "gas_price": 3.2, "ev_price": 0.4},
    ]


def test_두_silver의_날짜_집합이_다르면_거부한다():
    with pytest.raises(ValueError, match="날짜 집합"):
        dag_module.combine_price_tables(
            gas_table({"date": date(2026, 7, 1), "gas_price": 3.1}),
            ev_table({"date": date(2026, 7, 2), "ev_price": 0.3}),
        )


def test_원천_silver에_중복_날짜가_있으면_거부한다():
    with pytest.raises(ValueError, match="중복 날짜"):
        dag_module.combine_price_tables(
            gas_table(
                {"date": date(2026, 7, 1), "gas_price": 3.1},
                {"date": date(2026, 7, 1), "gas_price": 3.2},
            ),
            ev_table({"date": date(2026, 7, 1), "ev_price": 0.3}),
        )


def test_통합_task는_월별_고정_파일을_교체한다(tmp_path, monkeypatch):
    monkeypatch.setattr(dag_module, "SILVER_DIR", str(tmp_path))
    gas_path, ev_path = write_sources(
        tmp_path,
        [{"date": date(2026, 7, 1), "gas_price": 3.1}],
        [{"date": date(2026, 7, 1), "ev_price": 0.3}],
    )
    integrate = DAG.get_task("integrate_silver").python_callable
    first = integrate(result(gas_path, 1), result(ev_path, 1))

    pq.write_table(
        ev_table({"date": date(2026, 7, 1), "ev_price": 0.4}), ev_path
    )
    second = integrate(result(gas_path, 1), result(ev_path, 1))
    path = dag_module.integrated_silver_file(str(tmp_path), "2026-07")

    assert first["locations"] == second["locations"] == [str(path)]
    assert list(path.parent.glob("*.parquet")) == [path]
    assert pq.ParquetFile(path).read().to_pylist() == [
        {"date": date(2026, 7, 1), "gas_price": 3.1, "ev_price": 0.4}
    ]
