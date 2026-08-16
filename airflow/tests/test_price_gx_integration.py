"""Gas·EV 요금 DAG의 GX 성공·실패 흐름을 실제 Task 상태로 확인합니다.

1. 두 Raw→Bronze DAG의 대표 GX 성공·실패 흐름을 유지한다.
2. 통합 월별 Silver DAG의 6개 Task가 정상 데이터에서 모두 성공한다.
3. Gas·EV·통합 GX 실패는 재시도 후 Task/DagRun 실패와 콜백으로 이어진다.
"""

import importlib
import json
from datetime import date, datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from dags import ev_charging_price_raw_to_bronze_dag as ev_bronze_module
from dags import gas_ev_price_bronze_to_silver_dag as silver_module
from dags import gas_price_raw_to_bronze_dag as gas_bronze_module
from scripts.ev_charging_price_raw_to_bronze import tasks as ev_bronze_tasks
from scripts.gas_ev_price_bronze_to_silver import tasks as silver_tasks
from scripts.gas_price_raw_to_bronze import tasks as gas_bronze_tasks

ev_layout = importlib.import_module("lambda.functions.common.ev_charging_layout")
gas_layout = importlib.import_module("lambda.functions.common.gas_price_layout")
ev_loader = importlib.import_module(
    "lambda.functions.ev_charging_stations_bronze_to_silver.loader"
)
gas_loader = importlib.import_module(
    "lambda.functions.gas_price_bronze_to_silver.loader"
)

RAW_CASES = {
    "ev_bronze": (
        ev_bronze_module,
        ev_bronze_module.ev_charging_price_raw_to_bronze_dag,
        "raw_to_bronze",
        "validate_bronze",
        "expect_column_values_to_be_in_set",
        "state",
    ),
    "gas_bronze": (
        gas_bronze_module,
        gas_bronze_module.gas_price_raw_to_bronze_dag,
        "raw_to_bronze",
        "validate_bronze",
        "expect_column_values_to_be_between",
        "parsed_price",
    ),
}


def _raw_result(case_name: str, root, invalid: bool, monkeypatch) -> dict:
    if case_name == "ev_bronze":
        monkeypatch.setattr(ev_bronze_tasks, "BRONZE_DIR", str(root))
        collected_at = datetime(2026, 8, 9, 12, tzinfo=timezone.utc)
        path = ev_layout.bronze_file(str(root), collected_at)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(
                {
                    "total_results": 1,
                    "fuel_stations": [
                        {
                            "id": 1,
                            "state": "CA" if invalid else "NY",
                            "fuel_type_code": "ELEC",
                            "zip": "10001",
                            "ev_pricing": "$0.30/kWh",
                        }
                    ],
                }
            )
        )
        return {
            "row_count": 1,
            "locations": [str(path)],
            "collected_date": "2026-08-09",
            "state": "NY",
            "fuel_type_code": "ELEC",
        }

    monkeypatch.setattr(gas_bronze_tasks, "BRONZE_DIR", str(root))
    path = gas_layout.bronze_file(str(root), "2026-08-09")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "state": "NY",
                "fuel_type": "regular",
                "price_raw": "$0.00" if invalid else "$3.159",
                "price_date_raw": "8/8/26",
                "source_url": "https://gasprices.aaa.com/?state=NY",
                "collected_at": "2026-08-09T12:00:00+00:00",
            }
        )
    )
    return {
        "row_count": 1,
        "locations": [str(path)],
        "collected_date": "2026-08-09",
    }


def _run_raw_dag(case_name, result, monkeypatch, callback):
    _, dag, upstream_id, validation_id, *_ = RAW_CASES[case_name]
    upstream = dag.get_task(upstream_id)
    validation = dag.get_task(validation_id)
    monkeypatch.setattr(upstream, "python_callable", lambda **_: result)
    monkeypatch.setattr(validation, "on_failure_callback", [callback])
    run = dag.test(logical_date=datetime(2026, 8, 12, tzinfo=timezone.utc))
    instances = {instance.task_id: instance for instance in run.get_task_instances()}
    return run, instances


@pytest.mark.parametrize("case_name", RAW_CASES)
def test_Raw_Bronze_정상_데이터는_적재와_GX가_성공한다(
    case_name, tmp_path, monkeypatch
):
    _, _, upstream_id, validation_id, *_ = RAW_CASES[case_name]
    result = _raw_result(case_name, tmp_path / case_name, False, monkeypatch)
    callbacks = []
    run, instances = _run_raw_dag(
        case_name, result, monkeypatch, lambda context: callbacks.append(context)
    )

    assert run.state == "success"
    assert instances[upstream_id].state == "success"
    assert instances[validation_id].state == "success"
    assert callbacks == []


@pytest.mark.parametrize("case_name", RAW_CASES)
def test_Raw_Bronze_GX_실패는_재시도없이_콜백으로_이어진다(
    case_name, tmp_path, monkeypatch, caplog
):
    module, dag, upstream_id, validation_id, expectation, column = RAW_CASES[
        case_name
    ]
    validation = dag.get_task(validation_id)
    assert module.slack_failure_callback in validation.on_failure_callback
    result = _raw_result(case_name, tmp_path / case_name, True, monkeypatch)
    callbacks = []
    run, instances = _run_raw_dag(
        case_name,
        result,
        monkeypatch,
        lambda context: callbacks.append(context["task_instance"].task_id),
    )

    assert run.state == "failed"
    assert instances[upstream_id].state == "success"
    assert instances[validation_id].state == "failed"
    assert instances[validation_id].try_number == 1
    assert callbacks == [validation_id]
    assert f"expectation={expectation}" in caplog.text
    assert f"column={column}" in caplog.text


def _write_silver_inputs(root, invalid_layer: str | None = None):
    gas_path = gas_layout.silver_file(str(root), "2026-07")
    ev_path = ev_layout.silver_file(str(root), "2026-07")
    gas_path.parent.mkdir(parents=True, exist_ok=True)
    ev_path.parent.mkdir(parents=True, exist_ok=True)

    gas_rows = [{"date": date(2026, 7, 1), "gas_price": 3.159}]
    if invalid_layer == "gas_silver":
        gas_rows.append(gas_rows[0].copy())
    ev_price = 0.0 if invalid_layer == "ev_silver" else 0.31
    ev_rows = [{"date": date(2026, 7, 1), "ev_price": ev_price}]
    pq.write_table(pa.Table.from_pylist(gas_rows, schema=gas_loader.SCHEMA), gas_path)
    pq.write_table(pa.Table.from_pylist(ev_rows, schema=ev_loader.SCHEMA), ev_path)
    return (
        {
            "row_count": len(gas_rows),
            "locations": [str(gas_path)],
            "collected_month": "2026-07",
        },
        {
            "row_count": 1,
            "locations": [str(ev_path)],
            "collected_month": "2026-07",
        },
    )


def _prepare_silver_dag(root, monkeypatch, invalid_layer=None, callback=None):
    monkeypatch.setattr(silver_tasks, "SILVER_DIR", str(root))
    gas_result, ev_result = _write_silver_inputs(root, invalid_layer)
    dag = silver_module.gas_ev_price_bronze_to_silver_dag
    monkeypatch.setattr(
        dag.get_task("gas_bronze_to_silver"),
        "python_callable",
        lambda **_: gas_result,
    )
    monkeypatch.setattr(
        dag.get_task("ev_bronze_to_silver"),
        "python_callable",
        lambda **_: ev_result,
    )

    if invalid_layer == "integrated_silver":
        path = silver_tasks.integrated_silver_file(str(root), "2026-07")
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(
            pa.Table.from_pylist(
                [
                    {
                        "date": date(2026, 7, 1),
                        "gas_price": 3.159,
                        "ev_price": None,
                    }
                ],
                schema=silver_tasks.INTEGRATED_SCHEMA,
            ),
            path,
        )
        integrated_result = {
            "row_count": 1,
            "locations": [str(path)],
            "collected_month": "2026-07",
        }
        monkeypatch.setattr(
            dag.get_task("integrate_silver"),
            "python_callable",
            lambda *_, **__: integrated_result,
        )

    if callback and invalid_layer:
        monkeypatch.setattr(
            dag.get_task(f"validate_{invalid_layer}"),
            "on_failure_callback",
            [callback],
        )
    return dag


def test_통합_Silver_DAG의_7개_Task가_모두_성공한다(tmp_path, monkeypatch):
    dag = _prepare_silver_dag(tmp_path, monkeypatch)
    run = dag.test(
        logical_date=datetime(2026, 8, 13, tzinfo=timezone.utc),
        run_conf={"require_complete_month": 0},
    )
    instances = {instance.task_id: instance for instance in run.get_task_instances()}

    assert run.state == "success"
    assert {instance.state for instance in instances.values()} == {"success"}


@pytest.mark.parametrize(
    ("invalid_layer", "expectation", "column"),
    [
        ("gas_silver", "expect_column_values_to_be_unique", "date"),
        ("ev_silver", "expect_column_values_to_be_between", "ev_price"),
        (
            "integrated_silver",
            "expect_column_values_to_not_be_null",
            "ev_price",
        ),
    ],
)
def test_통합_Silver_GX_실패는_재시도없이_DAG를_실패시킨다(
    invalid_layer, expectation, column, tmp_path, monkeypatch, caplog
):
    callbacks = []
    validation_id = f"validate_{invalid_layer}"
    dag = _prepare_silver_dag(
        tmp_path,
        monkeypatch,
        invalid_layer,
        lambda context: callbacks.append(context["task_instance"].task_id),
    )
    run = dag.test(
        logical_date=datetime(2026, 8, 13, tzinfo=timezone.utc),
        run_conf={"require_complete_month": 0},
    )
    instances = {instance.task_id: instance for instance in run.get_task_instances()}

    assert run.state == "failed"
    assert instances[validation_id].state == "failed"
    assert instances[validation_id].try_number == 1
    assert callbacks == [validation_id]
    assert f"gx_validation failed layer={invalid_layer}" in caplog.text
    assert f"expectation={expectation}" in caplog.text
    assert f"column={column}" in caplog.text
    assert "observed_value=None" not in caplog.text
