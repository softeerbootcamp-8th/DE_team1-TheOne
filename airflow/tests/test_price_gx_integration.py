"""Gas·EV 네 요금 DAG의 GX 성공·실패 흐름을 실제 Task 상태로 확인합니다.

1. 정상 적재 결과는 upstream, validation, DagRun이 모두 성공한다.
2. 대표 GX 위반은 validation을 한 번 재시도한 뒤 실패한다.
3. 최종 실패는 규칙·컬럼·관측값을 기록하고 Slack 콜백을 한 번 호출한다.
"""

import importlib
import json
from datetime import date, datetime, timedelta, timezone

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from dags import ev_charging_price_bronze_to_silver_dag as ev_silver_module
from dags import ev_charging_price_raw_to_bronze_dag as ev_bronze_module
from dags import gas_price_bronze_to_silver_dag as gas_silver_module
from dags import gas_price_raw_to_bronze_dag as gas_bronze_module


ev_layout = importlib.import_module("lambda.functions.common.ev_charging_layout")
gas_layout = importlib.import_module("lambda.functions.common.gas_price_layout")
ev_loader = importlib.import_module(
    "lambda.functions.ev_charging_stations_bronze_to_silver.loader"
)
gas_loader = importlib.import_module(
    "lambda.functions.gas_price_bronze_to_silver.loader"
)

CASES = {
    "ev_bronze": (
        ev_bronze_module,
        ev_bronze_module.ev_charging_price_raw_to_bronze_dag,
        "raw_to_bronze",
        "validate_bronze",
        "bronze",
        "expect_column_values_to_be_in_set",
        "state",
    ),
    "ev_silver": (
        ev_silver_module,
        ev_silver_module.ev_charging_price_bronze_to_silver_dag,
        "bronze_to_silver",
        "validate_silver",
        "silver",
        "expect_column_pair_values_to_be_equal",
        "classified_station_count/nyc_station_count",
    ),
    "gas_bronze": (
        gas_bronze_module,
        gas_bronze_module.gas_price_raw_to_bronze_dag,
        "raw_to_bronze",
        "validate_bronze",
        "bronze",
        "expect_column_values_to_be_between",
        "parsed_price",
    ),
    "gas_silver": (
        gas_silver_module,
        gas_silver_module.gas_price_bronze_to_silver_dag,
        "bronze_to_silver",
        "validate_silver",
        "silver",
        "expect_column_values_to_be_unique",
        "date",
    ),
}


def _ev_silver_row(**overrides) -> dict:
    row = {
        "city": "New York City",
        "state": "NY",
        "fuel_type_code": "ELEC",
        "average_price_usd_per_kwh": 0.31,
        "price_date": date(2026, 7, 1),
        "currency": "USD",
        "price_unit": "kWh",
        "nyc_station_count": 10,
        "normalized_price_count": 8,
        "free_station_count": 1,
        "missing_price_count": 1,
        "unsupported_price_count": 0,
        "source_url": "https://developer.nlr.gov/",
        "collected_at": datetime(2026, 7, 1, 10, tzinfo=timezone.utc),
        "bronze_path": "data/bronze/ev_charging_stations/collected_date=2026-07-01",
    }
    row.update(overrides)
    return row


def _gas_silver_row(**overrides) -> dict:
    row = {
        "date": date(2026, 7, 1),
        "gas_price": 3.159,
    }
    row.update(overrides)
    return row


def _build_result(case_name: str, root, invalid: bool, monkeypatch) -> dict:
    if case_name == "ev_bronze":
        monkeypatch.setattr(ev_bronze_module, "BRONZE_DIR", str(root))
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

    if case_name == "gas_bronze":
        monkeypatch.setattr(gas_bronze_module, "BRONZE_DIR", str(root))
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

    if case_name == "ev_silver":
        monkeypatch.setattr(ev_silver_module, "SILVER_DIR", str(root))
        path = ev_layout.silver_file(str(root), "2026-07")
        path.parent.mkdir(parents=True, exist_ok=True)
        row = _ev_silver_row(normalized_price_count=1) if invalid else _ev_silver_row()
        pq.write_table(pa.Table.from_pylist([row], schema=ev_loader.SCHEMA), path)
        return {
            "row_count": 1,
            "locations": [str(path)],
            "collected_month": "2026-07",
        }

    monkeypatch.setattr(gas_silver_module, "SILVER_DIR", str(root))
    path = gas_layout.silver_file(str(root), "2026-07")
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = [_gas_silver_row(), _gas_silver_row()] if invalid else [_gas_silver_row()]
    pq.write_table(pa.Table.from_pylist(rows, schema=gas_loader.SCHEMA), path)
    return {
        "row_count": len(rows),
        "locations": [str(path)],
        "collected_month": "2026-07",
    }


def _run_dag(case_name, result, monkeypatch, callback):
    _, dag, upstream_id, validation_id, *_ = CASES[case_name]
    upstream = dag.get_task(upstream_id)
    validation = dag.get_task(validation_id)
    monkeypatch.setattr(upstream, "python_callable", lambda **_: result)
    monkeypatch.setattr(validation, "retry_delay", timedelta(0))
    monkeypatch.setattr(validation, "on_failure_callback", [callback])
    run = dag.test(logical_date=datetime(2026, 8, 12, tzinfo=timezone.utc))
    instances = {instance.task_id: instance for instance in run.get_task_instances()}
    return run, instances


@pytest.mark.parametrize("case_name", CASES)
def test_정상_데이터는_적재와_gx_validation_task가_성공한다(
    case_name, tmp_path, monkeypatch
):
    _, _, upstream_id, validation_id, *_ = CASES[case_name]
    result = _build_result(case_name, tmp_path / case_name, False, monkeypatch)
    callbacks = []

    run, instances = _run_dag(
        case_name,
        result,
        monkeypatch,
        lambda context: callbacks.append(context),
    )

    assert run.state == "success"
    assert instances[upstream_id].state == "success"
    assert instances[validation_id].state == "success"
    assert instances[validation_id].try_number == 1
    assert callbacks == []


@pytest.mark.parametrize("case_name", CASES)
def test_gx_실패는_재시도후_task와_dag를_실패시키고_콜백을_호출한다(
    case_name, tmp_path, monkeypatch, caplog
):
    module, dag, upstream_id, validation_id, layer, expectation, column = CASES[
        case_name
    ]
    validation = dag.get_task(validation_id)
    assert validation.retries == 1
    assert validation.retry_delay == timedelta(minutes=10)
    assert module.slack_failure_callback in validation.on_failure_callback

    result = _build_result(case_name, tmp_path / case_name, True, monkeypatch)
    callbacks = []

    run, instances = _run_dag(
        case_name,
        result,
        monkeypatch,
        lambda context: callbacks.append(context["task_instance"].task_id),
    )

    assert run.state == "failed"
    assert instances[upstream_id].state == "success"
    assert instances[validation_id].state == "failed"
    assert instances[validation_id].try_number == 2
    assert callbacks == [validation_id]
    assert f"gx_validation failed layer={layer}" in caplog.text
    assert f"expectation={expectation}" in caplog.text
    assert f"column={column}" in caplog.text
    assert "unexpected_count=" in caplog.text
    assert "observed_value=" in caplog.text
    assert "observed_value=None" not in caplog.text
