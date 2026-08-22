"""main DAG dry-run 계약.

1. 모든 main DAG의 마지막 파라미터는 boolean `dry_run=false`
2. source 감시는 미변경 원천도 dry-run이면 하위 DAG로 전달
3. source dry-run은 Variable을 갱신하지 않고 READY Asset은 발행
4. 하위 DAG conf는 `dry_run`을 문자열이 아닌 boolean으로 전달
5. Asset 이벤트의 `dry_run`은 Gold의 `--dry-run`까지 전달
6. 배포 dry-run Lambda 이벤트만 S3 저장소 설정을 전달
"""

import importlib
from types import SimpleNamespace

import pytest

from dags.source_api_refresh_dag import source_api_refresh_dag
from main.airflow.common import assets
from main.airflow.common.dry_run import configure_dry_run_event
from main.airflow.scripts.monthly_taxi_trip_silver_to_gold import tasks as gold_tasks
from main.airflow.scripts.source_api_refresh import tasks as source_tasks


MAIN_DAGS = {
    "driver_vehicle_monthly_snapshot_raw_to_silver_dag": (
        "driver_vehicle_monthly_snapshot_raw_to_silver_dag"
    ),
    "eia_electricity_price_raw_to_silver_dag": (
        "eia_electricity_price_raw_to_silver_dag"
    ),
    "eia_fuel_price_silver_dag": "eia_fuel_price_silver_dag",
    "eia_gas_price_raw_to_silver_dag": "eia_gas_price_raw_to_silver_dag",
    "monthly_taxi_trip_raw_to_silver_dag": "monthly_taxi_trip_dag",
    "monthly_taxi_trip_silver_to_gold_dag": "monthly_taxi_trip_silver_to_gold_dag",
    "lease_vehicle_inventory_raw_to_silver_dag": (
        "lease_vehicle_inventory_raw_to_silver_dag"
    ),
    "source_api_refresh_dag": "source_api_refresh_dag",
}


@pytest.mark.parametrize(("module_name", "variable"), MAIN_DAGS.items())
def test_모든_main_DAG의_마지막_파라미터는_boolean_dry_run이다(
    module_name,
    variable,
):
    dag = getattr(importlib.import_module(f"dags.{module_name}"), variable)
    param = dag.params.get_param("dry_run")

    assert list(dag.params)[-1] == "dry_run"
    assert param.value is False
    assert param.schema["type"] == "boolean"


def test_미변경_원천도_dry_run이면_short_circuit하지_않는다(monkeypatch):
    result = {
        "dataset": "monthly_taxi_trip",
        "year_month": "2026-08",
        "year": "2026",
        "month": "08",
        "api_base_url": "https://source.example",
        "etag": '"same"',
        "last_modified": "Fri, 21 Aug 2026 00:00:00 GMT",
        "changed": False,
        "version": "same",
    }
    monkeypatch.setattr(source_tasks.Variable, "get", lambda *args, **kwargs: None)
    monkeypatch.setattr(source_tasks, "inspect_source", lambda *args, **kwargs: result)

    actual = source_tasks.check_and_should_refresh_task.function(
        "monthly_taxi_trip",
        params={
            "api_base_url": "https://source.example",
            "request_timeout": 30,
            "dry_run": True,
        },
    )

    assert actual == result


def test_source_dry_run은_상태를_갱신하지_않고_READY_Asset을_발행한다(
    monkeypatch,
):
    published = []
    monkeypatch.setattr(
        source_tasks.Variable,
        "set",
        lambda *args, **kwargs: pytest.fail("dry-run에서 Variable.set 호출"),
    )
    monkeypatch.setattr(
        source_tasks.assets,
        "publish_month_partition",
        lambda outlet_events, asset, year_month, **kwargs: published.append(
            (asset, year_month, kwargs)
        ),
    )
    result = {
        "dataset": "monthly_taxi_trip",
        "api_base_url": "https://source.example",
        "year_month": "2026-08",
        "etag": '"same"',
        "last_modified": "Fri, 21 Aug 2026 00:00:00 GMT",
        "changed": False,
    }

    source_tasks.mark_processed_task.function(result, params={"dry_run": True})
    source_tasks.publish_api_refresh_ready_task.function(
        ["check_and_should_refresh_monthly_taxi_trip"],
        params={"dry_run": True},
        task_instance=SimpleNamespace(xcom_pull=lambda **kwargs: [result]),
        outlet_events=object(),
    )

    assert published == [
        (assets.API_SILVER_REFRESH_READY, "2026-08", {"dry_run": True})
    ]


def test_source는_dry_run을_boolean으로_하위_DAG에_전달한다():
    trigger = source_api_refresh_dag.get_task("trigger_monthly_taxi_trip")

    class TaskInstance:
        def xcom_pull(self, task_ids):
            return {
                "year": "2026",
                "month": "08",
                "api_base_url": "https://source.example",
                "year_month": "2026-08",
                "version": "same",
            }

    context = {
        "params": {"dry_run": True},
        "run_id": "manual__dry_run",
        "ti": TaskInstance(),
    }
    conf = trigger.render_template(trigger.conf, context)
    trigger_run_id = trigger.render_template(trigger.trigger_run_id, context)

    assert conf == {
        "year": "2026",
        "month": "08",
        "api_base_url": "https://source.example",
        "dry_run": True,
    }
    assert trigger_run_id.endswith("__dry_run__manual__dry_run")


def test_Asset_dry_run은_Gold의_dry_run으로_전달된다(tmp_path):
    year_month = "2026-08"
    monthly_trip = tmp_path / "monthly_taxi_trip" / f"year_month={year_month}"
    monthly_trip.mkdir(parents=True)
    (monthly_trip / "part-00000.parquet").touch()
    files = {
        "driver_vehicle_monthly_snapshot": (
            "driver_vehicle_monthly_snapshot.parquet"
        ),
        "lease_vehicle_inventory": "lease_vehicle_inventory.parquet",
        "gas_ev_price": "gas_ev_price.parquet",
    }
    for dataset, file_name in files.items():
        partition = tmp_path / dataset / f"year_month={year_month}"
        partition.mkdir(parents=True)
        (partition / file_name).touch()

    params = {
        "year": None,
        "month": None,
        "monthly_taxi_trip_path": str(tmp_path / "monthly_taxi_trip"),
        "driver_vehicle_monthly_snapshot_path": str(
            tmp_path / "driver_vehicle_monthly_snapshot"
        ),
        "lease_vehicle_inventory_path": str(
            tmp_path / "lease_vehicle_inventory"
        ),
        "fuel_price_path": str(tmp_path / "gas_ev_price"),
        "dry_run": False,
    }
    result = gold_tasks.validate_inputs_task.function(
        params=params,
        dag_run=SimpleNamespace(partition_key=year_month),
        triggering_asset_events={
            assets.API_SILVER_REFRESH_READY: [
                SimpleNamespace(extra={"dry_run": True})
            ]
        },
    )

    assert result["dry_run"] is True


@pytest.mark.parametrize("dry_run", [True, False])
def test_Gold_build는_확정된_dry_run_boolean으로_옵션을_렌더링한다(dry_run):
    build = importlib.import_module(
        "dags.monthly_taxi_trip_silver_to_gold_dag"
    ).monthly_taxi_trip_silver_to_gold_dag.get_task("build_gold")
    resolved = {
        "monthly_taxi_trip_path": "/silver/monthly_taxi_trip.parquet",
        "driver_vehicle_monthly_snapshot_path": "/silver/driver.parquet",
        "lease_vehicle_inventory_path": "/silver/inventory.parquet",
        "fuel_price_path": "/silver/fuel.parquet",
        "year": "2026",
        "month": "8",
        "dry_run": dry_run,
    }
    context = {
        "params": {
            "threshold_profit_increase": 600,
            "output_dir": "/gold",
        },
        "task_instance": SimpleNamespace(xcom_pull=lambda **kwargs: resolved),
    }

    command = build.render_template(build.bash_command, context)

    assert ("--dry-run" in command) is dry_run


def test_운영_Gold도_Asset_dry_run을_EMR인자로_전달한다(monkeypatch):
    monkeypatch.setenv("SPARK_JOB_ENV", "prod")
    result = gold_tasks.validate_inputs_task.function(
        params={
            "year": "2026",
            "month": "8",
            "monthly_taxi_trip_path": "/unused",
            "dry_run": False,
        },
        dag_run=SimpleNamespace(partition_key=None),
        triggering_asset_events={
            assets.FUEL_PRICE_SILVER: [
                SimpleNamespace(extra={"dry_run": True})
            ]
        },
    )

    assert result["dry_run"] is True

    dag_module = importlib.import_module(
        "dags.monthly_taxi_trip_silver_to_gold_dag"
    )
    for name, value in {
        "EMR_APPLICATION_ID": "app-test",
        "EMR_EXECUTION_ROLE_ARN": "arn:aws:iam::123:role/emr",
        "DATA_LAKE_S3_BUCKET": "lake",
        "GOLD_DATABASE_URL": "postgresql://gold",
    }.items():
        monkeypatch.setenv(name, value)
    operator = dag_module._emr_build_gold()
    rendered = operator.render_template(
        operator.job_driver,
        {
            "params": {"threshold_profit_increase": 600},
            "task_instance": SimpleNamespace(xcom_pull=lambda **kwargs: result),
        },
    )
    arguments = rendered["sparkSubmit"]["entryPointArguments"]

    assert arguments[arguments.index("--dry-run") + 1] == "true"


def test_dry_run_Lambda_event만_S3_저장소를_전달한다(monkeypatch):
    monkeypatch.setenv("BRONZE_STORAGE", "s3")
    monkeypatch.setenv("DATA_LAKE_S3_BUCKET", "lake")
    normal = {"year": "2026", "month": "08"}

    assert configure_dry_run_event(normal.copy(), {"dry_run": False}) == normal
    assert configure_dry_run_event(normal.copy(), {"dry_run": True}) == {
        **normal,
        "dry_run": True,
        "storage": "s3",
        "bucket": "lake",
    }
