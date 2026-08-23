"""main 데이터 파이프라인이 실제 적재에 필요한 파라미터만 노출하는지 검증합니다."""

import importlib

import pytest


DAG_PARAMS = {
    "driver_vehicle_monthly_snapshot_raw_to_silver_dag": (
        "driver_vehicle_monthly_snapshot_raw_to_silver_dag",
        {
            "year",
            "month",
            "api_base_url",
            "base_dir",
            "silver_dir",
            "service_area",
        },
    ),
    "eia_electricity_price_raw_to_silver_dag": (
        "eia_electricity_price_raw_to_silver_dag",
        {"year", "month", "markup", "bronze_dir", "silver_dir"},
    ),
    "eia_fuel_price_silver_dag": (
        "eia_fuel_price_silver_dag",
        {"year_month", "silver_dir", "service_area"},
    ),
    "eia_gas_price_raw_to_silver_dag": (
        "eia_gas_price_raw_to_silver_dag",
        {"year", "month", "bronze_dir", "silver_dir", "service_area"},
    ),
    "lease_vehicle_inventory_raw_to_silver_dag": (
        "lease_vehicle_inventory_raw_to_silver_dag",
        {"year", "month", "api_base_url", "base_dir", "silver_dir", "service_area"},
    ),
    "monthly_taxi_trip_raw_to_silver_dag": (
        "monthly_taxi_trip_dag",
        {
            "year",
            "month",
            "service_area",
            "base_dir",
            "api_base_url",
            "error_threshold",
        },
    ),
    "monthly_taxi_trip_silver_to_gold_dag": (
        "monthly_taxi_trip_silver_to_gold_dag",
        {
            "year",
            "month",
            "threshold_profit_increase",
            "monthly_taxi_trip_path",
            "driver_vehicle_monthly_snapshot_path",
            "lease_vehicle_inventory_path",
            "fuel_price_path",
            "output_dir",
            "gold_stale_sla_days",
            "service_area",
        },
    ),
    "source_api_refresh_dag": (
        "source_api_refresh_dag",
        {"year", "month", "api_base_url", "request_timeout", "service_area"},
    ),
}


@pytest.mark.parametrize(("module_name", "dag_name", "expected"), [
    (module_name, dag_name, expected)
    for module_name, (dag_name, expected) in DAG_PARAMS.items()
])
def test_main_DAG는_실제_적재_파라미터만_노출한다(
    module_name: str,
    dag_name: str,
    expected: set[str],
) -> None:
    dag = getattr(importlib.import_module(f"dags.{module_name}"), dag_name)

    assert set(dag.params) == expected
