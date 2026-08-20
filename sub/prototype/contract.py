"""산출물 계약 스키마. `schema.source` 를 단일 진실 원천으로 씁니다.

운영 경로(`sub/airflow/scripts/synthetic_driver_trip_source/tasks.py`)와 같은
스키마를 읽어야 두 경로의 계약이 갈리지 않습니다.
"""

from __future__ import annotations

from schema.source import (
    DRIVER_VEHICLE_MONTHLY_SNAPSHOT_SCHEMA,
    LEASE_VEHICLE_INVENTORY_SCHEMA,
    MONTHLY_TAXI_TRIP_SCHEMA,
)


def trip_schema():
    return MONTHLY_TAXI_TRIP_SCHEMA


def driver_vehicle_schema():
    return DRIVER_VEHICLE_MONTHLY_SNAPSHOT_SCHEMA


def vehicle_inventory_schema():
    return LEASE_VEHICLE_INVENTORY_SCHEMA
