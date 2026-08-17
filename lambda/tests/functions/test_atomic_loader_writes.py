"""Loader의 파일 단위 원자적 교체를 검증합니다."""

from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from functions.driver_master_raw_to_bronze.loader import CompanySnapshotBronzeLoader
from functions.eia_fuel_price_bronze_to_silver.loader import (
    SCHEMA as FUEL_PRICE_SILVER_SCHEMA,
    EiaFuelPriceSilverLoader,
)
from functions.fueleconomy_vehicle_specs_bronze_to_silver.loader import (
    SCHEMA as SPECS_SILVER_SCHEMA,
    VehicleSpecsSilverLoader,
)
from functions.fueleconomy_vehicle_specs_raw_to_bronze.loader import (
    VehicleSpecsBronzeLoader,
)
from functions.hvfhv_raw_to_bronze.loader import HvfhvBronzeLoader
from functions.lyft_eligible_vehicles_raw_to_bronze.loader import (
    SCHEMA as LYFT_BRONZE_SCHEMA,
    LyftEligibleVehiclesBronzeLoader,
)
from functions.lyft_eligible_vehicles_bronze_to_silver.loader import (
    SCHEMA as LYFT_SILVER_SCHEMA,
    LyftEligibleVehiclesSilverLoader,
)
from functions.uber_eligible_vehicles_bronze_to_silver.loader import (
    SCHEMA as UBER_SILVER_SCHEMA,
    UberEligibleVehiclesSilverLoader,
)
from functions.uber_eligible_vehicles_raw_to_bronze.loader import (
    SCHEMA as UBER_BRONZE_SCHEMA,
    UberEligibleVehiclesBronzeLoader,
)
from functions.vehicle_catalog_raw_to_bronze.loader import (
    SCHEMA as CATALOG_BRONZE_SCHEMA,
    VehicleCatalogBronzeLoader,
)
from functions.vehicle_catalog_bronze_to_silver.loader import (
    SCHEMA as CATALOG_SILVER_SCHEMA,
    VehicleCatalogSilverLoader,
)
from functions.vehicle_master_silver.loader import (
    SCHEMA as MASTER_SILVER_SCHEMA,
    VehicleMasterSilverLoader,
)
from schema.bronze.hvfhv import SCHEMA as HVFHV_BRONZE_SCHEMA

COLLECTED_AT = datetime(2026, 8, 13, 3, tzinfo=timezone.utc)


def _row(schema: pa.Schema) -> dict:
    values = {}
    for field in schema:
        if pa.types.is_string(field.type):
            values[field.name] = "value"
        elif pa.types.is_integer(field.type):
            values[field.name] = 2020
        elif pa.types.is_floating(field.type):
            values[field.name] = 1.0
        elif pa.types.is_timestamp(field.type):
            values[field.name] = COLLECTED_AT
        elif pa.types.is_date(field.type):
            values[field.name] = COLLECTED_AT.date()
        elif pa.types.is_list(field.type):
            values[field.name] = ["value"]
        else:
            raise AssertionError(f"테스트 값이 없는 타입: {field.type}")
    return values


def _company(root):
    return CompanySnapshotBronzeLoader(
        str(root), "2026-08-13", COLLECTED_AT
    ), {"customer": pa.table({"customer_id": ["c1"]})}


def _hvfhv(root):
    sink = pa.BufferOutputStream()
    pq.write_table(HVFHV_BRONZE_SCHEMA.empty_table(), sink)
    return (
        HvfhvBronzeLoader(str(root), "2026-08", COLLECTED_AT),
        sink.getvalue().to_pybytes(),
    )


def _specs(root):
    return VehicleSpecsBronzeLoader(str(root), COLLECTED_AT), [
        {"source": "fueleconomy.gov", "id": "1", "collected_at": COLLECTED_AT}
    ]


def _lyft(root):
    row = _row(LYFT_BRONZE_SCHEMA)
    row["city_slug"] = "new-york"
    return LyftEligibleVehiclesBronzeLoader(
        str(root), "new-york", COLLECTED_AT
    ), [row]


def _uber(root):
    row = _row(UBER_BRONZE_SCHEMA)
    row["city_slug"] = "new-york"
    return UberEligibleVehiclesBronzeLoader(
        str(root), "new-york", COLLECTED_AT
    ), [row]


def _catalog(root):
    row = _row(CATALOG_BRONZE_SCHEMA)
    row["vendor"] = "fasttrack"
    return VehicleCatalogBronzeLoader(str(root), COLLECTED_AT), [row]


@pytest.mark.parametrize(
    "factory",
    [_company, _hvfhv, _specs, _lyft, _uber, _catalog],
    ids=lambda fn: fn.__name__,
)
def test_Raw_Bronze_교체실패는_기존파일을_보존하고_tmp를_정리한다(
    factory, tmp_path, monkeypatch
):
    loader, data = factory(tmp_path)
    loader.write(data)
    originals = {path: path.read_bytes() for path in tmp_path.rglob("*.parquet")}
    attempted_sources = []

    def fail_replace(source, target):
        attempted_sources.append(source)
        raise OSError("교체 실패")

    monkeypatch.setattr(Path, "replace", fail_replace)
    for _ in range(2):
        with pytest.raises(OSError, match="교체 실패"):
            loader.write(data)

    assert len(set(attempted_sources)) == 2
    assert {path: path.read_bytes() for path in originals} == originals
    assert not [path for path in tmp_path.rglob("*") if path.suffix == ".tmp"]


def _fuel_price_silver(root):
    return (
        EiaFuelPriceSilverLoader(str(root), "2026-08"),
        [_row(FUEL_PRICE_SILVER_SCHEMA)],
    )


def _specs_silver(root):
    row = _row(SPECS_SILVER_SCHEMA)
    row.update({"source": "fueleconomy.gov", "collected_at": COLLECTED_AT})
    return VehicleSpecsSilverLoader(str(root)), [row]


def _lyft_silver(root):
    row = _row(LYFT_SILVER_SCHEMA)
    row.update({"city": "new-york", "collected_at": COLLECTED_AT})
    return LyftEligibleVehiclesSilverLoader(str(root)), [row]


def _uber_silver(root):
    row = _row(UBER_SILVER_SCHEMA)
    row.update({"city": "new-york", "collected_at": COLLECTED_AT})
    return UberEligibleVehiclesSilverLoader(str(root)), [row]


def _catalog_silver(root):
    row = _row(CATALOG_SILVER_SCHEMA)
    row.update({"vendor": "fasttrack", "collected_at": COLLECTED_AT})
    return VehicleCatalogSilverLoader(str(root)), [row]


def _master_silver(root):
    row = _row(MASTER_SILVER_SCHEMA)
    row["city"] = "new-york"
    return VehicleMasterSilverLoader(str(root), "2026-08-13"), [row]


@pytest.mark.parametrize(
    "factory",
    [
        _fuel_price_silver,
        _specs_silver,
        _lyft_silver,
        _uber_silver,
        _catalog_silver,
        _master_silver,
    ],
    ids=lambda fn: fn.__name__,
)
def test_Silver_교체실패는_기존파일을_보존하고_tmp를_정리한다(
    factory, tmp_path, monkeypatch
):
    loader, data = factory(tmp_path)
    loader.write(data)
    originals = {path: path.read_bytes() for path in tmp_path.rglob("*.parquet")}
    attempted_sources = []

    def fail_replace(source, target):
        attempted_sources.append(source)
        raise OSError("교체 실패")

    monkeypatch.setattr(Path, "replace", fail_replace)
    for _ in range(2):
        with pytest.raises(OSError, match="교체 실패"):
            loader.write(data)

    assert len(set(attempted_sources)) == 2
    assert {path: path.read_bytes() for path in originals} == originals
    assert not [path for path in tmp_path.rglob("*") if path.suffix == ".tmp"]
