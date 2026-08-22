"""Loader의 파일 단위 원자적 교체를 검증합니다."""

from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pytest

from sub.aws_lambda.functions.fueleconomy_vehicle_specs_raw_to_curated.loader import (
    SCHEMA as SPECS_SILVER_SCHEMA,
    VehicleSpecsCuratedLoader,
)
from sub.aws_lambda.functions.fueleconomy_vehicle_specs_source_to_raw.loader import (
    VehicleSpecsRawLoader,
)
from sub.aws_lambda.functions.lyft_eligible_vehicles_source_to_raw.loader import (
    SCHEMA as LYFT_BRONZE_SCHEMA,
    LyftEligibleVehiclesRawLoader,
)
from sub.aws_lambda.functions.lyft_eligible_vehicles_raw_to_curated.loader import (
    SCHEMA as LYFT_SILVER_SCHEMA,
    LyftEligibleVehiclesCuratedLoader,
)
from sub.aws_lambda.functions.uber_eligible_vehicles_raw_to_curated.loader import (
    SCHEMA as UBER_SILVER_SCHEMA,
    UberEligibleVehiclesCuratedLoader,
)
from sub.aws_lambda.functions.uber_eligible_vehicles_source_to_raw.loader import (
    SCHEMA as UBER_BRONZE_SCHEMA,
    UberEligibleVehiclesRawLoader,
)
from sub.aws_lambda.functions.vehicle_catalog_source_to_raw.loader import (
    SCHEMA as CATALOG_BRONZE_SCHEMA,
    VehicleCatalogRawLoader,
)
from sub.aws_lambda.functions.vehicle_catalog_raw_to_curated.loader import (
    SCHEMA as CATALOG_SILVER_SCHEMA,
    VehicleCatalogCuratedLoader,
)
from sub.aws_lambda.functions.vehicle_master_curated_to_curated.loader import (
    SCHEMA as MASTER_SILVER_SCHEMA,
    VehicleMasterCuratedLoader,
)

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


def _specs(root):
    return VehicleSpecsRawLoader(str(root), COLLECTED_AT), [
        {"source": "fueleconomy.gov", "id": "1", "collected_at": COLLECTED_AT}
    ]


def _lyft(root):
    row = _row(LYFT_BRONZE_SCHEMA)
    row["city_slug"] = "new-york"
    return LyftEligibleVehiclesRawLoader(
        str(root), "new-york", COLLECTED_AT
    ), [row]


def _uber(root):
    row = _row(UBER_BRONZE_SCHEMA)
    row["city_slug"] = "new-york"
    return UberEligibleVehiclesRawLoader(
        str(root), "new-york", COLLECTED_AT
    ), [row]


def _catalog(root):
    row = _row(CATALOG_BRONZE_SCHEMA)
    row["vendor"] = "fasttrack"
    return VehicleCatalogRawLoader(str(root), COLLECTED_AT), [row]


@pytest.mark.parametrize(
    "factory",
    [_specs, _lyft, _uber, _catalog],
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


def _specs_silver(root):
    row = _row(SPECS_SILVER_SCHEMA)
    row.update({"source": "fueleconomy.gov", "collected_at": COLLECTED_AT})
    return VehicleSpecsCuratedLoader(str(root)), [row]


def _lyft_silver(root):
    row = _row(LYFT_SILVER_SCHEMA)
    row.update({"city": "new-york", "collected_at": COLLECTED_AT})
    return LyftEligibleVehiclesCuratedLoader(str(root)), [row]


def _uber_silver(root):
    row = _row(UBER_SILVER_SCHEMA)
    row.update({"city": "new-york", "collected_at": COLLECTED_AT})
    return UberEligibleVehiclesCuratedLoader(str(root)), [row]


def _catalog_silver(root):
    row = _row(CATALOG_SILVER_SCHEMA)
    row.update({"vendor": "fasttrack", "collected_at": COLLECTED_AT})
    return VehicleCatalogCuratedLoader(str(root)), [row]


def _master_silver(root):
    row = _row(MASTER_SILVER_SCHEMA)
    row["city"] = "new-york"
    return VehicleMasterCuratedLoader(str(root), "2026-08-13"), [row]


@pytest.mark.parametrize(
    "factory",
    [
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
