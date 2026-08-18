"""HVFHV+taxi_id 데이터 Raw→Bronze 수집 시나리오.

1. 요청한 HVFHV 한 파일만 원본 bytes 그대로 월 파티션에 저장
2. 같은 월 재실행은 파일을 추가하지 않고 기존 결과 재사용
3. checksum/manifest 계약 위반은 완료 파일을 공개하지 않음
"""

import hashlib
import json
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from shared.lambda_runtime.common import monthly_dataset
from functions.hvfhv_raw_to_bronze.handler import lambda_handler
from schema.bronze.hvfhv import SCHEMA


YEAR_MONTH = "2026-08"
API_URL = "http://source.example"


def _parquet_bytes() -> bytes:
    row = {
        field.name: (
            datetime(2026, 8, 1, 9)
            if pa.types.is_timestamp(field.type)
            else 1
            if pa.types.is_integer(field.type)
            else 1.0
            if pa.types.is_floating(field.type)
            else "taxi-1"
            if field.name == "taxi_id"
            else "x"
        )
        for field in SCHEMA
    }
    sink = pa.BufferOutputStream()
    pq.write_table(pa.Table.from_pylist([row], schema=SCHEMA), sink)
    return sink.getvalue().to_pybytes()


CONTENT = _parquet_bytes()


def _manifest() -> dict:
    return {
        "year_month": YEAR_MONTH,
        "datasets": {
            "hvfhv_taxi_trips": {
                "row_count": 1,
                "sha256": hashlib.sha256(CONTENT).hexdigest(),
                "download_url": f"/v1/data/{YEAR_MONTH}/datasets/hvfhv_taxi_trips",
            },
            "driver_vehicle_leases": {
                "row_count": 1,
                "sha256": "0" * 64,
                "download_url": f"/v1/data/{YEAR_MONTH}/datasets/driver_vehicle_leases",
            },
        },
    }


class Response:
    def __init__(self, *, payload=None, content=b""):
        self._payload = payload
        self.content = content

    def json(self):
        return self._payload

    def raise_for_status(self):
        return None


def _api(monkeypatch, manifest: dict, requested: list[str] | None = None) -> None:
    responses = {
        f"{API_URL}/v1/data/{YEAR_MONTH}": Response(payload=manifest),
        f"{API_URL}/v1/data/latest": Response(payload=manifest),
        f"{API_URL}/v1/data/{YEAR_MONTH}/datasets/hvfhv_taxi_trips": Response(
            content=CONTENT
        ),
    }

    def get(url, **kwargs):
        if requested is not None:
            requested.append(url)
        return responses[url]

    monkeypatch.setattr(monthly_dataset.requests, "get", get)


def _event(tmp_path) -> dict:
    return {
        "api_base_url": API_URL,
        "base_dir": str(tmp_path),
        "year": "2026",
        "month": "8",
    }


def test_HVFHV한파일만_원본bytes그대로_Bronze에_저장한다(tmp_path, monkeypatch):
    requested = []
    _api(monkeypatch, _manifest(), requested)

    result = lambda_handler(_event(tmp_path))

    path = Path(result["locations"][0])
    assert path.read_bytes() == CONTENT
    assert path.name == "data.parquet"
    assert path.parent.name == f"year_month={YEAR_MONTH}"
    assert path.parent.parent.name == "hvfhv"
    marker = json.loads(Path(result["marker_location"]).read_text(encoding="utf-8"))
    assert set(marker) == {"dataset", "row_count", "sha256", "year_month"}
    assert all("driver_vehicle_leases" not in url for url in requested)


def test_같은월을_다시수집해도_파일이_추가되지_않는다(tmp_path, monkeypatch):
    _api(monkeypatch, _manifest())
    first = lambda_handler(_event(tmp_path))
    _api(monkeypatch, _manifest())
    second = lambda_handler(_event(tmp_path))

    assert first["locations"] == second["locations"]
    assert second["already_collected"] is True
    assert len(list((tmp_path / "hvfhv").rglob("*.parquet"))) == 1


def test_checksum이_다르면_Bronze파일과_marker를_공개하지_않는다(
    tmp_path, monkeypatch
):
    manifest = _manifest()
    manifest["datasets"]["hvfhv_taxi_trips"]["sha256"] = "0" * 64
    _api(monkeypatch, manifest)

    with pytest.raises(ValueError, match="checksum"):
        lambda_handler(_event(tmp_path))

    assert not list(tmp_path.rglob("*.parquet"))
    assert not list(tmp_path.rglob("*.json"))


def test_월을_지정하지_않으면_latest_데이터를_수집한다(tmp_path, monkeypatch):
    _api(monkeypatch, _manifest())

    result = lambda_handler({"api_base_url": API_URL, "base_dir": str(tmp_path)})

    assert result["year_month"] == YEAR_MONTH


@pytest.mark.parametrize("event", [{"year": "2026"}, {"month": "08"}])
def test_연월은_둘다_주거나_둘다_비워야한다(event, tmp_path):
    event.update({"api_base_url": API_URL, "base_dir": str(tmp_path)})
    with pytest.raises(ValueError, match="함께"):
        lambda_handler(event)
