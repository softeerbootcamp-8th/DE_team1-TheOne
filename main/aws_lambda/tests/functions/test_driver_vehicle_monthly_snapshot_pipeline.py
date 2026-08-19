"""기사 차량 월별 스냅샷 Raw→Bronze 수집 시나리오.

1. 기사 차량 스냅샷 한 파일만 원본 그대로 저장
2. 같은 월 재실행은 중복 파일을 만들지 않음
3. 필수 dataset·checksum 위반은 적재 전에 실패
"""

import hashlib
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from main.aws_lambda.common import monthly_dataset
from functions.driver_vehicle_monthly_snapshot_raw_to_bronze.handler import lambda_handler


YEAR_MONTH = "2026-08"
API_URL = "http://source.example"
ROWS = [
    {
        "snapshot_month": YEAR_MONTH,
        "driver_id": "driver-1",
        "taxi_id": "taxi-1",
        "vehicle_model_id": "model-1",
        "manufacturer": "KIA",
        "model_name": "SPORTAGE",
        "fuel_type": "GAS",
        "comfort_eligible": True,
        "weekly_lease_fee": 350.0,
        "snapshot_created_at": datetime(2026, 8, 1),
    }
]


def _parquet_bytes() -> bytes:
    sink = pa.BufferOutputStream()
    pq.write_table(pa.Table.from_pylist(ROWS), sink)
    return sink.getvalue().to_pybytes()


CONTENT = _parquet_bytes()


def _manifest() -> dict:
    return {
        "year_month": YEAR_MONTH,
        "datasets": {
            "driver_vehicle_monthly_snapshot": {
                "row_count": 1,
                "sha256": hashlib.sha256(CONTENT).hexdigest(),
                "download_url": (
                    f"/v1/data/{YEAR_MONTH}/datasets/driver_vehicle_monthly_snapshot"
                ),
            },
            "hvfhv_taxi_trips": {
                "row_count": 1,
                "sha256": "0" * 64,
                "download_url": f"/v1/data/{YEAR_MONTH}/datasets/hvfhv_taxi_trips",
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


def _api(monkeypatch, manifest: dict, requested: list[str] | None = None):
    responses = {
        f"{API_URL}/v1/data/{YEAR_MONTH}": Response(payload=manifest),
        f"{API_URL}/v1/data/{YEAR_MONTH}/datasets/driver_vehicle_monthly_snapshot": Response(
            content=CONTENT
        ),
    }

    def get(url, **kwargs):
        if requested is not None:
            requested.append(url)
        return responses[url]

    monkeypatch.setattr(monthly_dataset.requests, "get", get)


def _event(tmp_path):
    return {
        "api_base_url": API_URL,
        "base_dir": str(tmp_path),
        "year": "2026",
        "month": "8",
    }


def test_기사차량스냅샷만_원본bytes그대로_Bronze에_저장한다(tmp_path, monkeypatch):
    requested = []
    _api(monkeypatch, _manifest(), requested)

    result = lambda_handler(_event(tmp_path))

    path = Path(result["locations"][0])
    assert path.read_bytes() == CONTENT
    assert path.parent.parent.name == "driver_vehicle_monthly_snapshot"
    assert all("hvfhv_taxi_trips" not in url for url in requested)


def test_같은월을_다시수집해도_중복파일이_생기지않는다(tmp_path, monkeypatch):
    _api(monkeypatch, _manifest())
    first = lambda_handler(_event(tmp_path))
    _api(monkeypatch, _manifest())
    second = lambda_handler(_event(tmp_path))

    assert first["locations"] == second["locations"]
    assert second["already_collected"] is True
    assert len(list(tmp_path.rglob("*.parquet"))) == 1


def test_manifest에_기사차량스냅샷dataset이_없으면_다운로드하지않는다(
    tmp_path, monkeypatch
):
    manifest = _manifest()
    del manifest["datasets"]["driver_vehicle_monthly_snapshot"]
    requested = []

    def get(url, **kwargs):
        requested.append(url)
        return Response(payload=manifest)

    monkeypatch.setattr(monthly_dataset.requests, "get", get)
    with pytest.raises(ValueError, match="필수 dataset"):
        lambda_handler(_event(tmp_path))

    assert requested == [f"{API_URL}/v1/data/{YEAR_MONTH}"]


def test_checksum이_다르면_완료파일을_공개하지않는다(tmp_path, monkeypatch):
    manifest = _manifest()
    manifest["datasets"]["driver_vehicle_monthly_snapshot"]["sha256"] = "0" * 64
    _api(monkeypatch, manifest)

    with pytest.raises(ValueError, match="checksum"):
        lambda_handler(_event(tmp_path))

    assert not list(tmp_path.rglob("*.parquet"))
