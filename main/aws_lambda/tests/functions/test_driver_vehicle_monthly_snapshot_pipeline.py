"""기사 차량 월별 스냅샷 Raw→Bronze 수집 시나리오.

1. 기사 차량 스냅샷 Parquet URL만 호출해 원본 그대로 저장
2. 원천 행 수 없이 받은 Parquet footer에서 행 수 계산
"""

from datetime import date, datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from main.aws_lambda.common import monthly_dataset
from functions.driver_vehicle_monthly_snapshot_raw_to_bronze.handler import lambda_handler


YEAR_MONTH = "2026-08"
API_URL = "http://source.example"
DATASET_URL = f"{API_URL}/v1/data/{YEAR_MONTH}/datasets/driver_vehicle_monthly_snapshot"
COLLECTED_AT = datetime(2026, 8, 20, 10, 15, 31, 123456, tzinfo=timezone.utc)
ROWS = [
    {
        "snapshot_month": YEAR_MONTH,
        "driver_id": f"driver-{index}",
        "taxi_id": f"taxi-{index}",
        "vehicle_model_id": "model-1",
        "manufacturer": "KIA",
        "model_name": "SPORTAGE",
        "fuel_type": "GAS",
        "comfort_eligible": True,
        "extra_comfort_eligible": False,
        "weekly_lease_fee": 350.0,
        "join_date": date(2024, 1, 1),
        "exit_date": None,
        "experience_years": 5,
        "vehicle_since": date(2025, 1, 1),
        "snapshot_created_at": datetime(2026, 8, 1),
    }
    for index in range(2)
]


def _parquet_bytes() -> bytes:
    sink = pa.BufferOutputStream()
    pq.write_table(pa.Table.from_pylist(ROWS), sink)
    return sink.getvalue().to_pybytes()


CONTENT = _parquet_bytes()


class Response:
    url = DATASET_URL
    content = CONTENT

    def raise_for_status(self):
        return None


def _api(monkeypatch, requested: list[str]) -> None:
    def get(url, **kwargs):
        requested.append(url)
        return Response()

    monkeypatch.setattr(monthly_dataset.requests, "get", get)


def test_기사차량스냅샷Parquet만_직접받아_footer행수와함께_Bronze에_저장한다(
    tmp_path, monkeypatch
):
    requested = []
    _api(monkeypatch, requested)
    monkeypatch.setattr(monthly_dataset, "_utc_now", lambda: COLLECTED_AT, raising=False)

    result = lambda_handler(
        {
            "api_base_url": API_URL,
            "base_dir": str(tmp_path),
            "year": "2026",
            "month": "8",
        }
    )

    path = Path(result["locations"][0])
    assert requested == [DATASET_URL]
    assert path.read_bytes() == CONTENT
    assert path.name == "data.parquet"
    assert path.parent.name == "collected_at=20260820T101531123456Z"
    assert path.parent.parent.parent.name == "driver_vehicle_monthly_snapshot"
    assert result["collected_at"] == "2026-08-20T10:15:31.123456Z"
    assert result["row_count"] == pq.ParquetFile(path).metadata.num_rows == 2
    assert result["source_changed"] is True
    assert "sha256" not in result and "marker_location" not in result
