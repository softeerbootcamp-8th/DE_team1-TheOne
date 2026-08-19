"""보유 차량 데이터 Raw→Bronze 수집 시나리오.

1. 보유 차량 Parquet URL만 호출해 원본 그대로 저장
2. 원천 행 수 없이 받은 Parquet footer에서 행 수 계산
"""

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from main.aws_lambda.common import monthly_dataset
from functions.lease_vehicle_inventory_raw_to_bronze.handler import lambda_handler


YEAR_MONTH = "2026-08"
API_URL = "http://source.example"
DATASET_URL = f"{API_URL}/v1/data/{YEAR_MONTH}/datasets/lease_vehicle_inventory"
ROWS = [
    {
        "vehicle_model_id": f"model-{index}",
        "manufacturer": "KIA",
        "model_name": "SPORTAGE",
        "model_year": 2023,
        "fuel_type": "GAS",
        "fuel_efficiency": 28.5,
        "comfort_eligible": True,
        "extra_comfort_eligible": False,
        "weekly_price_usd": 350.0,
        "image_url": "http://images.example/kia-sportage.png",
        "stock": 12,
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


def test_보유차량Parquet만_직접받아_footer행수와함께_Bronze에_저장한다(
    tmp_path, monkeypatch
):
    requested = []
    _api(monkeypatch, requested)

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
    assert path.parent.parent.name == "lease_vehicle_inventory"
    assert result["row_count"] == pq.ParquetFile(path).metadata.num_rows == 2
    assert "sha256" not in result and "marker_location" not in result
