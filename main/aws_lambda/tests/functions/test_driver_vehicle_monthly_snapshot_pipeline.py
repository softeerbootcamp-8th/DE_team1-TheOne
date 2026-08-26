"""기사 차량 월별 스냅샷 Raw→Bronze 수집 시나리오.

1. 기사 차량 스냅샷 Parquet URL만 호출해 원본 그대로 저장
2. 원천 행 수 없이 받은 Parquet footer에서 행 수 계산
3. service_area가 로컬·S3 Bronze 경로의 year_month 바로 위에 들어감
"""

import hashlib
import json
from datetime import date, datetime, timezone
from pathlib import Path

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from moto import mock_aws

from main.aws_lambda.common import monthly_dataset
from functions.driver_vehicle_monthly_snapshot_raw_to_bronze.handler import lambda_handler


YEAR_MONTH = "2026-08"
API_URL = "http://source.example"
DATASET_URL = f"{API_URL}/v1/data/{YEAR_MONTH}/datasets/driver_vehicle_monthly_snapshot"
S3_BUCKET = "test-de-theone"
S3_REGION = "ap-northeast-2"
COLLECTED_AT = datetime(2026, 8, 20, 10, 15, 31, 123456, tzinfo=timezone.utc)
ETAG = '"snapshot-etag"'
LAST_MODIFIED = "Thu, 20 Aug 2026 10:00:00 GMT"
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
    headers = {"ETag": ETAG, "Last-Modified": LAST_MODIFIED}

    def raise_for_status(self):
        return None


def _api(monkeypatch, requested: list[tuple[str, dict | None]]) -> None:
    def get(url, **kwargs):
        params = kwargs.get("params")
        requested.append((url, params))
        response = Response()
        if params:
            response.url = f"{url}?service_area={params['service_area']}"
        return response

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
            "service_area": "TX",
        }
    )

    path = Path(result["locations"][0])
    assert requested == [(DATASET_URL, {"service_area": "TX"})]
    assert path.read_bytes() == CONTENT
    assert path.name == "data.parquet"
    assert path.parent.name == "collected_at=20260820T101531123456Z"
    assert path.parent.parent.name == "year_month=2026-08"
    assert path.parent.parent.parent.name == "service_area=TX"
    assert path.parent.parent.parent.parent.name == "driver_vehicle_monthly_snapshot"
    assert result["collected_at"] == "2026-08-20T10:15:31.123456Z"
    assert result["row_count"] == pq.ParquetFile(path).metadata.num_rows == 2
    assert result["source_changed"] is True
    assert "sha256" not in result and "marker_location" not in result
    manifest = json.loads((path.parent / "manifest.json").read_text())
    assert manifest["sha256"] == hashlib.sha256(CONTENT).hexdigest()
    assert manifest["source_etag"] == ETAG
    assert manifest["service_area"] == "TX"


def test_service_area를_S3_Bronze_경로에_적용한다(monkeypatch):
    requested = []
    _api(monkeypatch, requested)
    monkeypatch.setattr(monthly_dataset, "_utc_now", lambda: COLLECTED_AT, raising=False)

    with mock_aws():
        client = boto3.client("s3", region_name=S3_REGION)
        client.create_bucket(
            Bucket=S3_BUCKET,
            CreateBucketConfiguration={"LocationConstraint": S3_REGION},
        )
        result = lambda_handler(
            {
                "api_base_url": API_URL,
                "storage": "s3",
                "bucket": S3_BUCKET,
                "year": "2026",
                "month": "8",
                "service_area": "TX",
            }
        )

        key = (
            "bronze/driver_vehicle_monthly_snapshot/service_area=TX/"
            "year_month=2026-08/collected_at=20260820T101531123456Z/data.parquet"
        )
        assert result["locations"] == [f"s3://{S3_BUCKET}/{key}"]
        assert client.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read() == CONTENT
        manifest_key = key.rsplit("/", 1)[0] + "/manifest.json"
        manifest = json.loads(
            client.get_object(Bucket=S3_BUCKET, Key=manifest_key)["Body"].read()
        )
        assert manifest["sha256"] == hashlib.sha256(CONTENT).hexdigest()
        assert manifest["source_etag"] == ETAG
