"""HVFHV+taxi_id 데이터 Raw→Bronze 수집 시나리오.

1. 월별 Parquet URL 한 번만 호출해 원본 bytes와 footer 행 수를 저장
2. 같은 월 재실행은 같은 파일을 원자적으로 교체
3. 빈 응답과 잘못된 Parquet은 완료 파일을 공개하지 않음
4. latest 응답의 최종 URL에서 실제 월을 확인
5. 다른 host로 이동한 응답은 저장 전에 거부
"""

from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from main.aws_lambda.common import monthly_dataset
from functions.hvfhv_raw_to_bronze.handler import lambda_handler
from schema.bronze import MONTHLY_TAXI_TRIP_SCHEMA as SCHEMA


YEAR_MONTH = "2026-08"
API_URL = "http://source.example"
DATASET_URL = f"{API_URL}/v1/data/{YEAR_MONTH}/datasets/hvfhv_taxi_trips"
LATEST_URL = f"{API_URL}/v1/data/latest/datasets/hvfhv_taxi_trips"


def _parquet_bytes(taxi_id: str = "taxi-1") -> bytes:
    row = {
        field.name: (
            datetime(2026, 8, 1, 9)
            if pa.types.is_timestamp(field.type)
            else 1
            if pa.types.is_integer(field.type)
            else 1.0
            if pa.types.is_floating(field.type)
            else taxi_id
            if field.name == "taxi_id"
            else "x"
        )
        for field in SCHEMA
    }
    sink = pa.BufferOutputStream()
    pq.write_table(pa.Table.from_pylist([row], schema=SCHEMA), sink)
    return sink.getvalue().to_pybytes()


CONTENT = _parquet_bytes()


class Response:
    def __init__(self, *, url=DATASET_URL, content=CONTENT):
        self.url = url
        self.content = content

    def raise_for_status(self):
        return None


def _api(
    monkeypatch,
    requested: list[str] | None = None,
    *,
    content: bytes = CONTENT,
    response_url: str = DATASET_URL,
) -> None:
    def get(url, **kwargs):
        if requested is not None:
            requested.append(url)
        return Response(url=response_url, content=content)

    monkeypatch.setattr(monthly_dataset.requests, "get", get)


def _event(tmp_path) -> dict:
    return {
        "api_base_url": API_URL,
        "base_dir": str(tmp_path),
        "year": "2026",
        "month": "8",
    }


def test_HVFHV_Parquet_URL만_호출해_원본과_footer행수를_저장한다(
    tmp_path, monkeypatch
):
    requested = []
    _api(monkeypatch, requested)

    result = lambda_handler(_event(tmp_path))

    path = Path(result["locations"][0])
    assert requested == [DATASET_URL]
    assert path.read_bytes() == CONTENT
    assert path.parent.name == f"year_month={YEAR_MONTH}"
    assert path.parent.parent.name == "hvfhv"
    assert result["row_count"] == pq.ParquetFile(path).metadata.num_rows == 1
    assert set(result) == {
        "file_size_bytes",
        "locations",
        "month",
        "row_count",
        "year",
        "year_month",
    }


def test_같은월을_다시수집하면_같은파일을_원자적으로_교체한다(
    tmp_path, monkeypatch
):
    _api(monkeypatch)
    first = lambda_handler(_event(tmp_path))

    corrected = _parquet_bytes(taxi_id="taxi-2")
    _api(monkeypatch, content=corrected)
    second = lambda_handler(_event(tmp_path))

    path = Path(second["locations"][0])
    assert first["locations"] == second["locations"]
    assert path.read_bytes() == corrected
    assert len(list((tmp_path / "hvfhv").rglob("*.parquet"))) == 1
    assert not list((tmp_path / "hvfhv").rglob("*.json"))


@pytest.mark.parametrize("content", [b"", b"not parquet"], ids=["empty", "invalid"])
def test_읽을수없는_응답은_Bronze파일을_공개하지않는다(
    content, tmp_path, monkeypatch
):
    _api(monkeypatch, content=content)

    with pytest.raises(ValueError, match="비어 있습니다|Parquet이 아닙니다"):
        lambda_handler(_event(tmp_path))

    assert not list(tmp_path.rglob("*.parquet"))


def test_월을_지정하지않으면_latest의_최종URL에서_실제월을_확인한다(
    tmp_path, monkeypatch
):
    requested = []
    _api(monkeypatch, requested)

    result = lambda_handler({"api_base_url": API_URL, "base_dir": str(tmp_path)})

    assert requested == [LATEST_URL]
    assert result["year_month"] == YEAR_MONTH


def test_다른host로_이동한_응답은_저장하지않는다(tmp_path, monkeypatch):
    _api(
        monkeypatch,
        response_url=(
            f"http://other.example/v1/data/{YEAR_MONTH}/datasets/hvfhv_taxi_trips"
        ),
    )

    with pytest.raises(ValueError, match="같은 host"):
        lambda_handler(_event(tmp_path))

    assert not list(tmp_path.rglob("*.parquet"))


@pytest.mark.parametrize("event", [{"year": "2026"}, {"month": "08"}])
def test_연월은_둘다_주거나_둘다_비워야한다(event, tmp_path):
    event.update({"api_base_url": API_URL, "base_dir": str(tmp_path)})
    with pytest.raises(ValueError, match="함께"):
        lambda_handler(event)
