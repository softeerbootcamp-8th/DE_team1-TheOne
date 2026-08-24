"""HVFHV+taxi_id 데이터 Raw→Bronze 수집 시나리오.

1. 월별 Parquet URL 한 번만 호출해 원본 bytes와 footer 행 수를 저장
2. 같은 원본 재수집은 최신 파일을 재사용하고 변경된 원본만 새 파일로 보존
3. 빈 응답과 잘못된 Parquet은 완료 파일을 공개하지 않음
4. latest 응답의 최종 URL에서 실제 월을 확인
5. 다른 host로 이동한 응답은 저장 전에 거부
6. service_area를 지정하면 데이터셋과 월 사이의 지역 경로에 저장
"""

from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from main.aws_lambda.common import monthly_dataset
from functions.monthly_taxi_trip_raw_to_bronze.handler import lambda_handler
from schema.bronze import MONTHLY_TAXI_TRIP_SCHEMA as SCHEMA


YEAR_MONTH = "2026-08"
API_URL = "http://source.example"
DATASET_URL = f"{API_URL}/v1/data/{YEAR_MONTH}/datasets/monthly_taxi_trip"
LATEST_URL = f"{API_URL}/v1/data/latest/datasets/monthly_taxi_trip"
FIRST_COLLECTED_AT = datetime(
    2026, 8, 20, 10, 15, 30, 123456, tzinfo=timezone.utc
)
SECOND_COLLECTED_AT = datetime(
    2026, 8, 20, 11, 22, 5, 654321, tzinfo=timezone.utc
)


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
        "service_area": "NYC",
    }


def _clock(monkeypatch, *moments: datetime) -> None:
    values = iter(moments)
    monkeypatch.setattr(monthly_dataset, "_utc_now", lambda: next(values), raising=False)


def test_HVFHV_Parquet_URL만_호출해_원본과_footer행수를_저장한다(
    tmp_path, monkeypatch
):
    requested = []
    _api(monkeypatch, requested)
    _clock(monkeypatch, FIRST_COLLECTED_AT)

    result = lambda_handler(_event(tmp_path))

    path = Path(result["locations"][0])
    assert requested == [DATASET_URL]
    assert path.read_bytes() == CONTENT
    assert path.name == "data.parquet"
    assert path.parent.name == "collected_at=20260820T101530123456Z"
    assert path.parent.parent.name == f"year_month={YEAR_MONTH}"
    assert path.parent.parent.parent.name == "service_area=NYC"
    assert path.parent.parent.parent.parent.name == "monthly_taxi_trip"
    assert result["collected_at"] == "2026-08-20T10:15:30.123456Z"
    assert result["row_count"] == pq.ParquetFile(path).metadata.num_rows == 1
    assert result["source_changed"] is True
    assert set(result) == {
        "file_size_bytes",
        "collected_at",
        "locations",
        "month",
        "row_count",
        "source_changed",
        "year",
        "year_month",
    }


def test_service_area를_데이터셋과_월사이의_Bronze경로에_저장한다(
    tmp_path, monkeypatch
):
    calls = []

    def get(url, **kwargs):
        calls.append((url, kwargs))
        return Response(url=f"{DATASET_URL}?service_area=TX")

    monkeypatch.setattr(monthly_dataset.requests, "get", get)
    _clock(monkeypatch, FIRST_COLLECTED_AT)
    event = {**_event(tmp_path), "service_area": "TX"}

    result = lambda_handler(event)

    path = Path(result["locations"][0])
    assert calls[0][1]["params"] == {"service_area": "TX"}
    assert path.parent.parent.parent.name == "service_area=TX"
    assert path.parent.parent.parent.parent.name == "monthly_taxi_trip"


def test_같은월의_원본이_변경되면_수집시각파일을_추가해_이력을_보존한다(
    tmp_path, monkeypatch
):
    _api(monkeypatch)
    _clock(monkeypatch, FIRST_COLLECTED_AT, SECOND_COLLECTED_AT)
    first = lambda_handler(_event(tmp_path))

    corrected = _parquet_bytes(taxi_id="taxi-2")
    _api(monkeypatch, content=corrected)
    second = lambda_handler(_event(tmp_path))

    first_path = Path(first["locations"][0])
    second_path = Path(second["locations"][0])
    assert first_path != second_path
    assert first_path.read_bytes() == CONTENT
    assert second_path.read_bytes() == corrected
    assert len(list((tmp_path / "monthly_taxi_trip").rglob("*.parquet"))) == 2
    assert not list((tmp_path / "monthly_taxi_trip").rglob("*.json"))


def test_같은원본을_다시수집하면_최신파일을_재사용한다(tmp_path, monkeypatch):
    _api(monkeypatch)
    _clock(monkeypatch, FIRST_COLLECTED_AT, SECOND_COLLECTED_AT)

    first = lambda_handler(_event(tmp_path))
    second = lambda_handler(_event(tmp_path))

    assert second["locations"] == first["locations"]
    assert second["collected_at"] == first["collected_at"]
    assert first["source_changed"] is True
    assert second["source_changed"] is False
    assert len(list((tmp_path / "monthly_taxi_trip").rglob("*.parquet"))) == 1


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

    result = lambda_handler(
        {
            "api_base_url": API_URL,
            "base_dir": str(tmp_path),
            "service_area": "NYC",
        }
    )

    assert requested == [LATEST_URL]
    assert result["year_month"] == YEAR_MONTH


def test_다른host로_이동한_응답은_저장하지않는다(tmp_path, monkeypatch):
    _api(
        monkeypatch,
        response_url=(
            f"http://other.example/v1/data/{YEAR_MONTH}/datasets/monthly_taxi_trip"
        ),
    )

    with pytest.raises(ValueError, match="같은 host"):
        lambda_handler(_event(tmp_path))

    assert not list(tmp_path.rglob("*.parquet"))


@pytest.mark.parametrize("event", [{"year": "2026"}, {"month": "08"}])
def test_연월은_둘다_주거나_둘다_비워야한다(event, tmp_path):
    event.update(
        {"api_base_url": API_URL, "base_dir": str(tmp_path), "service_area": "NYC"}
    )
    with pytest.raises(ValueError, match="함께"):
        lambda_handler(event)
