"""Gas Price 원문 스냅샷 -> Bronze -> Silver 파이프라인 시나리오.

1. Handler가 HTML 스냅샷을 먼저 저장하고 Bronze JSON에서 원문 경로를 참조
2. HTML 파싱 실패 후에도 원문 스냅샷은 보존
3. 경계 분리 후에도 기존 Bronze -> Silver 변환 결과 유지
"""

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from functions.common import gas_price_layout as layout
from functions.gas_price_bronze_to_silver.handler import lambda_handler as to_silver
from functions.gas_price_bronze_to_silver.loader import SCHEMA
from functions.gas_price_raw_to_bronze import extractor as raw_extractor
from functions.gas_price_raw_to_bronze.extractor import PAGE_URL, parse
from functions.gas_price_raw_to_bronze.handler import lambda_handler as to_bronze
from functions.gas_price_raw_to_bronze.loader import GasPriceBronzeLoader

RAW_ROW = {
    "state": "NY",
    "fuel_type": "regular",
    "price_raw": "$3.210",
    "price_date_raw": "8/8/26",
    "source_url": PAGE_URL,
}
COLLECTED_AT = datetime(2026, 8, 9, 12, 0, tzinfo=timezone.utc)


def write_bronze(bronze_dir: Path, row: dict, collected_at: datetime) -> Path:
    result = GasPriceBronzeLoader(str(bronze_dir), collected_at).write(row)
    return Path(result.location)


def read_silver(path: Path) -> list[dict]:
    return pq.ParquetFile(path).read().to_pylist()


def test_extract는_가격과_기준일을_문자열_원문으로_유지한다():
    html = """
    <main id="maincontent">
      <div class="map-badges">
        <div class="average-price--blue">
          <p class="numb">$3.210<i class="fa"></i></p>
          <span>Price as of<br>8/8/26</span>
        </div>
      </div>
    </main>
    """

    assert parse(html) == RAW_ROW


def test_raw_to_bronze는_수집일별_json에_원문을_저장한다(tmp_path):
    result = GasPriceBronzeLoader(str(tmp_path), COLLECTED_AT).write(RAW_ROW)
    path = Path(result.location)

    assert path == layout.bronze_file(str(tmp_path), "2026-08-09")
    assert json.loads(path.read_text(encoding="utf-8")) == {
        **RAW_ROW,
        "collected_at": COLLECTED_AT.isoformat(),
    }
    assert result.row_count == 1


def test_raw_to_bronze_handler가_DAG에_필요한_응답을_반환한다(
    tmp_path, monkeypatch
):
    class Response:
        text = """
        <main id="maincontent">
          <div class="map-badges">
            <div class="average-price--blue">
              <p class="numb">$3.210</p>
              <span>Price as of 8/8/26</span>
            </div>
          </div>
        </main>
        """

        @staticmethod
        def raise_for_status():
            return None

    monkeypatch.setattr(
        raw_extractor.requests,
        "get",
        lambda *args, **kwargs: Response(),
    )

    result = to_bronze({"base_dir": str(tmp_path)})

    assert result["row_count"] == 1
    assert result["state"] == "NY"
    assert result["fuel_type"] == "regular"
    assert result["price_date"] == "2026-08-08"
    bronze_path = Path(result["locations"][0])
    record = json.loads(bronze_path.read_text(encoding="utf-8"))
    snapshot_path = Path(record["source_snapshot_path"])

    assert bronze_path.exists()
    assert snapshot_path == next(tmp_path.glob("gas_price/raw/*/source.html"))
    assert snapshot_path.read_text(encoding="utf-8") == Response.text


def test_HTML_파싱이_실패해도_원문_스냅샷은_남는다(tmp_path, monkeypatch):
    class Response:
        text = "<html><body>페이지 구조 변경</body></html>"

        @staticmethod
        def raise_for_status():
            return None

    monkeypatch.setattr(
        raw_extractor.requests,
        "get",
        lambda *args, **kwargs: Response(),
    )

    with pytest.raises(RuntimeError, match="페이지 구조 변경 의심"):
        to_bronze({"base_dir": str(tmp_path)})

    snapshots = list(tmp_path.glob("gas_price/raw/*/source.html"))
    assert len(snapshots) == 1
    assert snapshots[0].read_text(encoding="utf-8") == Response.text


def test_bronze_원문을_월별_silver_parquet으로_변환한다(tmp_path):
    bronze_dir, silver_dir = tmp_path / "bronze", tmp_path / "silver"
    first_path = write_bronze(bronze_dir, RAW_ROW, COLLECTED_AT)
    second_at = COLLECTED_AT + timedelta(days=1)
    second_row = {
        **RAW_ROW,
        "price_raw": "$3.250",
        "price_date_raw": "8/9/26",
    }
    second_path = write_bronze(bronze_dir, second_row, second_at)

    result = to_silver(
        {
            "collected_month": "2026-08",
            "bronze_dir": str(bronze_dir),
            "silver_dir": str(silver_dir),
        }
    )
    silver_path = layout.silver_file(str(silver_dir), "2026-08")
    table = pq.ParquetFile(silver_path).read()

    assert result == {
        "row_count": 2,
        "locations": [str(silver_path)],
        "collected_month": "2026-08",
    }
    assert table.schema == SCHEMA
    assert table.to_pylist() == [
        {
            "state": "NY",
            "fuel_type": "regular",
            "price_usd_per_gallon": 3.21,
            "price_date": date(2026, 8, 8),
            "source_url": PAGE_URL,
            "collected_at": COLLECTED_AT,
            "bronze_path": str(first_path),
        },
        {
            "state": "NY",
            "fuel_type": "regular",
            "price_usd_per_gallon": 3.25,
            "price_date": date(2026, 8, 9),
            "source_url": PAGE_URL,
            "collected_at": second_at,
            "bronze_path": str(second_path),
        },
    ]


def test_같은_가격일은_최신_수집본으로_월파일을_덮어쓴다(tmp_path):
    bronze_dir, silver_dir = tmp_path / "bronze", tmp_path / "silver"
    write_bronze(bronze_dir, RAW_ROW, COLLECTED_AT)
    first = to_silver(
        {
            "collected_month": "2026-08",
            "bronze_dir": str(bronze_dir),
            "silver_dir": str(silver_dir),
        }
    )

    write_bronze(
        bronze_dir,
        {**RAW_ROW, "price_raw": "$3.300"},
        COLLECTED_AT + timedelta(days=1),
    )
    second = to_silver(
        {
            "collected_month": "2026-08",
            "bronze_dir": str(bronze_dir),
            "silver_dir": str(silver_dir),
        }
    )

    silver_path = Path(second["locations"][0])
    rows = read_silver(silver_path)
    assert first["locations"] == second["locations"]
    assert len(list(silver_path.parent.glob("*.parquet"))) == 1
    assert len(rows) == 1
    assert rows[0]["price_usd_per_gallon"] == 3.3
    assert rows[0]["collected_at"] == COLLECTED_AT + timedelta(days=1)


def test_collected_month_형식이_잘못되면_실패한다(tmp_path):
    with pytest.raises(ValueError, match="YYYY-MM"):
        to_silver(
            {
                "collected_month": "2026-8",
                "bronze_dir": str(tmp_path),
                "silver_dir": str(tmp_path),
            }
        )


def test_대상_월의_bronze가_없으면_실패한다(tmp_path):
    with pytest.raises(FileNotFoundError, match="Bronze JSON 파일이 없습니다"):
        to_silver(
            {
                "collected_month": "2026-08",
                "bronze_dir": str(tmp_path / "bronze"),
                "silver_dir": str(tmp_path / "silver"),
            }
        )
