"""Gas Price Raw -> Bronze와 기존 Bronze -> Silver 동작을 검증합니다."""

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from functions.common import gas_price_layout as layout
from functions.gas_price_bronze_to_silver.handler import lambda_handler as to_silver
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
SILVER_INPUT_ROW = {
    "state": "NY",
    "fuel_type": "regular",
    "price_usd_per_gallon": 3.21,
    "price_date": date(2026, 8, 8),
    "source_url": PAGE_URL,
}
COLLECTED_AT = datetime(2026, 8, 9, 12, 0, tzinfo=timezone.utc)


def write_silver_input(bronze_dir: Path, row: dict, collected_at: datetime) -> str:
    """Bronze -> Silver 전환 전의 기존 정규화 스키마 fixture를 씁니다."""
    path = (
        layout.bronze_partition(str(bronze_dir), f"{collected_at:%Y-%m-%d}")
        / f"{row['price_date']:%Y-%m-%d}.json"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                **row,
                "price_date": row["price_date"].isoformat(),
                "collected_at": collected_at.isoformat(),
            }
        )
        + "\n",
        encoding="utf-8",
    )
    return str(path)


def silver_json(result: dict, price_date: date) -> Path:
    return (
        Path(result["locations"][0])
        / f"price_date={price_date.isoformat()}"
        / "gas_price.json"
    )


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
    assert Path(result["locations"][0]).exists()


def test_bronze_to_silver(tmp_path):
    bronze_dir, silver_dir = tmp_path / "bronze", tmp_path / "silver"

    location = write_silver_input(bronze_dir, SILVER_INPUT_ROW, COLLECTED_AT)
    # Bronze 를 쓰는 쪽과 Silver 가 읽는 쪽이 같은 데이터셋 경로를 봐야 합니다.
    assert Path(location).parent.parent == layout.dataset_path(str(bronze_dir))

    result = to_silver(
        event={
            "collected_date": f"{COLLECTED_AT:%Y-%m-%d}",
            "bronze_dir": str(bronze_dir),
            "silver_dir": str(silver_dir),
            "expect_price_date": SILVER_INPUT_ROW["price_date"].isoformat(),
        }
    )

    assert result["row_count"] == 1
    assert silver_json(result, SILVER_INPUT_ROW["price_date"]).exists()


def test_과거_파티션이_깨져도_당일_처리는_성공한다(tmp_path):
    """이 이슈의 핵심 — 과거의 오류가 오늘 실행을 막지 않아야 합니다."""
    bronze_dir, silver_dir = tmp_path / "bronze", tmp_path / "silver"

    past_collected_at = COLLECTED_AT - timedelta(days=6)
    past_location = write_silver_input(
        bronze_dir,
        {**SILVER_INPUT_ROW, "price_date": date(2026, 8, 2)},
        past_collected_at,
    )
    Path(past_location).write_text("{망가진 JSON", encoding="utf-8")

    write_silver_input(bronze_dir, SILVER_INPUT_ROW, COLLECTED_AT)

    result = to_silver(
        event={
            "collected_date": f"{COLLECTED_AT:%Y-%m-%d}",
            "bronze_dir": str(bronze_dir),
            "silver_dir": str(silver_dir),
            "expect_price_date": SILVER_INPUT_ROW["price_date"].isoformat(),
        }
    )

    assert result["row_count"] == 1
    assert result["processed_count"] == 1


def test_당일_파일이_깨지면_실패한다(tmp_path):
    bronze_dir, silver_dir = tmp_path / "bronze", tmp_path / "silver"

    location = write_silver_input(bronze_dir, SILVER_INPUT_ROW, COLLECTED_AT)
    Path(location).write_text("{망가진 JSON", encoding="utf-8")

    with pytest.raises(RuntimeError):
        to_silver(
            event={
                "collected_date": f"{COLLECTED_AT:%Y-%m-%d}",
                "bronze_dir": str(bronze_dir),
                "silver_dir": str(silver_dir),
            }
        )


def test_백필은_그_달_전체를_다시_정제한다(tmp_path):
    bronze_dir, silver_dir = tmp_path / "bronze", tmp_path / "silver"

    write_silver_input(
        bronze_dir,
        {**SILVER_INPUT_ROW, "price_date": date(2026, 8, 2)},
        COLLECTED_AT - timedelta(days=6),
    )
    write_silver_input(bronze_dir, SILVER_INPUT_ROW, COLLECTED_AT)

    result = to_silver(
        event={
            "collected_month": f"{COLLECTED_AT:%Y-%m}",
            "bronze_dir": str(bronze_dir),
            "silver_dir": str(silver_dir),
        }
    )

    assert result["processed_count"] == 2
    assert silver_json(result, date(2026, 8, 2)).exists()
    assert silver_json(result, SILVER_INPUT_ROW["price_date"]).exists()


def test_대상_날짜가_처리되지_않으면_실패한다(tmp_path):
    """이전 실행이 남긴 Silver 파일이 있어도 존재만으로 통과하면 안 됩니다."""
    bronze_dir, silver_dir = tmp_path / "bronze", tmp_path / "silver"

    # 어제 수집분으로 Silver 파일을 미리 만들어 둡니다.
    write_silver_input(
        bronze_dir, SILVER_INPUT_ROW, COLLECTED_AT - timedelta(days=1)
    )
    to_silver(
        event={
            "collected_date": f"{COLLECTED_AT - timedelta(days=1):%Y-%m-%d}",
            "bronze_dir": str(bronze_dir),
            "silver_dir": str(silver_dir),
        }
    )

    # 오늘 수집분은 다른 price_date 인데 어제 날짜를 기대하면 실패해야 합니다.
    write_silver_input(
        bronze_dir,
        {**SILVER_INPUT_ROW, "price_date": date(2026, 8, 9)},
        COLLECTED_AT,
    )

    with pytest.raises(RuntimeError, match="대상 날짜를 처리하지 않았습니다"):
        to_silver(
            event={
                "collected_date": f"{COLLECTED_AT:%Y-%m-%d}",
                "bronze_dir": str(bronze_dir),
                "silver_dir": str(silver_dir),
                "expect_price_date": SILVER_INPUT_ROW["price_date"].isoformat(),
            }
        )


def test_대상을_둘_다_지정하면_실패한다(tmp_path):
    with pytest.raises(ValueError, match="정확히 하나만"):
        to_silver(
            event={
                "collected_date": "2026-08-09",
                "collected_month": "2026-08",
                "bronze_dir": str(tmp_path),
                "silver_dir": str(tmp_path),
            }
        )
