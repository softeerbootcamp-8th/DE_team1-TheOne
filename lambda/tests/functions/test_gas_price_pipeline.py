"""Gas Price Raw -> Bronze -> Silver 배선 검증 (네트워크 없이 Loader부터 실행)."""

from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from functions.common import gas_price_layout as layout
from functions.gas_price_bronze_to_silver.handler import lambda_handler as to_silver
from functions.gas_price_raw_to_bronze.loader import GasPriceBronzeLoader

ROW = {
    "state": "NY",
    "fuel_type": "regular",
    "price_usd_per_gallon": 3.21,
    "price_date": date(2026, 8, 8),
    "source_url": "https://gasprices.aaa.com/?state=NY",
}
COLLECTED_AT = datetime(2026, 8, 9, 12, 0, tzinfo=timezone.utc)


def write_bronze(bronze_dir: Path, row: dict, collected_at: datetime) -> str:
    return GasPriceBronzeLoader(str(bronze_dir), collected_at).write(row).location


def silver_json(result: dict, price_date: date) -> Path:
    return (
        Path(result["locations"][0])
        / f"price_date={price_date.isoformat()}"
        / "gas_price.json"
    )


def test_bronze_파일명에서_price_date를_되읽을_수_있다():
    """핸들러가 이 왕복에 의존합니다 (bronze_file <-> price_date_from_bronze_file)."""
    path = layout.bronze_file("/tmp/bronze", "2026-08-09", ROW["price_date"])

    assert layout.price_date_from_bronze_file(str(path)) == ROW["price_date"].isoformat()
    assert path.parent.parent.name == layout.DATASET


def test_bronze_to_silver(tmp_path):
    bronze_dir, silver_dir = tmp_path / "bronze", tmp_path / "silver"

    location = write_bronze(bronze_dir, ROW, COLLECTED_AT)
    # Bronze 를 쓰는 쪽과 Silver 가 읽는 쪽이 같은 데이터셋 경로를 봐야 합니다.
    assert Path(location).parent.parent == layout.dataset_path(str(bronze_dir))

    result = to_silver(
        event={
            "collected_date": f"{COLLECTED_AT:%Y-%m-%d}",
            "bronze_dir": str(bronze_dir),
            "silver_dir": str(silver_dir),
            "expect_price_date": ROW["price_date"].isoformat(),
        }
    )

    assert result["row_count"] == 1
    assert silver_json(result, ROW["price_date"]).exists()


def test_과거_파티션이_깨져도_당일_처리는_성공한다(tmp_path):
    """이 이슈의 핵심 — 과거의 오류가 오늘 실행을 막지 않아야 합니다."""
    bronze_dir, silver_dir = tmp_path / "bronze", tmp_path / "silver"

    past_collected_at = COLLECTED_AT - timedelta(days=6)
    past_location = write_bronze(
        bronze_dir,
        {**ROW, "price_date": date(2026, 8, 2)},
        past_collected_at,
    )
    Path(past_location).write_text("{망가진 JSON", encoding="utf-8")

    write_bronze(bronze_dir, ROW, COLLECTED_AT)

    result = to_silver(
        event={
            "collected_date": f"{COLLECTED_AT:%Y-%m-%d}",
            "bronze_dir": str(bronze_dir),
            "silver_dir": str(silver_dir),
            "expect_price_date": ROW["price_date"].isoformat(),
        }
    )

    assert result["row_count"] == 1
    assert result["processed_count"] == 1


def test_당일_파일이_깨지면_실패한다(tmp_path):
    bronze_dir, silver_dir = tmp_path / "bronze", tmp_path / "silver"

    location = write_bronze(bronze_dir, ROW, COLLECTED_AT)
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

    write_bronze(
        bronze_dir,
        {**ROW, "price_date": date(2026, 8, 2)},
        COLLECTED_AT - timedelta(days=6),
    )
    write_bronze(bronze_dir, ROW, COLLECTED_AT)

    result = to_silver(
        event={
            "collected_month": f"{COLLECTED_AT:%Y-%m}",
            "bronze_dir": str(bronze_dir),
            "silver_dir": str(silver_dir),
        }
    )

    assert result["processed_count"] == 2
    assert silver_json(result, date(2026, 8, 2)).exists()
    assert silver_json(result, ROW["price_date"]).exists()


def test_대상_날짜가_처리되지_않으면_실패한다(tmp_path):
    """이전 실행이 남긴 Silver 파일이 있어도 존재만으로 통과하면 안 됩니다."""
    bronze_dir, silver_dir = tmp_path / "bronze", tmp_path / "silver"

    # 어제 수집분으로 Silver 파일을 미리 만들어 둡니다.
    write_bronze(bronze_dir, ROW, COLLECTED_AT - timedelta(days=1))
    to_silver(
        event={
            "collected_date": f"{COLLECTED_AT - timedelta(days=1):%Y-%m-%d}",
            "bronze_dir": str(bronze_dir),
            "silver_dir": str(silver_dir),
        }
    )

    # 오늘 수집분은 다른 price_date 인데 어제 날짜를 기대하면 실패해야 합니다.
    write_bronze(bronze_dir, {**ROW, "price_date": date(2026, 8, 9)}, COLLECTED_AT)

    with pytest.raises(RuntimeError, match="대상 날짜를 처리하지 않았습니다"):
        to_silver(
            event={
                "collected_date": f"{COLLECTED_AT:%Y-%m-%d}",
                "bronze_dir": str(bronze_dir),
                "silver_dir": str(silver_dir),
                "expect_price_date": ROW["price_date"].isoformat(),
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
