"""Gas Price Raw -> Bronze -> Silver 배선 검증 (네트워크 없이 Loader부터 실행)."""

from datetime import date, datetime, timezone
from pathlib import Path

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


def test_bronze_to_silver(tmp_path):
    bronze_dir = tmp_path / "bronze"
    silver_dir = tmp_path / "silver"

    write_result = GasPriceBronzeLoader(str(bronze_dir), COLLECTED_AT).write(ROW)
    assert write_result.row_count == 1
    # handler가 파일 이름에서 price_date를 읽으므로 이 규칙이 깨지면 안 됩니다.
    assert Path(write_result.location).stem == ROW["price_date"].isoformat()

    result = to_silver(
        event={
            "collected_month": f"{COLLECTED_AT:%Y-%m}",
            "bronze_dir": str(bronze_dir),
            "silver_dir": str(silver_dir),
        }
    )

    assert result["row_count"] == 1
    silver_json = (
        Path(result["location"])
        / f"price_date={ROW['price_date'].isoformat()}"
        / "gas_price.json"
    )
    assert silver_json.exists()
