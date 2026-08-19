"""차종별 제원 Bronze -> Silver 배선 검증 (네트워크 없이 Loader부터 실행)."""

from datetime import datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from sub.aws_lambda.common import vehicle_specs_layout as layout
from sub.aws_lambda.functions.fueleconomy_vehicle_specs_bronze_to_silver.handler import (
    lambda_handler as to_silver,
)
from sub.aws_lambda.functions.fueleconomy_vehicle_specs_raw_to_bronze.extractor import parse
from sub.aws_lambda.functions.fueleconomy_vehicle_specs_raw_to_bronze.loader import (
    VehicleSpecsBronzeLoader,
)

COLLECTED_AT = datetime(2027, 1, 1, 4, 0, tzinfo=timezone.utc)
COLLECTED_DATE = f"{COLLECTED_AT:%Y-%m-%d}"
SOURCE = "fueleconomy.gov"

# 원본 CSV 의 축소판. 내연기관 / 전기차 / 조인 키를 못 만드는 행을 섞었습니다.
CSV = (
    "id,year,make,model,baseModel,comb08,combE,range,atvType,cylinders\n"
    "1,2026,Mitsubishi,Outlander Sport 4WD,Outlander Sport,26,0,0,,4\n"
    "2,2026,Tesla,Model 3,Model 3,132,25,272,EV,\n"
)


def write_bronze(bronze_dir: Path, csv: str = CSV, collected_at=COLLECTED_AT) -> str:
    rows = parse(csv, collected_at)
    return VehicleSpecsBronzeLoader(str(bronze_dir), collected_at).write(rows).location


def run_silver(bronze_dir: Path, silver_dir: Path, collected_date: str) -> dict:
    return to_silver(
        event={
            "collected_date": collected_date,
            "bronze_dir": str(bronze_dir),
            "silver_dir": str(silver_dir),
        }
    )


def test_문자열_원본을_숫자와_조인_키로_정제한다(tmp_path):
    bronze_dir, silver_dir = tmp_path / "bronze", tmp_path / "silver"
    write_bronze(bronze_dir)

    result = run_silver(bronze_dir, silver_dir, COLLECTED_DATE)

    assert result["row_count"] == 2
    assert len(result["locations"]) == 1

    silver_path = Path(result["locations"][0])
    assert silver_path == layout.silver_file(
        str(silver_dir), COLLECTED_AT.date(), SOURCE
    )

    written = {row["source_id"]: row for row in pq.ParquetFile(silver_path).read().to_pylist()}

    gas = written["1"]
    assert gas["year"] == 2026
    assert gas["combined_mpg"] == 26.0
    # 구동방식 접미사가 붙은 model 과 빠진 baseModel 을 둘 다 남깁니다.
    assert gas["model_key"] == "OUTLANDER SPORT 4WD"
    assert gas["base_model_key"] == "OUTLANDER SPORT"
    assert gas["atv_type"] is None

    ev = written["2"]
    assert ev["combined_kwh_per_100mi"] == 25.0
    assert ev["range_miles"] == 272.0
    assert ev["atv_type"] == "EV"


def test_대장과_붙일_조인_키가_만들어진다(tmp_path):
    """대장은 "OUTLANDER SPORT", 제원 model 은 "Outlander Sport 4WD" 입니다.

    model_key 로는 안 붙고 base_model_key 로 붙어야 합니다.
    """
    from sub.aws_lambda.functions.vehicle_catalog_bronze_to_silver.transformer import (
        VehicleCatalogSilverTransformer,
    )

    bronze_dir, silver_dir = tmp_path / "bronze", tmp_path / "silver"
    write_bronze(bronze_dir)
    result = run_silver(bronze_dir, silver_dir, COLLECTED_DATE)
    specs = {
        row["source_id"]: row
        for row in pq.ParquetFile(result["locations"][0]).read().to_pylist()
    }["1"]

    catalog = VehicleCatalogSilverTransformer().transform(
        [
            {
                "vendor": "fasttrack",
                "make": "MITSUBISHI",
                "model": "OUTLANDER SPORT",
                "raw_name": "MITSUBISHI OUTLANDER SPORT",
                "price_usd": 554.0,
                "price_period": "week",
                "image_url": "https://example.com/outlander-sport.png",
                "source_url": "https://example.com",
                "collected_at": COLLECTED_AT,
            }
        ]
    )[0]

    assert specs["make_key"] == catalog["make_key"]
    assert specs["model_key"] != catalog["model_key"]
    assert specs["base_model_key"] == catalog["model_key"]


def test_조인_키를_못_만드는_행이_조금이면_건너뛴다(tmp_path):
    """공공 CSV 5만 행이라 결측이 섞입니다. 전량 실패시키면 그달 수집이 날아갑니다."""
    bronze_dir, silver_dir = tmp_path / "bronze", tmp_path / "silver"
    rows = parse(CSV, COLLECTED_AT)
    # 200행 중 1행만 model 이 비어 있는 상황 (0.5% < 임계치 1%)
    padded = [
        {**rows[0], "id": str(index)} for index in range(100, 299)
    ] + [{**rows[0], "id": "999", "model": None}]
    VehicleSpecsBronzeLoader(str(bronze_dir), COLLECTED_AT).write(padded)

    result = run_silver(bronze_dir, silver_dir, COLLECTED_DATE)

    assert result["row_count"] == 199


def test_조인_키를_못_만드는_행이_너무_많으면_실패한다(tmp_path):
    """원본 구조가 바뀌면 조용히 절반만 적재되지 않아야 합니다."""
    bronze_dir, silver_dir = tmp_path / "bronze", tmp_path / "silver"
    rows = parse(CSV, COLLECTED_AT)
    broken = [{**rows[0], "id": str(index), "model": None} for index in range(10)]
    broken[0]["model"] = "Outlander Sport 4WD"
    VehicleSpecsBronzeLoader(str(bronze_dir), COLLECTED_AT).write(broken)

    with pytest.raises(ValueError, match="건너뛴 행이 너무 많습니다"):
        run_silver(bronze_dir, silver_dir, COLLECTED_DATE)


def test_같은_수집일을_다시_변환하면_덮어쓴다(tmp_path):
    bronze_dir, silver_dir = tmp_path / "bronze", tmp_path / "silver"
    write_bronze(bronze_dir)

    first = run_silver(bronze_dir, silver_dir, COLLECTED_DATE)
    second = run_silver(bronze_dir, silver_dir, COLLECTED_DATE)

    assert first["locations"] == second["locations"]
    partition = Path(first["locations"][0]).parent
    assert len(list(partition.glob("*.parquet"))) == 1


def test_같은_날_여러_번_수집하면_최신_Bronze_를_읽는다(tmp_path):
    """Bronze 는 덮어쓰지 않고 쌓입니다. Silver 는 마지막 것만 봐야 합니다."""
    bronze_dir, silver_dir = tmp_path / "bronze", tmp_path / "silver"
    write_bronze(bronze_dir)

    later = COLLECTED_AT.replace(hour=6)
    extra = CSV + "3,2026,Kia,Niro,Niro,53,0,0,Hybrid,4\n"
    write_bronze(bronze_dir, extra, later)

    result = run_silver(bronze_dir, silver_dir, COLLECTED_DATE)

    assert result["row_count"] == 3


def test_Bronze_파티션이_없으면_실패한다(tmp_path):
    with pytest.raises(FileNotFoundError):
        run_silver(tmp_path / "bronze", tmp_path / "silver", COLLECTED_DATE)


def test_collected_date_형식을_검증한다(tmp_path):
    with pytest.raises(ValueError, match="YYYY-MM-DD"):
        run_silver(tmp_path / "bronze", tmp_path / "silver", "2027/01/01")
